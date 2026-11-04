// Package scheduler 将待调度 Pod 按资源请求分配给健康节点。
package scheduler

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/bits"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"mini-k8s/internal/api"
)

const prefix = "/api/mini/v1/"

// Scheduler 定期读取快照并请求 API 原子绑定；不在本地修改资源预留。
type Scheduler struct {
	base     string
	token    string
	client   *http.Client
	interval time.Duration
}

// New 构造带独立身份和 TLS 证书校验的调度器。
func New(server, tokenFile, caFile string, interval time.Duration) (*Scheduler, error) {
	u, err := url.Parse(server)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("server must be an HTTPS origin")
	}
	if interval <= 0 {
		return nil, errors.New("resync must be positive")
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	if strings.TrimSpace(string(token)) == "" {
		return nil, errors.New("token file is empty")
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file contains no certificates")
		}
	}
	return &Scheduler{base: strings.TrimSuffix(server, "/"), token: strings.TrimSpace(string(token)), interval: interval,
		client: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}}}, nil
}

func (s *Scheduler) call(ctx context.Context, method, path string, body, target any) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.base+prefix+path, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("API %s %s: HTTP %d", method, path, resp.StatusCode)
	}
	if target != nil {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(target); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

// Run 持续重新读取状态；冲突交由下一轮以新版本安全重试。
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if err := s.reconcile(ctx); err != nil && ctx.Err() == nil {
			log.Printf("scheduler reconcile: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) reconcile(ctx context.Context) error {
	var pods struct {
		Items []api.Pod `json:"items"`
	}
	if _, err := s.call(ctx, http.MethodGet, "pods", nil, &pods); err != nil {
		return err
	}
	// 保证多实例使用相同顺序；真正的并发安全由 API 的事务保证。
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Metadata.Name < pods.Items[j].Metadata.Name })
	for _, pod := range pods.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if pod.NodeUID != "" || pod.Metadata.DeletionTimestamp != nil || pod.Status.Phase != "Pending" {
			continue
		}
		var nodes struct {
			Items []api.Node `json:"items"`
		}
		if _, err := s.call(ctx, http.MethodGet, "nodes", nil, &nodes); err != nil {
			return err
		}
		node, reason := chooseNode(pod.Spec.Resources.Requests, nodes.Items)
		path := "pods/" + url.PathEscape(pod.Metadata.Name)
		var status int
		var err error
		if node != nil {
			status, err = s.call(ctx, http.MethodPost, path+"/bind", map[string]any{
				"nodeName": node.Metadata.Name, "nodeUID": node.Metadata.UID,
				"podUID": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion,
			}, nil)
		} else if pod.Status.Reason != reason {
			status, err = s.call(ctx, http.MethodPut, path+"/scheduling", map[string]any{
				"uid": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion, "reason": reason,
			}, nil)
		}
		// 资源版本变化、节点状态变化或删除均在下次快照中重新判断。
		if err != nil && status != http.StatusConflict && status != http.StatusNotFound {
			return err
		}
	}
	return nil
}

// lessRatio 用 128 位乘积比较绑定后的 CPU 使用率，避免整数溢出及浮点误差。
func lessRatio(aUsed, aTotal, bUsed, bTotal int64) int {
	ah, al := bits.Mul64(uint64(aUsed), uint64(bTotal))
	bh, bl := bits.Mul64(uint64(bUsed), uint64(aTotal))
	if ah < bh || (ah == bh && al < bl) {
		return -1
	}
	if ah > bh || (ah == bh && al > bl) {
		return 1
	}
	return 0
}

func chooseNode(req api.Resources, nodes []api.Node) (*api.Node, string) {
	var best *api.Node
	ready, cpuShort, memoryShort := false, false, false
	for i := range nodes {
		n := &nodes[i]
		if !n.Status.Ready || n.Spec.Unschedulable || n.Metadata.DeletionTimestamp != nil {
			continue
		}
		ready = true
		cap, used := n.Spec.Allocatable, n.Status.Reserved
		// 异常的负数或超额状态不得用于调度。
		cpu := req.CPUMilli > 0 && cap.CPUMilli > 0 && used.CPUMilli >= 0 && used.CPUMilli <= cap.CPUMilli && req.CPUMilli <= cap.CPUMilli-used.CPUMilli
		memory := req.MemoryBytes > 0 && cap.MemoryBytes > 0 && used.MemoryBytes >= 0 && used.MemoryBytes <= cap.MemoryBytes && req.MemoryBytes <= cap.MemoryBytes-used.MemoryBytes
		if !cpu {
			cpuShort = true
		}
		if !memory {
			memoryShort = true
		}
		if !cpu || !memory {
			continue
		}
		if best == nil {
			best = n
			continue
		}
		cmp := lessRatio(used.CPUMilli+req.CPUMilli, cap.CPUMilli, best.Status.Reserved.CPUMilli+req.CPUMilli, best.Spec.Allocatable.CPUMilli)
		if cmp < 0 || (cmp == 0 && n.Metadata.Name < best.Metadata.Name) {
			best = n
		}
	}
	if best != nil {
		return best, ""
	}
	if !ready {
		return nil, "NoReadyNodes"
	}
	if cpuShort && memoryShort {
		return nil, "InsufficientCPUAndMemory"
	}
	if cpuShort {
		return nil, "InsufficientCPU"
	}
	return nil, "InsufficientMemory"
}
