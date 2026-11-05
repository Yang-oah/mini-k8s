// Package proxy 将 Service 的就绪端点快照转换为节点本机转发规则。
package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"mini-k8s/internal/api"
)

// Endpoint 是仅供转发的 Pod 地址；Pod 身份与版本用于重读验证。
type Endpoint struct {
	IP   string
	Port int
}

// Route 包含一个 ClusterIP 及当前已验证的端点。
type Route struct {
	IP        string
	Port      int
	Endpoints []Endpoint
}

// Installer 应只操作当前实例拥有的专用规则。
type Installer interface {
	Apply(context.Context, []Route) error
	Close() error
}

// Proxy 周期性刷新 API；读取失败时保留上一次规则。
type Proxy struct {
	base      string
	token     string
	client    *http.Client
	interval  time.Duration
	installer Installer
}

type apiStatusError struct {
	path string
	code int
}

func (e *apiStatusError) Error() string {
	return fmt.Sprintf("API GET %s returned HTTP %d", e.path, e.code)
}

func missing(err error) bool {
	var status *apiStatusError
	return errors.As(err, &status) && status.code == http.StatusNotFound
}

// New 校验 HTTPS 和只读凭据；不允许重定向携带认证信息。
func New(server, caFile, tokenFile string, interval time.Duration, installer Installer) (*Proxy, error) {
	u, err := url.Parse(server)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("server must be an HTTPS origin")
	}
	if interval <= 0 || interval > 2*time.Second || installer == nil {
		return nil, errors.New("resync must be between zero and 2 seconds and installer is required")
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil || strings.TrimSpace(string(token)) == "" {
		return nil, errors.New("cannot read nonempty proxy token")
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil || !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("cannot read proxy CA")
		}
	}
	return &Proxy{base: strings.TrimSuffix(server, "/"), token: strings.TrimSpace(string(token)), interval: interval, installer: installer,
		client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (p *Proxy) get(ctx context.Context, path string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/api/mini/v1/"+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &apiStatusError{path: path, code: resp.StatusCode}
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(dst)
}

// Run 初次读取成功前不更改系统规则；错误发生时保留上次结果并重试。
func (p *Proxy) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	defer p.installer.Close()
	for {
		if err := p.reconcile(ctx); err != nil && ctx.Err() == nil {
			log.Printf("proxy reconciliation failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Proxy) reconcile(ctx context.Context) error {
	// 一轮快照不能因单个缓慢请求无限占住后续摘除。
	snapshotCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var services struct {
		Items []api.Service `json:"items"`
	}
	var pods struct {
		Items []api.Pod `json:"items"`
	}
	var nodes struct {
		Items []api.Node `json:"items"`
	}
	if err := p.get(snapshotCtx, "services", &services); err != nil {
		return err
	}
	if err := p.get(snapshotCtx, "pods", &pods); err != nil {
		return err
	}
	if err := p.get(snapshotCtx, "nodes", &nodes); err != nil {
		return err
	}
	// 三次 List 并非同一事务；写规则前重读每个 Service 和候选 Pod/Node。
	serviceGone, podGone, nodeGone := make([]bool, len(services.Items)), make([]bool, len(pods.Items)), make([]bool, len(nodes.Items))
	var tasks []func(context.Context) error
	for index := range services.Items {
		i := index
		tasks = append(tasks, func(ctx context.Context) error {
			var current api.Service
			err := p.get(ctx, "services/"+url.PathEscape(services.Items[i].Metadata.Name), &current)
			if missing(err) {
				serviceGone[i] = true
				return nil
			}
			if err != nil {
				return err
			}
			if current.Metadata.UID != services.Items[i].Metadata.UID || current.Metadata.ResourceVersion != services.Items[i].Metadata.ResourceVersion {
				serviceGone[i] = true
				return nil
			}
			services.Items[i] = current
			return nil
		})
	}
	for index := range pods.Items {
		i := index
		if !pods.Items[i].Status.Ready {
			continue
		}
		tasks = append(tasks, func(ctx context.Context) error {
			var current api.Pod
			err := p.get(ctx, "pods/"+url.PathEscape(pods.Items[i].Metadata.Name), &current)
			if missing(err) {
				podGone[i] = true
				return nil
			}
			if err != nil {
				return err
			}
			if current.Metadata.UID != pods.Items[i].Metadata.UID {
				podGone[i] = true
				return nil
			}
			pods.Items[i] = current
			return nil
		})
	}
	if err := runChecks(snapshotCtx, tasks, 16); err != nil {
		return err
	}
	tasks = nil
	// 节点最后读取，使 Pod 证明与失联代次尽可能接近同一时刻。
	for index := range nodes.Items {
		i := index
		tasks = append(tasks, func(ctx context.Context) error {
			var current api.Node
			err := p.get(ctx, "nodes/"+url.PathEscape(nodes.Items[i].Metadata.Name), &current)
			if missing(err) {
				nodeGone[i] = true
				return nil
			}
			if err != nil {
				return err
			}
			if current.Metadata.UID != nodes.Items[i].Metadata.UID {
				nodeGone[i] = true
				return nil
			}
			nodes.Items[i] = current
			return nil
		})
	}
	if err := runChecks(snapshotCtx, tasks, 16); err != nil {
		return err
	}
	for i := len(services.Items) - 1; i >= 0; i-- {
		if serviceGone[i] {
			services.Items = append(services.Items[:i], services.Items[i+1:]...)
		}
	}
	for i := len(pods.Items) - 1; i >= 0; i-- {
		if podGone[i] {
			pods.Items = append(pods.Items[:i], pods.Items[i+1:]...)
		}
	}
	for i := len(nodes.Items) - 1; i >= 0; i-- {
		if nodeGone[i] {
			nodes.Items = append(nodes.Items[:i], nodes.Items[i+1:]...)
		}
	}
	routes, err := buildRoutes(services.Items, pods.Items, nodes.Items)
	if err != nil {
		return err
	}
	return p.installer.Apply(ctx, routes)
}

// 限制并发重读，404 由调用者处理；其他错误使整轮失败并保留上次规则。
func runChecks(ctx context.Context, tasks []func(context.Context) error, workers int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan func(context.Context) error)
	firstError := make(chan error, 1)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if err := task(ctx); err != nil {
					select {
					case firstError <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}
	for _, task := range tasks {
		select {
		case jobs <- task:
		case <-ctx.Done():
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-firstError:
		return err
	default:
		return ctx.Err()
	}
}

func matches(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if actual, ok := labels[key]; !ok || actual != value {
			return false
		}
	}
	return true
}

func ipv4(value string) bool {
	ip, err := netip.ParseAddr(value)
	return err == nil && ip.Is4() && !ip.IsUnspecified() && !ip.IsLoopback() && !ip.IsMulticast()
}

// 即使节点上报异常地址，也不允许把 Service 导向其 Pod 网段以外。
func podIPMatchesNode(value, cidr string) bool {
	ip, err := netip.ParseAddr(value)
	if err != nil || !ipv4(value) {
		return false
	}
	prefix, err := netip.ParsePrefix(cidr)
	return err == nil && prefix.Addr().Is4() && prefix == prefix.Masked() && prefix.Contains(ip)
}

func buildRoutes(services []api.Service, pods []api.Pod, nodes []api.Node) ([]Route, error) {
	byUID := make(map[string]api.Node, len(nodes))
	for _, n := range nodes {
		if n.Metadata.UID != "" {
			byUID[n.Metadata.UID] = n
		}
	}
	seen := make(map[string]bool)
	routes := make([]Route, 0, len(services))
	for _, s := range services {
		if s.Metadata.DeletionTimestamp != nil {
			continue
		}
		if s.Metadata.UID == "" || !ipv4(s.Spec.ClusterIP) || s.Spec.Port < 1 || s.Spec.Port > 65535 || s.Spec.TargetPort < 1 || s.Spec.TargetPort > 65535 || len(s.Spec.Selector) == 0 || seen[s.Spec.ClusterIP] {
			return nil, errors.New("invalid or duplicate service address")
		}
		seen[s.Spec.ClusterIP] = true
		route := Route{IP: s.Spec.ClusterIP, Port: s.Spec.Port}
		for _, pod := range pods {
			n, ok := byUID[pod.NodeUID]
			// Node GET 由控制面按自身时钟投影过期状态，避免跨主机时钟偏差。
			if !ok || pod.Metadata.UID == "" || n.Metadata.DeletionTimestamp != nil || !n.Status.Ready || n.Status.LastHeartbeat.IsZero() || n.Status.ReadinessEpoch == 0 || pod.Status.ReadinessEpoch != n.Status.ReadinessEpoch || pod.Metadata.DeletionTimestamp != nil || pod.Status.Phase != "Running" || !pod.Status.Ready || !podIPMatchesNode(pod.Status.PodIP, n.Spec.PodCIDR) || !matches(s.Spec.Selector, pod.Metadata.Labels) {
				continue
			}
			route.Endpoints = append(route.Endpoints, Endpoint{IP: pod.Status.PodIP, Port: s.Spec.TargetPort})
		}
		sort.Slice(route.Endpoints, func(i, j int) bool { return route.Endpoints[i].IP < route.Endpoints[j].IP })
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].IP < routes[j].IP })
	return routes, nil
}
