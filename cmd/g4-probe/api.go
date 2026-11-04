package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"mini-k8s/internal/api"
)

type apiOptions struct{ server, caFile, tokenFile string }

func (o *apiOptions) bind(f *flag.FlagSet) {
	f.StringVar(&o.server, "server", "", "HTTPS API origin")
	f.StringVar(&o.caFile, "ca-file", "", "API CA PEM file")
	f.StringVar(&o.tokenFile, "token-file", "", "administrator token file")
}

type apiClient struct {
	base, token string
	http        *http.Client
}

func newAPIClient(o apiOptions) (*apiClient, error) {
	u, err := url.Parse(o.server)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("server must be an HTTPS origin")
	}
	if o.tokenFile == "" || o.caFile == "" {
		return nil, errors.New("token-file and ca-file are required")
	}
	token, err := os.ReadFile(o.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token file: %w", err)
	}
	ca, err := os.ReadFile(o.caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	if strings.TrimSpace(string(token)) == "" {
		return nil, errors.New("empty token file")
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid CA file")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	return &apiClient{base: strings.TrimSuffix(o.server, "/"), token: strings.TrimSpace(string(token)), http: &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// API 错误只记录状态，不复制可能含凭据或环境变量的响应正文。
func (c *apiClient) request(method, resource string, body []byte, result any) (int, error) {
	req, err := http.NewRequest(method, c.base+"/api/mini/v1/"+resource, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(result); err != nil {
		return resp.StatusCode, fmt.Errorf("invalid API response: %w", err)
	}
	return resp.StatusCode, nil
}

type observedPod struct {
	Name            string        `json:"name"`
	UID             string        `json:"uid"`
	OwnerUID        string        `json:"ownerUID,omitempty"`
	NodeUID         string        `json:"nodeUID,omitempty"`
	ResourceVersion int64         `json:"resourceVersion"`
	Deleted         bool          `json:"deleted"`
	Requests        api.Resources `json:"requests"`
	Status          api.PodStatus `json:"status"`
}
type observedNode struct {
	Name            string         `json:"name"`
	UID             string         `json:"uid"`
	ResourceVersion int64          `json:"resourceVersion"`
	Spec            api.NodeSpec   `json:"spec"`
	Status          api.NodeStatus `json:"status"`
}
type apiSnapshot struct {
	Started  time.Time      `json:"started"`
	Finished time.Time      `json:"finished"`
	Nodes    []observedNode `json:"nodes"`
	Pods     []observedPod  `json:"pods"`
	Errors   []string       `json:"errors,omitempty"`
}

// 两个集合分别读取，不是原子快照；请求时间范围随结果记录。
func snapshot(c *apiClient) apiSnapshot {
	result := apiSnapshot{Started: time.Now().UTC(), Nodes: []observedNode{}, Pods: []observedPod{}}
	var nodes struct {
		Items []api.Node `json:"items"`
	}
	if _, err := c.request(http.MethodGet, "nodes", nil, &nodes); err != nil {
		result.Errors = append(result.Errors, "nodes: "+err.Error())
	}
	for _, n := range nodes.Items {
		result.Nodes = append(result.Nodes, observedNode{Name: n.Metadata.Name, UID: n.Metadata.UID, ResourceVersion: n.Metadata.ResourceVersion, Spec: n.Spec, Status: n.Status})
	}
	var pods struct {
		Items []api.Pod `json:"items"`
	}
	if _, err := c.request(http.MethodGet, "pods", nil, &pods); err != nil {
		result.Errors = append(result.Errors, "pods: "+err.Error())
	}
	for _, p := range pods.Items {
		result.Pods = append(result.Pods, observedPod{Name: p.Metadata.Name, UID: p.Metadata.UID, OwnerUID: p.Metadata.OwnerUID, NodeUID: p.NodeUID, ResourceVersion: p.Metadata.ResourceVersion, Deleted: p.Metadata.DeletionTimestamp != nil, Requests: p.Spec.Resources.Requests, Status: p.Status})
	}
	result.Finished = time.Now().UTC()
	return result
}

func observe(args []string) error {
	f := flag.NewFlagSet("observe", flag.ContinueOnError)
	var options apiOptions
	options.bind(f)
	duration := f.Duration("duration", 30*time.Second, "observation duration")
	interval := f.Duration("interval", time.Second, "snapshot interval")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *duration <= 0 || *interval <= 0 || flagsExtra(f) {
		return errors.New("positive duration and interval are required")
	}
	c, err := newAPIClient(options)
	if err != nil {
		return err
	}
	end := time.Now().Add(*duration)
	encoder := json.NewEncoder(os.Stdout)
	failed := false
	for {
		value := snapshot(c)
		if len(value.Errors) != 0 {
			failed = true
		}
		if err := encoder.Encode(value); err != nil {
			return err
		}
		if !time.Now().Before(end) {
			break
		}
		wait := time.Until(end)
		if *interval < wait {
			wait = *interval
		}
		time.Sleep(wait)
	}
	if failed {
		return errors.New("one or more API snapshots failed")
	}
	return nil
}

var podPrefix = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type createResult struct {
	Name     string    `json:"name"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	HTTP     int       `json:"http"`
	UID      string    `json:"uid,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// 请求同时发出；失败或超时可能已经落库，绝不自动重试或删除。
func create(args []string) error {
	f := flag.NewFlagSet("create", flag.ContinueOnError)
	var options apiOptions
	options.bind(f)
	manifest := f.String("manifest", "", "JSON Pod template")
	prefix := f.String("name-prefix", "", "unique prefix for Pod names")
	count := f.Int("count", 0, "number of concurrent requests")
	if err := f.Parse(args); err != nil {
		return err
	}
	if flagsExtra(f) || *count < 1 || *count > 200 || len(*prefix) > 58 || !podPrefix.MatchString(*prefix) || *manifest == "" {
		return errors.New("manifest, unique valid name-prefix (max 58 chars), and count (1..200) required")
	}
	data, err := os.ReadFile(*manifest)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	if len(data) > 1<<20 {
		return errors.New("manifest exceeds 1 MiB")
	}
	var pod api.Pod
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&pod); err != nil {
		return fmt.Errorf("decode Pod template: %w", err)
	}
	if pod.Kind != "Pod" || pod.APIVersion != api.Version || pod.Metadata.UID != "" || pod.Metadata.ResourceVersion != 0 || pod.Metadata.OwnerUID != "" || pod.Metadata.DeletionTimestamp != nil || pod.NodeUID != "" || pod.Status != (api.PodStatus{}) {
		return errors.New("template must be an unbound standalone Pod without server-managed metadata or status")
	}
	c, err := newAPIClient(options)
	if err != nil {
		return err
	}
	results := make([]createResult, *count)
	gate := make(chan struct{})
	var wg, ready sync.WaitGroup
	ready.Add(len(results))
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := pod
			p.Metadata.Name = fmt.Sprintf("%s-%03d", *prefix, i+1)
			result := createResult{Name: p.Metadata.Name}
			body, err := json.Marshal(p)
			ready.Done()
			<-gate
			result.Started = time.Now().UTC()
			if err == nil {
				var created api.Pod
				result.HTTP, err = c.request(http.MethodPost, "pods", body, &created)
				if err == nil {
					result.UID = created.Metadata.UID
					if result.UID == "" || created.Metadata.Name != result.Name {
						err = errors.New("response identity missing or mismatched")
					}
				}
			}
			if err != nil {
				result.Error = err.Error()
			}
			result.Finished = time.Now().UTC()
			results[i] = result
		}(i)
	}
	ready.Wait()
	close(gate)
	wg.Wait()
	encoder := json.NewEncoder(os.Stdout)
	failed := false
	for _, result := range results {
		if result.Error != "" {
			failed = true
		}
		if err := encoder.Encode(result); err != nil {
			return err
		}
	}
	if failed {
		return errors.New("at least one create failed or has an uncertain result; inspect names before retrying")
	}
	return nil
}

func flagsExtra(f *flag.FlagSet) bool { return f.NArg() != 0 }
