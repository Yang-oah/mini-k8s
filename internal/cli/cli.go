// Package cli 实现面向用户的 mini API 客户端。
package cli

import (
	"bytes"
	"context"
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
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"mini-k8s/internal/api"
)

const apiPath = "/api/mini/v1/"

type client struct {
	base  string
	token string
	http  *http.Client
}

func newClient(server, tokenFile, caFile string) (*client, error) {
	u, err := url.Parse(server)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("server must be an HTTPS origin")
	}
	if tokenFile == "" {
		return nil, errors.New("--token-file is required")
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
	return &client{base: strings.TrimSuffix(server, "/"), token: strings.TrimSpace(string(token)), http: &http.Client{
		Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}},
	}}, nil
}

func (c *client) request(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+apiPath+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return data, resp.StatusCode, fmt.Errorf("API returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	return data, resp.StatusCode, nil
}

func collection(kind string) (string, error) {
	switch strings.ToLower(kind) {
	case "pod", "pods":
		return "pods", nil
	case "replicaset", "replicasets", "rs":
		return "replicasets", nil
	case "node", "nodes":
		return "nodes", nil
	case "service", "services", "svc":
		return "services", nil
	default:
		return "", fmt.Errorf("unsupported resource kind %q", kind)
	}
}

type manifest struct {
	collection string
	name       string
	object     map[string]any
}

// readManifests 逐个解析 YAML 文档，再使用 JSON 资源模型严格检查未知字段。
// 全部文档先通过本地检查后才提交；多对象写入本身不具有事务性。
func readManifests(r io.Reader) ([]manifest, error) {
	data, err := io.ReadAll(io.LimitReader(r, (4<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 4<<20 {
		return nil, errors.New("manifest exceeds 4 MiB")
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var manifests []manifest
	for i := 1; ; i++ {
		var node yaml.Node
		if err := dec.Decode(&node); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("document %d: %w", i, err)
		}
		if len(node.Content) == 0 || node.Content[0].Tag == "!!null" {
			continue
		}
		var obj map[string]any
		if err := node.Decode(&obj); err != nil {
			return nil, fmt.Errorf("document %d: %w", i, err)
		}
		data, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("document %d: invalid mapping: %w", i, err)
		}
		kind, _ := obj["kind"].(string)
		var target any
		switch kind {
		case "Pod":
			target = &api.Pod{}
		case "ReplicaSet":
			target = &api.ReplicaSet{}
		case "Node":
			target = &api.Node{}
		case "Service":
			target = &api.Service{}
		default:
			return nil, fmt.Errorf("document %d: unsupported kind %q", i, kind)
		}
		if err := api.DecodeStrict(bytes.NewReader(data), target); err != nil {
			return nil, fmt.Errorf("document %d: %w", i, err)
		}
		meta, ok := obj["metadata"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("document %d: metadata must be a mapping", i)
		}
		name, _ := meta["name"].(string)
		if name == "" || strings.ContainsAny(name, "/?#") {
			return nil, fmt.Errorf("document %d: invalid metadata.name", i)
		}
		// 用户声明不能指定属于 API、控制器或 Agent 的字段。
		for _, key := range []string{"uid", "resourceVersion", "ownerUID", "deletionTimestamp"} {
			if _, found := meta[key]; found {
				return nil, fmt.Errorf("document %d: metadata.%s is system managed", i, key)
			}
		}
		for _, key := range []string{"status", "nodeUID"} {
			if _, found := obj[key]; found {
				return nil, fmt.Errorf("document %d: %s is system managed", i, key)
			}
		}
		if kind == "Service" {
			if spec, ok := obj["spec"].(map[string]any); ok {
				if _, exists := spec["clusterIP"]; exists {
					return nil, fmt.Errorf("document %d: spec.clusterIP is system managed", i)
				}
			}
		}
		col, _ := collection(kind)
		manifests = append(manifests, manifest{col, name, obj})
	}
	if len(manifests) == 0 {
		return nil, errors.New("manifest contains no resources")
	}
	return manifests, nil
}

func (c *client) apply(ctx context.Context, m manifest) error {
	path := m.collection + "/" + url.PathEscape(m.name)
	current, code, err := c.request(ctx, http.MethodGet, path, nil)
	method, target := http.MethodPost, m.collection
	if err != nil && code != http.StatusNotFound {
		return err
	}
	obj := m.object
	if err == nil {
		var existing map[string]any
		decoder := json.NewDecoder(bytes.NewReader(current))
		// resourceVersion 是整数；避免转成 float64 后丢失精度。
		decoder.UseNumber()
		if err := decoder.Decode(&existing); err != nil {
			return fmt.Errorf("decode current object: %w", err)
		}
		oldMeta, ok := existing["metadata"].(map[string]any)
		if !ok {
			return errors.New("current object has invalid metadata")
		}
		// 基于最新对象替换声明字段，保留 UID、版本、绑定和 status。
		// API 用保留的 resourceVersion 检查并发冲突；此处不盲目重试。
		newMeta := obj["metadata"].(map[string]any)
		for _, key := range []string{"labels", "annotations"} {
			if value, exists := newMeta[key]; exists {
				oldMeta[key] = value
			} else {
				delete(oldMeta, key)
			}
		}
		existing["spec"] = obj["spec"]
		// ClusterIP 由服务端分配，apply 时保留原值。
		if m.collection == "services" {
			var old map[string]any
			if json.Unmarshal(current, &old) == nil {
				if spec, ok := old["spec"].(map[string]any); ok {
					if ip, ok := spec["clusterIP"]; ok {
						if desired, ok := existing["spec"].(map[string]any); ok {
							desired["clusterIP"] = ip
						}
					}
				}
			}
		}
		obj, method, target = existing, http.MethodPut, path
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, _, err = c.request(ctx, method, target, body)
	return err
}

func fileArg(args []string) (string, error) {
	f := flag.NewFlagSet("file", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	name := f.String("f", "", "manifest file")
	if err := f.Parse(args); err != nil || *name == "" || f.NArg() != 0 {
		return "", errors.New("expected -f <manifest.yaml>")
	}
	return *name, nil
}

// Run 执行一次 CLI 调用；全局连接参数必须位于子命令之前。
func Run(ctx context.Context, args []string, out io.Writer) error {
	f := flag.NewFlagSet("mini-k8s", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	server := f.String("server", "https://127.0.0.1:8443", "API HTTPS origin")
	token := f.String("token-file", "", "bearer token file")
	ca := f.String("ca-file", "", "additional CA PEM file")
	if err := f.Parse(args); err != nil {
		return err
	}
	args = f.Args()
	if len(args) == 0 {
		return errors.New("expected apply, get, delete, or scale")
	}
	c, err := newClient(*server, *token, *ca)
	if err != nil {
		return err
	}
	switch args[0] {
	case "apply", "delete":
		name, err := fileArg(args[1:])
		if err != nil {
			return err
		}
		file, err := os.Open(name)
		if err != nil {
			return err
		}
		manifests, err := readManifests(file)
		file.Close()
		if err != nil {
			return err
		}
		// 逐对象提交并输出已完成项；后续失败不会回滚之前的对象。
		for _, m := range manifests {
			if args[0] == "apply" {
				err = c.apply(ctx, m)
			} else {
				_, code, deleteErr := c.request(ctx, http.MethodDelete, m.collection+"/"+url.PathEscape(m.name), nil)
				if code != http.StatusNotFound {
					err = deleteErr
				}
			}
			if err != nil {
				return fmt.Errorf("%s/%s: %w", m.collection, m.name, err)
			}
			fmt.Fprintf(out, "%s/%s %s\n", m.collection, m.name, args[0])
		}
		return nil
	case "get":
		if len(args) < 2 || len(args) > 3 {
			return errors.New("expected get <resource> [name]")
		}
		col, err := collection(args[1])
		if err != nil {
			return err
		}
		if len(args) == 3 {
			col += "/" + url.PathEscape(args[2])
		}
		data, _, err := c.request(ctx, http.MethodGet, col, nil)
		if err != nil {
			return err
		}
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, data, "", "  "); err != nil {
			return fmt.Errorf("invalid API response: %w", err)
		}
		_, err = fmt.Fprintln(out, pretty.String())
		return err
	case "scale":
		if len(args) != 5 || args[1] != "rs" || args[3] != "--replicas" {
			return errors.New("expected scale rs <name> --replicas <count>")
		}
		n, err := strconv.Atoi(args[4])
		if err != nil || n < 0 {
			return errors.New("replicas must be a nonnegative integer")
		}
		path := "replicasets/" + url.PathEscape(args[2])
		data, _, err := c.request(ctx, http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		var obj map[string]any
		decoder := json.NewDecoder(bytes.NewReader(data))
		// 保持版本整数原值，让服务端可靠检测读取后的并发修改。
		decoder.UseNumber()
		if err := decoder.Decode(&obj); err != nil {
			return err
		}
		spec, ok := obj["spec"].(map[string]any)
		if !ok {
			return errors.New("current ReplicaSet has invalid spec")
		}
		spec["replicas"] = n
		body, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		if _, _, err := c.request(ctx, http.MethodPut, path, body); err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "replicasets/%s scaled to %d\n", args[2], n)
		return err
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
