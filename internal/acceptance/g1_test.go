// Package acceptance 通过真实 API 进程和真实 etcd 验证控制面契约。
// 缺少验收环境变量时测试会跳过；跳过不代表阶段验收通过。
package acceptance

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mini-k8s/internal/cli"
)

const admin = "g1-admin-secret"
const scheduler = "g1-scheduler-secret"
const agent = "g1-agent-secret"
const otherAgent = "g1-other-agent-secret"
const sensitive = "g1-sensitive-env-value"

type object map[string]any

// g1Fixture 管理本次测试的 API 进程、独立名称、凭据和连接。
type g1Fixture struct {
	t                                                               *testing.T
	url, cert, key, auth, token, node, other, pod, rs, binary, etcd string
	client                                                          *http.Client
	process                                                         *exec.Cmd
	log                                                             *os.File
}

func (f *g1Fixture) start() {
	f.t.Helper()
	f.process = exec.Command(f.binary, "-listen", strings.TrimPrefix(f.url, "https://"), "-etcd", f.etcd, "-tls-cert", f.cert, "-tls-key", f.key, "-auth-file", f.auth)
	f.process.Stdout, f.process.Stderr = f.log, f.log
	if err := f.process.Start(); err != nil {
		f.t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, code, err := f.request("GET", "pods", admin, nil)
		if err == nil && code == 200 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.t.Fatal("API did not become ready within 10 seconds")
}

func (f *g1Fixture) stop() {
	f.t.Helper()
	if f.process != nil && f.process.Process != nil {
		_ = f.process.Process.Kill()
		_ = f.process.Wait()
		f.process = nil
	}
}

func (f *g1Fixture) request(method, path, token string, payload any) ([]byte, int, error) {
	var reader io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, f.url+"/api/mini/v1/"+path, reader)
	if err != nil {
		return nil, 0, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	return b, resp.StatusCode, err
}

func (f *g1Fixture) call(method, path, token string, payload any, code int) object {
	f.t.Helper()
	b, got, err := f.request(method, path, token, payload)
	if err != nil || got != code {
		f.t.Fatalf("%s %s: got HTTP %d (%v), want %d: %s", method, path, got, err, code, b)
	}
	var out object
	if len(b) > 0 {
		if err := json.Unmarshal(b, &out); err != nil {
			f.t.Fatal(err)
		}
	}
	return out
}

func metadataOf(o object) object         { return object(o["metadata"].(map[string]any)) }
func specOf(o object) object             { return object(o["spec"].(map[string]any)) }
func resourceVersionOf(o object) float64 { return metadataOf(o)["resourceVersion"].(float64) }
func uidOf(o object) string              { return metadataOf(o)["uid"].(string) }

func podManifest(name string) object {
	return object{
		"apiVersion": "mini.k8s/v1", "kind": "Pod", "metadata": object{"name": name},
		"spec": object{
			"image": "example.invalid/demo:fixed", "restartPolicy": "Always",
			"env": object{"SENSITIVE": sensitive},
			"resources": object{
				"requests": object{"cpuMilli": 100, "memoryBytes": 1024},
				"limits":   object{"cpuMilli": 200, "memoryBytes": 2048},
			},
			"readiness": object{"port": 8080},
		},
	}
}
func replicaSetManifest(name string) object {
	p := podManifest(name)
	return object{
		"apiVersion": "mini.k8s/v1", "kind": "ReplicaSet", "metadata": object{"name": name},
		"spec": object{
			"replicas": 1, "selector": object{"app": name},
			"template": object{"labels": object{"app": name}, "spec": p["spec"]},
		},
	}
}

func (f *g1Fixture) runCLI(args ...string) {
	f.t.Helper()
	var out bytes.Buffer
	args = append([]string{"--server", f.url, "--ca-file", f.cert, "--token-file", f.token}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Run(ctx, args, &out); err != nil {
		f.t.Fatalf("CLI %v failed: %v (%s)", args[6:], err, out.String())
	}
}

func (f *g1Fixture) manifest(v object) string {
	f.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		f.t.Fatal(err)
	}
	file := filepath.Join(f.t.TempDir(), "manifest.json")
	if err = os.WriteFile(file, b, 0600); err != nil {
		f.t.Fatal(err)
	}
	return file
}

// cleanup 只经正式 API 清理本轮对象；已绑定 Pod 须由节点身份确认清理。
// 若无法确认 Pod 已消失，保留节点，避免破坏后续排查和恢复所需的预留信息。
func (f *g1Fixture) cleanup() {
	podCleared := true
	for _, name := range []string{f.pod} {
		b, code, err := f.request("GET", "pods/"+name, admin, nil)
		if err != nil {
			f.t.Errorf("cleanup read: %v", err)
			podCleared = false
			continue
		}
		if code == 404 {
			continue
		}
		if code != 200 {
			f.t.Errorf("cleanup read HTTP %d", code)
			podCleared = false
			continue
		}
		var p object
		_ = json.Unmarshal(b, &p)
		_, code, err = f.request("DELETE", "pods/"+name, admin, nil)
		if err != nil || (code != 200 && code != 404) {
			f.t.Errorf("cleanup delete %s: HTTP %d %v", name, code, err)
			podCleared = false
			continue
		}
		if p["nodeUID"] != nil && p["nodeUID"] != "" {
			b, code, err = f.request("GET", "pods/"+name, admin, nil)
			if err == nil && code == 200 {
				_ = json.Unmarshal(b, &p)
				_, code, err = f.request("POST", "pods/"+name+"/cleanup", agent, object{"uid": uidOf(p), "resourceVersion": resourceVersionOf(p)})
				if err != nil || code != 200 {
					f.t.Errorf("cleanup bound Pod: HTTP %d %v", code, err)
				}
			}
		}
		_, code, err = f.request("GET", "pods/"+name, admin, nil)
		if err != nil || code != 404 {
			f.t.Errorf("cleanup verification %s: HTTP %d %v", name, code, err)
			podCleared = false
		}
	}
	paths := []string{"replicasets/" + f.rs}
	if podCleared {
		paths = append(paths, "nodes/"+f.node, "nodes/"+f.other)
	} else {
		f.t.Error("retaining test nodes because Pod cleanup was not confirmed")
	}
	for _, path := range paths {
		_, code, err := f.request("DELETE", path, admin, nil)
		if err != nil || (code != 200 && code != 404) {
			f.t.Errorf("cleanup %s: HTTP %d %v", path, code, err)
		}
		_, code, err = f.request("GET", path, admin, nil)
		if err != nil || code != 404 {
			f.t.Errorf("cleanup verification %s: HTTP %d %v", path, code, err)
		}
	}
}

// setupG1 启动独立 API 进程，连接调用者提供的真实 etcd；证书和凭据仅供本轮使用。
func setupG1(t *testing.T) *g1Fixture {
	t.Helper()
	endpoint, binary := os.Getenv("G1_ETCD_ENDPOINT"), os.Getenv("G1_API_BINARY")
	if endpoint == "" || binary == "" {
		t.Skip("set G1_ETCD_ENDPOINT and G1_API_BINARY to run real-etcd acceptance")
	}
	dir := t.TempDir()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	f := &g1Fixture{t: t, etcd: endpoint, binary: binary, node: "g1-node-" + suffix, other: "g1-other-" + suffix, pod: "g1-pod-" + suffix, rs: "g1-rs-" + suffix}
	f.cert = filepath.Join(dir, "ca.pem")
	f.key = filepath.Join(dir, "server-key.pem")
	f.auth = filepath.Join(dir, "auth.json")
	f.token = filepath.Join(dir, "token")
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "g1-test"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{f.cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), f.key: pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), f.token: []byte(admin)} {
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := json.Marshal(object{"admin": admin, "scheduler": scheduler, "agents": object{f.node: agent, f.other: otherAgent}})
	if err = os.WriteFile(f.auth, b, 0600); err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	f.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.url = "https://" + l.Addr().String()
	_ = l.Close()
	logPath := os.Getenv("G1_API_LOG")
	if logPath == "" {
		logPath = filepath.Join(dir, "api.log")
	}
	f.log, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.stop(); _ = f.log.Close() })
	f.start()
	t.Cleanup(func() { f.t = t; f.cleanup() })
	return f
}

func TestG1(t *testing.T) {
	f := setupG1(t)
	// G1-01：通过正式 CLI 验证声明、伸缩、重复提交及同名重建。
	if !t.Run("G1-01_CRUD", func(t *testing.T) {
		f.t = t
		podFile, replicaSetFile := f.manifest(podManifest(f.pod)), f.manifest(replicaSetManifest(f.rs))
		f.runCLI("apply", "-f", podFile)
		f.runCLI("apply", "-f", replicaSetFile)
		p := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		oldUID := uidOf(p)
		f.runCLI("apply", "-f", podFile)
		if uidOf(f.call("GET", "pods/"+f.pod, admin, nil, 200)) != oldUID {
			t.Fatal("repeat apply changed UID")
		}
		f.runCLI("get", "pod", f.pod)
		f.runCLI("scale", "rs", f.rs, "--replicas", "3")
		rs := f.call("GET", "replicasets/"+f.rs, admin, nil, 200)
		if specOf(rs)["replicas"] != float64(3) {
			t.Fatal("scale not persisted")
		}
		f.runCLI("delete", "-f", replicaSetFile)
		f.call("GET", "replicasets/"+f.rs, admin, nil, 404)
		f.runCLI("apply", "-f", replicaSetFile)
		f.runCLI("delete", "-f", podFile)
		f.runCLI("delete", "-f", podFile)
		f.runCLI("apply", "-f", podFile)
		if uidOf(f.call("GET", "pods/"+f.pod, admin, nil, 200)) == oldUID {
			t.Fatal("recreated Pod retained UID")
		}
	}) {
		return
	}
	// G1-02：重启 API 进程后，etcd 中的 UID、规格和状态仍可读。
	if !t.Run("G1-02_persistence", func(t *testing.T) {
		f.t = t
		before := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		f.stop()
		f.start()
		after := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		if uidOf(before) != uidOf(after) || fmt.Sprint(before["spec"]) != fmt.Sprint(after["spec"]) || fmt.Sprint(before["status"]) != fmt.Sprint(after["status"]) {
			t.Fatal("restart changed persisted state")
		}
	}) {
		return
	}
	// G1-03：相同旧版本并发写入只能成功一次；非法字段不得改变对象。
	if !t.Run("G1-03_CAS_fields", func(t *testing.T) {
		f.t = t
		rs := f.call("GET", "replicasets/"+f.rs, admin, nil, 200)
		data, _ := json.Marshal(rs)
		var a, b object
		_ = json.Unmarshal(data, &a)
		_ = json.Unmarshal(data, &b)
		specOf(a)["replicas"] = float64(2)
		specOf(b)["replicas"] = float64(4)
		ch := make(chan int, 2)
		for _, v := range []object{a, b} {
			go func(v object) {
				_, code, err := f.request("PUT", "replicasets/"+f.rs, admin, v)
				if err != nil {
					ch <- 0
				} else {
					ch <- code
				}
			}(v)
		}
		x, y := <-ch, <-ch
		if !((x == 200 && y == 409) || (x == 409 && y == 200)) {
			t.Fatalf("CAS got %d and %d", x, y)
		}
		p := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		before, _ := json.Marshal(p)
		specOf(p)["unknown"] = true
		f.call("PUT", "pods/"+f.pod, admin, p, 400)
		_ = json.Unmarshal(before, &p)
		specOf(p)["image"] = "modified"
		f.call("PUT", "pods/"+f.pod, admin, p, 400)
		_ = json.Unmarshal(before, &p)
		f.call("PUT", "pods/"+f.pod+"/status", admin, object{"resourceVersion": resourceVersionOf(p), "status": object{"phase": "Running"}}, 403)
		after, _ := json.Marshal(f.call("GET", "pods/"+f.pod, admin, nil, 200))
		if !bytes.Equal(before, after) {
			t.Fatal("rejected writes changed resource")
		}
	}) {
		return
	}
	// G1-04：先 List 再修改并 Watch，断开后从已处理版本重连。
	if !t.Run("G1-04_list_watch", func(t *testing.T) {
		f.t = t
		list := f.call("GET", "pods", admin, nil, 200)
		version := int64(list["resourceVersion"].(float64))
		for i := 0; i < 2; i++ {
			p := f.call("GET", "pods/"+f.pod, admin, nil, 200)
			metadataOf(p)["annotations"] = object{"watch": fmt.Sprint(i)}
			f.call("PUT", "pods/"+f.pod, admin, p, 200)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/mini/v1/pods?watch=true&resourceVersion=%d", f.url, version), nil)
			req.Header.Set("Authorization", "Bearer "+admin)
			resp, err := f.client.Do(req)
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			var event object
			err = json.NewDecoder(resp.Body).Decode(&event)
			_ = resp.Body.Close()
			cancel()
			if err != nil || resp.StatusCode != 200 {
				t.Fatalf("watch: HTTP %d, %v", resp.StatusCode, err)
			}
			observed, ok := event["object"].(map[string]any)
			if !ok || metadataOf(object(observed))["name"] != f.pod || metadataOf(object(observed))["annotations"].(map[string]any)["watch"] != fmt.Sprint(i) {
				t.Fatalf("wrong watch event: %v", event)
			}
			version = int64(event["resourceVersion"].(float64))
		}
	}) {
		return
	}
	// G1-05：用模拟节点状态和正式绑定接口验证授权、幂等与资源预留。
	if !t.Run("G1-05_binding", func(t *testing.T) {
		f.t = t
		for _, name := range []string{f.node, f.other} {
			f.call("POST", "nodes", admin, object{"apiVersion": "mini.k8s/v1", "kind": "Node", "metadata": object{"name": name}, "spec": object{"allocatable": object{"cpuMilli": 1000, "memoryBytes": 100000}, "podCIDR": "10.8.0.0/24"}}, 201)
		}
		n := f.call("GET", "nodes/"+f.node, admin, nil, 200)
		f.call("PUT", "nodes/"+f.node+"/status", agent, object{"resourceVersion": resourceVersionOf(n), "status": object{"ready": true, "reserved": object{"cpuMilli": 0, "memoryBytes": 0}}}, 200)
		p := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		req := object{"nodeName": f.node, "nodeUID": uidOf(n), "podUID": uidOf(p), "resourceVersion": resourceVersionOf(p)}
		f.call("POST", "pods/"+f.pod+"/bind", admin, req, 403)
		f.call("POST", "pods/"+f.pod+"/bind", scheduler, req, 200)
		f.call("POST", "pods/"+f.pod+"/bind", scheduler, req, 200)
		n = f.call("GET", "nodes/"+f.node, admin, nil, 200)
		st := object(n["status"].(map[string]any))
		reserved := object(st["reserved"].(map[string]any))
		if reserved["cpuMilli"] != float64(100) || reserved["memoryBytes"] != float64(1024) {
			t.Fatalf("unexpected reservation: %v", reserved)
		}
		p = f.call("GET", "pods/"+f.pod, admin, nil, 200)
		before := p["spec"]
		f.call("PUT", "pods/"+f.pod+"/status", agent, object{"resourceVersion": resourceVersionOf(p), "status": object{"phase": "Running"}}, 200)
		p = f.call("GET", "pods/"+f.pod, admin, nil, 200)
		if fmt.Sprint(before) != fmt.Sprint(p["spec"]) {
			t.Fatal("status update modified spec")
		}
		f.call("DELETE", "pods/"+f.pod, admin, nil, 200)
		p = f.call("GET", "pods/"+f.pod, admin, nil, 200)
		req["resourceVersion"] = resourceVersionOf(p)
		f.call("POST", "pods/"+f.pod+"/bind", scheduler, req, 409)
	}) {
		return
	}
	// G1-06：验证缺少凭据、跨节点越权，以及日志中的必要字段和脱敏。
	t.Run("G1-06_auth_log", func(t *testing.T) {
		f.t = t
		f.call("GET", "pods", "", nil, 401)
		n := f.call("GET", "nodes/"+f.node, admin, nil, 200)
		f.call("PUT", "nodes/"+f.node+"/status", otherAgent, object{"resourceVersion": resourceVersionOf(n), "status": object{"ready": true}}, 403)
		data, err := os.ReadFile(f.log.Name())
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, secret := range []string{admin, scheduler, agent, otherAgent, sensitive} {
			if strings.Contains(text, secret) {
				t.Fatal("API log exposed a credential or env value")
			}
		}
		for _, field := range []string{"requestId=", "identity=", "uid=", "result="} {
			if !strings.Contains(text, field) {
				t.Fatalf("API log missing %s", field)
			}
		}
		if !strings.Contains(text, "result=403") || !strings.Contains(text, "result=200") {
			t.Fatal("API log missing success or failure result")
		}
	})
}
