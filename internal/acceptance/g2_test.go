package acceptance

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const controllerToken = "g2-controller-secret"

// faultProxy 只拦截一次 Controller 的真实 Pod 创建；其余请求原样转发。
type faultProxy struct {
	mu      sync.Mutex
	mode    string
	seen    chan int
	release chan struct{}
	backend *url.URL
	proxy   *httputil.ReverseProxy
}

func (p *faultProxy) arm(mode string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode = mode
	p.seen = make(chan int, 1)
	p.release = make(chan struct{})
}

func (p *faultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	mode, seen, release := "", p.seen, p.release
	if r.Method == http.MethodPost && r.URL.Path == "/api/mini/v1/pods" && p.mode != "" {
		mode, p.mode = p.mode, ""
	}
	p.mu.Unlock()
	if mode == "pause" {
		seen <- 0
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
	}
	if mode != "drop" {
		p.proxy.ServeHTTP(w, r)
		return
	}
	// 真正提交后才断开客户端连接；若 API 未返回 201，测试不能通过。
	req := r.Clone(r.Context())
	req.URL.Scheme, req.URL.Host = p.backend.Scheme, p.backend.Host
	req.RequestURI = ""
	resp, err := p.proxy.Transport.RoundTrip(req)
	if err != nil {
		seen <- 0
		http.Error(w, "backend request failed", http.StatusBadGateway)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		seen <- 0
		http.Error(w, "cannot drop response", http.StatusInternalServerError)
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		seen <- 0
		return
	}
	if err := conn.Close(); err != nil {
		seen <- 0
		return
	}
	seen <- resp.StatusCode
}

type g2Fixture struct {
	base          *g1Fixture
	t             *testing.T
	managerBinary string
	managerToken  string
	managerLog    *os.File
	manager       *exec.Cmd
	proxy         *faultProxy
	proxyServer   *httptest.Server
	sets          map[string]string // 本轮 ReplicaSet 名称及 UID
	node          string
	standalone    string
}

func setupG2(t *testing.T) *g2Fixture {
	t.Helper()
	binary := os.Getenv("G2_MANAGER_BINARY")
	if binary == "" {
		t.Skip("set G2_MANAGER_BINARY and the G1 real-etcd variables to run G2")
	}
	f := setupG1(t)
	g := &g2Fixture{base: f, t: t, managerBinary: binary, sets: map[string]string{}, node: f.node + "-g2", standalone: f.pod + "-independent"}
	// 节点及 Controller 使用独立身份，状态和绑定仍经正式 API。
	f.stop()
	credentials, _ := json.Marshal(object{"admin": admin, "scheduler": scheduler, "controller": controllerToken, "agents": object{f.node: agent, f.other: otherAgent, g.node: agent + "-g2"}})
	if err := os.WriteFile(f.auth, credentials, 0600); err != nil {
		t.Fatal(err)
	}
	f.start()
	g.managerToken = filepath.Join(t.TempDir(), "controller-token")
	if err := os.WriteFile(g.managerToken, []byte(controllerToken), 0600); err != nil {
		t.Fatal(err)
	}
	backend, err := url.Parse(f.url)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(backend)
	proxy.Transport = f.client.Transport
	proxy.FlushInterval = -1
	g.proxy = &faultProxy{backend: backend, proxy: proxy}
	server := httptest.NewUnstartedServer(g.proxy)
	certificate, err := tls.LoadX509KeyPair(f.cert, f.key)
	if err != nil {
		t.Fatal(err)
	}
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	g.proxyServer = server
	logPath := os.Getenv("G2_MANAGER_LOG")
	if logPath == "" {
		logPath = filepath.Join(t.TempDir(), "manager.log")
	}
	g.managerLog, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		g.t = t
		g.base.t = t
		g.cleanup()
		g.stopManager()
		g.proxyServer.Close()
		_ = g.managerLog.Close()
	})
	g.startManager()
	return g
}

func (g *g2Fixture) startManager() {
	g.t.Helper()
	g.manager = exec.Command(g.managerBinary, "-server", g.proxyServer.URL, "-token-file", g.managerToken, "-ca-file", g.base.cert, "-resync", "2s")
	g.manager.Stdout, g.manager.Stderr = g.managerLog, g.managerLog
	if err := g.manager.Start(); err != nil {
		g.t.Fatal(err)
	}
}

func (g *g2Fixture) stopManager() {
	if g.manager != nil && g.manager.Process != nil {
		_ = g.manager.Process.Kill()
		_ = g.manager.Wait()
		g.manager = nil
	}
}

func (g *g2Fixture) createSet(suffix string, replicas int) (string, string) {
	g.t.Helper()
	name := g.base.rs + "-" + suffix
	manifest := replicaSetManifest(name)
	manifest["spec"].(object)["replicas"] = replicas
	created := g.base.call("POST", "replicasets", admin, manifest, 201)
	uid := uidOf(created)
	g.sets[name] = uid
	return name, uid
}

func (g *g2Fixture) scale(name string, replicas int) {
	g.t.Helper()
	rs := g.base.call("GET", "replicasets/"+name, admin, nil, 200)
	specOf(rs)["replicas"] = replicas
	g.base.call("PUT", "replicasets/"+name, admin, rs, 200)
}

func (g *g2Fixture) owned(uid string) []object {
	g.t.Helper()
	list := g.base.call("GET", "pods", admin, nil, 200)
	var pods []object
	for _, raw := range list["items"].([]any) {
		pod := object(raw.(map[string]any))
		if metadataOf(pod)["ownerUID"] == uid {
			pods = append(pods, pod)
		}
	}
	return pods
}

func (g *g2Fixture) waitCount(uid string, want int, timeout time.Duration) []object {
	g.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods := g.owned(uid)
		if len(pods) == want {
			return pods
		}
		time.Sleep(100 * time.Millisecond)
	}
	g.t.Fatalf("owner %s did not converge to %d Pods in %s", uid, want, timeout)
	return nil
}

func (g *g2Fixture) deleteSet(name, uid string) {
	g.t.Helper()
	g.base.call("DELETE", "replicasets/"+name, admin, nil, 200)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, code, err := g.base.request("GET", "replicasets/"+name, admin, nil)
		if err == nil && code == 404 && len(g.owned(uid)) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	g.t.Fatalf("ReplicaSet %s or owned Pods remain after deletion", name)
}

// 清理只处理本轮 owner UID；已绑定 Pod 先通过模拟节点确认停止。
func (g *g2Fixture) cleanup() {
	g.base.t = g.t
	// 注入竞态时若断言提前失败，先释放被暂停的创建，避免阻塞清理。
	if g.proxy.release != nil {
		select {
		case <-g.proxy.release:
		default:
			close(g.proxy.release)
		}
	}
	_, code, err := g.base.request("DELETE", "pods/"+g.standalone, admin, nil)
	if err != nil || (code != 200 && code != 404) {
		g.t.Errorf("cleanup independent Pod: HTTP %d %v", code, err)
	}
	_, code, err = g.base.request("GET", "pods/"+g.standalone, admin, nil)
	if err != nil || code != 404 {
		g.t.Errorf("independent Pod remains after cleanup: HTTP %d %v", code, err)
	}
	for name, uid := range g.sets {
		// 先停止补建，再删除子 Pod；否则清理本身可能触发新的对账。
		_, code, err := g.base.request("DELETE", "replicasets/"+name, admin, nil)
		if err != nil || (code != 200 && code != 404) {
			g.t.Errorf("cleanup ReplicaSet %s: HTTP %d %v", name, code, err)
		}
		for _, pod := range g.owned(uid) {
			podName := metadataOf(pod)["name"].(string)
			_, code, err := g.base.request("DELETE", "pods/"+podName, admin, nil)
			if err != nil || (code != 200 && code != 404) {
				g.t.Errorf("cleanup Pod %s: HTTP %d %v", podName, code, err)
				continue
			}
			if pod["nodeUID"] != nil && pod["nodeUID"] != "" {
				b, code, err := g.base.request("GET", "pods/"+podName, admin, nil)
				if err == nil && code == 200 {
					var current object
					_ = json.Unmarshal(b, &current)
					_, code, err = g.base.request("POST", "pods/"+podName+"/cleanup", agent+"-g2", object{"uid": uidOf(current), "resourceVersion": resourceVersionOf(current)})
					if err != nil || (code != 200 && code != 404) {
						g.t.Errorf("cleanup bound Pod %s: HTTP %d %v", podName, code, err)
					}
				}
			}
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			_, code, err = g.base.request("GET", "replicasets/"+name, admin, nil)
			if err == nil && code == 404 && len(g.owned(uid)) == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if code != 404 || len(g.owned(uid)) != 0 {
			g.t.Errorf("cleanup incomplete for %s", name)
		}
	}
	// 仅在不存在本轮所属 Pod 时删除模拟节点，保留失败现场的预留信息。
	clean := true
	for _, uid := range g.sets {
		if len(g.owned(uid)) != 0 {
			clean = false
		}
	}
	if clean {
		_, code, err := g.base.request("DELETE", "nodes/"+g.node, admin, nil)
		if err != nil || (code != 200 && code != 404) {
			g.t.Errorf("cleanup node: HTTP %d %v", code, err)
		}
		_, code, err = g.base.request("GET", "nodes/"+g.node, admin, nil)
		if err != nil || code != 404 {
			g.t.Errorf("node remains after cleanup: HTTP %d %v", code, err)
		}
	} else {
		g.t.Error("retaining simulated node because owned Pods remain")
	}
}

func TestG2(t *testing.T) {
	g := setupG2(t)
	f := g.base
	run := func(name string, fn func(*testing.T)) bool {
		return t.Run(name, func(t *testing.T) { g.t, f.t = t, t; fn(t) })
	}

	// G2-01：每一步检查实际 owner UID 和数量，未绑定 Pod 应能删除。
	if !run("G2-01_scaling", func(t *testing.T) {
		name, uid := g.createSet("scale", 1)
		g.waitCount(uid, 1, 10*time.Second)
		for _, count := range []int{3, 1, 0} {
			g.scale(name, count)
			g.waitCount(uid, count, 10*time.Second)
		}
		g.deleteSet(name, uid)
	}) {
		return
	}

	// G2-02：重复对账及进程重启后，连续 30 秒检查没有多余副本。
	if !run("G2-02_restart", func(t *testing.T) {
		name, uid := g.createSet("restart", 3)
		g.waitCount(uid, 3, 10*time.Second)
		g.stopManager()
		g.startManager()
		g.waitCount(uid, 3, 10*time.Second)
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if count := len(g.owned(uid)); count != 3 {
				t.Fatalf("restart observation saw %d owned Pods, want 3", count)
			}
			time.Sleep(200 * time.Millisecond)
		}
		g.deleteSet(name, uid)
	}) {
		return
	}

	// G2-03：同标签独立 Pod 不属于 RS，删除 RS 也不能删除该 Pod。
	if !run("G2-03_ownership", func(t *testing.T) {
		name := f.rs + "-ownership"
		independent := podManifest(g.standalone)
		independent["metadata"].(object)["labels"] = object{"app": name}
		created := f.call("POST", "pods", admin, independent, 201)
		independentUID := uidOf(created)
		_, uid := g.createSet("ownership", 1)
		g.waitCount(uid, 1, 10*time.Second)
		g.deleteSet(name, uid)
		if uidOf(f.call("GET", "pods/"+g.standalone, admin, nil, 200)) != independentUID {
			t.Fatal("independent Pod was replaced")
		}
		f.call("DELETE", "pods/"+g.standalone, admin, nil, 200)
	}) {
		return
	}

	// G2-04：代理等待真实 API 返回 201 后断开连接，验证丢响应不会多建。
	if !run("G2-04_lost_response", func(t *testing.T) {
		g.proxy.arm("drop")
		name, uid := g.createSet("lost", 1)
		select {
		case status := <-g.proxy.seen:
			if status != 201 {
				t.Fatalf("intercepted create returned %d, expected committed 201", status)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("controller did not issue a Pod create")
		}
		g.waitCount(uid, 1, 30*time.Second)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if n := len(g.owned(uid)); n != 1 {
				t.Fatalf("lost response produced %d Pods", n)
			}
			time.Sleep(100 * time.Millisecond)
		}
		g.deleteSet(name, uid)
	}) {
		return
	}

	// G2-05：在 API 前暂停真实请求，先确认 RS 已标记删除，再放行请求。
	if !run("G2-05_delete_race", func(t *testing.T) {
		name, uid := g.createSet("race", 1)
		g.waitCount(uid, 1, 10*time.Second)
		g.proxy.arm("pause")
		g.scale(name, 2)
		select {
		case <-g.proxy.seen:
		case <-time.After(10 * time.Second):
			t.Fatal("controller create was not paused")
		}
		f.call("DELETE", "replicasets/"+name, admin, nil, 200)
		rs := f.call("GET", "replicasets/"+name, admin, nil, 200)
		if metadataOf(rs)["deletionTimestamp"] == nil {
			t.Fatal("ReplicaSet has not been marked deleting")
		}
		close(g.proxy.release)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			_, code, err := f.request("GET", "replicasets/"+name, admin, nil)
			if err == nil && code == 404 && len(g.owned(uid)) == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_, code, err := f.request("GET", "replicasets/"+name, admin, nil)
		if err != nil || code != 404 || len(g.owned(uid)) != 0 {
			t.Fatal("delete race left ReplicaSet or orphan Pods")
		}
		deadline = time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if n := len(g.owned(uid)); n != 0 {
				t.Fatalf("delete race left %d orphan Pods", n)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}) {
		return
	}

	// G2-06：Unknown 且尚未确认清理的绑定 Pod 始终计入防重复额度。
	run("G2-06_unconfirmed_stop", func(t *testing.T) {
		f.call("POST", "nodes", admin, object{"apiVersion": "mini.k8s/v1", "kind": "Node", "metadata": object{"name": g.node}, "spec": object{"allocatable": object{"cpuMilli": 1000, "memoryBytes": 100000}, "podCIDR": "10.9.0.0/24"}}, 201)
		name, uid := g.createSet("unknown", 1)
		pod := g.waitCount(uid, 1, 10*time.Second)[0]
		podName := metadataOf(pod)["name"].(string)
		// 模拟 Agent 在绑定前上报心跳，等待 Pod 创建的时间不消耗节点有效期。
		node := f.call("GET", "nodes/"+g.node, admin, nil, 200)
		f.call("PUT", "nodes/"+g.node+"/status", agent+"-g2", object{"resourceVersion": resourceVersionOf(node), "status": object{"ready": true, "reserved": object{"cpuMilli": 0, "memoryBytes": 0}}}, 200)
		f.call("POST", "pods/"+podName+"/bind", scheduler, object{"nodeName": g.node, "nodeUID": uidOf(node), "podUID": uidOf(pod), "resourceVersion": resourceVersionOf(pod)}, 200)
		for _, phase := range []string{"Running", "Unknown"} {
			pod = f.call("GET", "pods/"+podName, admin, nil, 200)
			f.call("PUT", "pods/"+podName+"/status", agent+"-g2", object{"resourceVersion": resourceVersionOf(pod), "status": object{"phase": phase}}, 200)
		}
		f.call("DELETE", "pods/"+podName, admin, nil, 200)
		g.scale(name, 0)
		g.scale(name, 1)
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			pods := g.owned(uid)
			if len(pods) != 1 || uidOf(pods[0]) != uidOf(pod) {
				t.Fatal("Unknown Pod did not retain its quota")
			}
			time.Sleep(100 * time.Millisecond)
		}
		g.scale(name, 3)
		g.waitCount(uid, 3, 10*time.Second)
		pod = f.call("GET", "pods/"+podName, admin, nil, 200)
		f.call("POST", "pods/"+podName+"/cleanup", agent+"-g2", object{"uid": uidOf(pod), "resourceVersion": resourceVersionOf(pod)}, 200)
		g.waitCount(uid, 3, 10*time.Second)
		g.deleteSet(name, uid)
	})
}
