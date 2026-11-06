package acceptance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"mini-k8s/internal/server"
)

const g3NodeToken = "g3-test-node-token"
const g3ControllerToken = "g3-test-controller-token"

type g3Fixture struct {
	t              *testing.T
	api            *httptest.Server
	store          *clientv3.Client
	client         *http.Client
	agentBinary    string
	agentProcess   *exec.Cmd
	agentExit      chan error
	managerBinary  string
	managerProcess *exec.Cmd
	managerExit    chan error
	managerToken   string
	agentLog       *os.File
	caFile         string
	tokenFile      string
	stateDir       string
	controlDir     string
	controlSocket  string
	logDir         string
	criSocket      string
	image          string
	podCIDRText    string
	podCIDR        *net.IPNet
	evidenceDir    string
	nodeName       string
	rsName         string
	podName        string
	podUID         string
	rsUID          string
	marker         string
	extraPods      map[string]string
	deadline       time.Time
	leases         map[string]g3Lease
	keepState      bool
	stateRoot      string
	boundAt        time.Time
}

type g3Lease struct{ UID, IP, SandboxID, Path string }

// Pod 网段来自实际 CNI 配置；不接受 IPv6 或无法解析的占位值。
func parseG3PodCIDR(value string) (*net.IPNet, error) {
	prefix, err := netip.ParsePrefix(value)
	if err != nil || !prefix.Addr().Is4() {
		return nil, fmt.Errorf("valid IPv4 CIDR required: %q", value)
	}
	ip, network, err := net.ParseCIDR(value)
	if err != nil {
		return nil, err
	}
	_, bits := network.Mask.Size()
	if ip.To4() == nil || bits != 32 {
		return nil, fmt.Errorf("IPv4 CIDR required")
	}
	return network, nil
}

func TestG3PodCIDR(t *testing.T) {
	for _, check := range []struct {
		value, address  string
		valid, contains bool
	}{
		{"10.88.0.0/24", "10.88.0.42", true, true},
		{"10.88.0.7/24", "10.88.1.1", true, false},
		{"", "", false, false},
		{"10.88.0.0", "", false, false},
		{"10.88.0.0/33", "", false, false},
		{"10.88.0.0/024", "", false, false},
		{"10.88.0.0/00", "", false, false},
		{"10.088.0.0/24", "", false, false},
		{"fd00::/64", "", false, false},
		{"::ffff:10.88.0.1/120", "", false, false},
	} {
		network, err := parseG3PodCIDR(check.value)
		if (err == nil) != check.valid {
			t.Errorf("CIDR %q: error %v", check.value, err)
			continue
		}
		if err == nil && network.Contains(net.ParseIP(check.address)) != check.contains {
			t.Errorf("CIDR %q membership of %q", check.value, check.address)
		}
	}
}

// 验收仅连接本机 etcd 和 Unix CRI socket；API 由测试启动并连接真实 etcd。
func setupG3(t *testing.T) *g3Fixture {
	t.Helper()
	endpoint := os.Getenv("G3_ETCD_ENDPOINT")
	agentBinary := os.Getenv("G3_AGENT_BINARY")
	managerBinary := os.Getenv("G3_MANAGER_BINARY")
	socket := os.Getenv("G3_CRI_ENDPOINT")
	image := os.Getenv("G3_TEST_IMAGE")
	podCIDRText := os.Getenv("G3_POD_CIDR")
	if endpoint == "" || agentBinary == "" || managerBinary == "" || socket == "" || image == "" {
		t.Skip("G3 requires real local etcd, mini-agent, mini-manager, CRI socket and pinned image")
	}
	podCIDR, err := parseG3PodCIDR(podCIDRText)
	if err != nil {
		t.Skipf("G3_POD_CIDR unavailable: %v", err)
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") || u.Scheme != "http" {
		t.Fatal("G3_ETCD_ENDPOINT must be a local HTTP endpoint")
	}
	if !strings.HasPrefix(socket, "unix:///") {
		t.Fatal("G3_CRI_ENDPOINT must be a local absolute Unix socket")
	}
	info, err := os.Stat(strings.TrimPrefix(socket, "unix://"))
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("CRI endpoint is not an accessible Unix socket: %v", err)
	}
	if !regexp.MustCompile(`@sha256:[a-f0-9]{64}$`).MatchString(image) {
		t.Fatal("G3_TEST_IMAGE must use a sha256 digest")
	}
	sandboxImage, err := g3RuntimePrerequisites(socket, image)
	if err != nil {
		t.Skipf("G3 runtime prerequisites unavailable: %v", err)
	}
	t.Logf("containerd sandbox image is present locally: %s", sandboxImage)
	store, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	g := &g3Fixture{t: t, store: store, agentBinary: agentBinary, managerBinary: managerBinary, criSocket: socket, image: image, podCIDRText: podCIDRText, podCIDR: podCIDR, evidenceDir: os.Getenv("G3_EVIDENCE_DIR"), nodeName: "g3-node-" + suffix, rsName: "g3-rs-" + suffix, podName: "g3-pod-" + suffix, marker: "G3-READY-" + suffix}
	handler := &server.Server{Client: store, Credentials: server.Credentials{Admin: admin, Scheduler: scheduler, Controller: g3ControllerToken, Agents: map[string]string{g.nodeName: g3NodeToken}}}
	g.api = httptest.NewTLSServer(handler)
	g.client = g.api.Client()
	g.client.Timeout = 5 * time.Second
	// 现场保存到证据目录，不使用 t.TempDir 的自动删除机制。
	if g.evidenceDir == "" {
		t.Fatal("G3_EVIDENCE_DIR is required to preserve failure state")
	}
	dir, err := os.MkdirTemp(g.evidenceDir, "g3-state-")
	if err != nil {
		t.Fatal(err)
	}
	g.stateRoot = dir
	g.caFile, g.tokenFile = filepath.Join(dir, "ca.pem"), filepath.Join(dir, "node-token")
	g.stateDir, g.logDir = filepath.Join(dir, "state"), filepath.Join(dir, "logs")
	g.extraPods = make(map[string]string)
	g.leases = make(map[string]g3Lease)
	if err = os.WriteFile(g.caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: g.api.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(g.tokenFile, []byte(g3NodeToken), 0600); err != nil {
		t.Fatal(err)
	}
	g.managerToken = filepath.Join(dir, "controller-token")
	if err = os.WriteFile(g.managerToken, []byte(g3ControllerToken), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := os.Getenv("G3_AGENT_LOG")
	if logPath == "" {
		logPath = filepath.Join(dir, "agent.log")
	}
	g.agentLog, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Unix socket 的路径长度有限；证据目录可能很深，因此使用独立的私有短目录。
	g.controlDir, err = os.MkdirTemp("/tmp", "mini-k8s-g3-")
	if err != nil {
		_ = g.agentLog.Close()
		t.Fatal(err)
	}
	g.controlSocket = filepath.Join(g.controlDir, "control.sock")
	t.Cleanup(func() {
		g.t = t
		g.stopManager()
		g.cleanup()
		g.stopAgent()
		g.api.Close()
		_ = g.store.Close()
		_ = g.agentLog.Close()
		if t.Failed() || g.keepState {
			t.Logf("Preserved runtime state and UID information: %s", g.stateRoot)
			t.Logf("Preserved control socket directory: %s", g.controlDir)
		} else {
			if err := os.RemoveAll(g.stateRoot); err != nil {
				t.Errorf("remove successful test state: %v", err)
			}
			if err := os.RemoveAll(g.controlDir); err != nil {
				t.Errorf("remove successful test control directory: %v", err)
			}
		}
	})
	return g
}

// 只读检查 CRI 就绪和 sandbox 镜像是否已在本机，绝不主动拉取镜像。
func g3RuntimePrerequisites(socket, image string) (string, error) {
	run := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		args = append([]string{"--runtime-endpoint", socket, "--image-endpoint", socket}, args...)
		return exec.CommandContext(ctx, "crictl", args...).CombinedOutput()
	}
	data, err := run("info")
	if err != nil {
		return "", fmt.Errorf("CRI info: %w", err)
	}
	var info struct {
		Status struct {
			Conditions []struct {
				Type   string `json:"type"`
				Status bool   `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
		Config struct {
			SandboxImage string `json:"sandboxImage"`
		} `json:"config"`
	}
	if err = json.Unmarshal(data, &info); err != nil {
		return "", fmt.Errorf("invalid CRI info: %w", err)
	}
	ready := map[string]bool{}
	for _, condition := range info.Status.Conditions {
		ready[condition.Type] = condition.Status
	}
	if !ready["RuntimeReady"] || !ready["NetworkReady"] {
		return "", fmt.Errorf("RuntimeReady and NetworkReady are required")
	}
	config := os.Getenv("G3_CONTAINERD_CONFIG")
	sandboxImage, usedConfig, err := selectG3SandboxImage(info.Config.SandboxImage, func() (string, error) {
		if config == "" {
			return "", fmt.Errorf("CRI info omits sandboxImage; set G3_CONTAINERD_CONFIG to the absolute configuration path used by the target containerd daemon")
		}
		if !filepath.IsAbs(config) || strings.ContainsAny(config, "\r\n") {
			return "", fmt.Errorf("G3_CONTAINERD_CONFIG must be an absolute local file path")
		}
		stat, err := os.Stat(config)
		if err != nil {
			return "", fmt.Errorf("read containerd configuration %q: %w", config, err)
		}
		if !stat.Mode().IsRegular() {
			return "", fmt.Errorf("containerd configuration %q is not a regular file", config)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		dump, err := exec.CommandContext(ctx, "containerd", "--config", config, "config", "dump").Output()
		cancel()
		if err != nil {
			return "", fmt.Errorf("containerd config dump for %q failed: %w", config, err)
		}
		sandboxImage, err := parseG3PinnedSandboxImage(string(dump))
		if err != nil {
			return "", fmt.Errorf("containerd configuration %q: %w", config, err)
		}
		return sandboxImage, nil
	})
	if err != nil {
		return "", err
	}
	if usedConfig {
		fmt.Printf("G3 sandbox image source: containerd config dump %s (must be the active daemon configuration)\n", config)
	} else {
		fmt.Println("G3 sandbox image source: CRI info config.sandboxImage")
	}
	for _, ref := range []string{image, sandboxImage} {
		if _, err = run("inspecti", ref); err != nil {
			return "", fmt.Errorf("CRI inspecti %q failed; image must be present locally: %w", ref, err)
		}
	}
	return sandboxImage, nil
}

// 1.x CRI 字段优先；只有字段缺失时才读取明确指定的 2.x 配置。
func selectG3SandboxImage(criImage string, fallback func() (string, error)) (string, bool, error) {
	image, usedConfig := criImage, false
	if image == "" {
		usedConfig = true
		var err error
		image, err = fallback()
		if err != nil {
			return "", usedConfig, err
		}
	}
	if !g3ImageReference.MatchString(image) {
		return "", usedConfig, fmt.Errorf("invalid configured sandbox image reference")
	}
	return image, usedConfig, nil
}

var (
	g3PinnedImagesTable = regexp.MustCompile(`^\s*\[\s*plugins\s*\.\s*(?:'io\.containerd\.cri\.v1\.images'|"io\.containerd\.cri\.v1\.images")\s*\.\s*pinned_images\s*\]\s*(?:#.*)?$`)
	g3SandboxKey        = regexp.MustCompile(`^\s*sandbox\s*=`)
	g3SandboxValue      = regexp.MustCompile(`^\s*sandbox\s*=\s*(?:'([^']*)'|\"([^\"]*)\")\s*(?:#.*)?$`)
	g3ImageReference    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@-]*$`)
)

// containerd dump 已由 containerd 本身校验；这里只接受明确表和不含转义的镜像引用。
func parseG3PinnedSandboxImage(dump string) (string, error) {
	scanner := bufio.NewScanner(strings.NewReader(dump))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	inside, seenTable, seenValue := false, false, false
	value := ""
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") {
			inside = g3PinnedImagesTable.MatchString(line)
			if inside {
				if seenTable {
					return "", fmt.Errorf("duplicate pinned_images table")
				}
				seenTable = true
			}
			continue
		}
		if !inside || !g3SandboxKey.MatchString(line) {
			continue
		}
		if seenValue {
			return "", fmt.Errorf("duplicate pinned sandbox image")
		}
		matches := g3SandboxValue.FindStringSubmatch(line)
		if matches == nil {
			return "", fmt.Errorf("unsupported pinned sandbox image syntax")
		}
		value = matches[1]
		if value == "" {
			value = matches[2]
		}
		if !g3ImageReference.MatchString(value) {
			return "", fmt.Errorf("invalid pinned sandbox image reference")
		}
		seenValue = true
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan config dump: %w", err)
	}
	if !seenTable || !seenValue {
		return "", fmt.Errorf("plugins.io.containerd.cri.v1.images.pinned_images.sandbox is absent")
	}
	return value, nil
}

func TestG3PinnedSandboxImage(t *testing.T) {
	for _, tc := range []struct {
		name, dump, want string
		invalid          bool
	}{
		{"containerd 2 literal", "[plugins.'io.containerd.cri.v1.images'.pinned_images]\n sandbox = 'registry.k8s.io/pause:3.10.1'\n", "registry.k8s.io/pause:3.10.1", false},
		{"double quotes and comment", "[plugins.\"io.containerd.cri.v1.images\".pinned_images] # section\n sandbox = \"localhost:5000/pause:1\" # image\n", "localhost:5000/pause:1", false},
		{"ignore other sections", "[other]\nsandbox='wrong'\n[plugins.'io.containerd.cri.v1.images'.pinned_images]\nsandbox='right:1'\n[after]\nsandbox='wrong'\n", "right:1", false},
		{"wrong plugin", "[plugins.'other'.pinned_images]\nsandbox='wrong'\n", "", true},
		{"missing sandbox", "[plugins.'io.containerd.cri.v1.images'.pinned_images]\nother='value'\n", "", true},
		{"duplicate key", "[plugins.'io.containerd.cri.v1.images'.pinned_images]\nsandbox='a'\nsandbox='b'\n", "", true},
		{"duplicate section", "[plugins.'io.containerd.cri.v1.images'.pinned_images]\nsandbox='a'\n[plugins.'io.containerd.cri.v1.images'.pinned_images]\nsandbox='b'\n", "", true},
		{"unquoted shell text", "[plugins.'io.containerd.cri.v1.images'.pinned_images]\nsandbox=$(cmd)\n", "", true},
		{"invalid reference", "[plugins.'io.containerd.cri.v1.images'.pinned_images]\nsandbox='registry/pause?token=secret'\n", "", true},
		{"empty reference", "[plugins.'io.containerd.cri.v1.images'.pinned_images]\nsandbox=''\n", "", true},
		{"mismatched table quotes", "[plugins.'io.containerd.cri.v1.images\".pinned_images]\nsandbox='a'\n", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseG3PinnedSandboxImage(tc.dump)
			if (err != nil) != tc.invalid || got != tc.want {
				t.Fatalf("got %q, error %v", got, err)
			}
		})
	}
}

func TestG3SandboxImageSelection(t *testing.T) {
	calls := 0
	fallback := func() (string, error) { calls++; return "registry.k8s.io/pause:3.10.1", nil }
	image, used, err := selectG3SandboxImage("registry.k8s.io/pause:3.9", fallback)
	if err != nil || used || calls != 0 || image != "registry.k8s.io/pause:3.9" {
		t.Fatalf("1.x CRI field not preferred: %q %t %d %v", image, used, calls, err)
	}
	image, used, err = selectG3SandboxImage("", fallback)
	if err != nil || !used || calls != 1 || image != "registry.k8s.io/pause:3.10.1" {
		t.Fatalf("2.x fallback failed: %q %t %d %v", image, used, calls, err)
	}
	_, _, err = selectG3SandboxImage("bad?reference", fallback)
	if err == nil || calls != 1 {
		t.Fatalf("invalid CRI field incorrectly fell back: %d %v", calls, err)
	}
	_, _, err = selectG3SandboxImage("", func() (string, error) { return "", fmt.Errorf("config unreadable") })
	if err == nil || !strings.Contains(err.Error(), "config unreadable") {
		t.Fatalf("fallback error was not preserved: %v", err)
	}
}

func (g *g3Fixture) request(method, path, token string, payload any) (object, int, error) {
	var body *bytes.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		body = bytes.NewReader(b)
	} else {
		body = bytes.NewReader(nil)
	}
	ctx, cancel := g.operationContext(5 * time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, g.api.URL+"/api/mini/v1/"+path, body)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var out object
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, resp.StatusCode, err
	}
	return out, resp.StatusCode, nil
}

func (g *g3Fixture) call(method, path, token string, payload any, want int) object {
	g.t.Helper()
	out, code, err := g.request(method, path, token, payload)
	if err != nil || code != want {
		g.t.Fatalf("%s %s: HTTP %d, %v, want %d", method, path, code, err, want)
	}
	return out
}

func (g *g3Fixture) startAgent() {
	g.t.Helper()
	g.agentProcess = exec.Command(g.agentBinary, "-server", g.api.URL, "-node", g.nodeName, "-token-file", g.tokenFile, "-ca-file", g.caFile, "-cri-endpoint", g.criSocket, "-state-dir", g.stateDir, "-control-socket", g.controlSocket, "-log-dir", g.logDir, "-resync", "2s")
	g.agentProcess.Stdout, g.agentProcess.Stderr = g.agentLog, g.agentLog
	if err := g.agentProcess.Start(); err != nil {
		g.t.Fatal(err)
	}
	g.agentExit = make(chan error, 1)
	process := g.agentProcess
	go func() { g.agentExit <- process.Wait() }()
}

func (g *g3Fixture) stopAgent() {
	if g.agentProcess != nil && g.agentProcess.Process != nil {
		_ = g.agentProcess.Process.Kill()
		<-g.agentExit
		g.agentProcess = nil
	}
}

func (g *g3Fixture) startManager() {
	g.t.Helper()
	g.managerProcess = exec.Command(g.managerBinary, "-server", g.api.URL, "-token-file", g.managerToken, "-ca-file", g.caFile, "-resync", "2s")
	g.managerProcess.Stdout, g.managerProcess.Stderr = g.agentLog, g.agentLog
	if err := g.managerProcess.Start(); err != nil {
		g.t.Fatal(err)
	}
	g.managerExit = make(chan error, 1)
	process := g.managerProcess
	go func() { g.managerExit <- process.Wait() }()
}

func (g *g3Fixture) stopManager() {
	if g.managerProcess != nil && g.managerProcess.Process != nil {
		_ = g.managerProcess.Process.Kill()
		<-g.managerExit
		g.managerProcess = nil
	}
}

func (g *g3Fixture) requireAlive(process *exec.Cmd, exit <-chan error, label string) {
	g.t.Helper()
	if process == nil {
		g.t.Fatalf("%s is not running", label)
	}
	select {
	case err := <-exit:
		// 保留已退出状态供后续清理读取，避免再次等待阻塞。
		if label == "Agent" {
			g.agentExit <- err
		} else {
			g.managerExit <- err
		}
		g.t.Fatalf("%s exited unexpectedly: %v", label, err)
	default:
	}
}

func (g *g3Fixture) operationContext(limit time.Duration) (context.Context, context.CancelFunc) {
	if !g.deadline.IsZero() && time.Until(g.deadline) < limit {
		return context.WithDeadline(context.Background(), g.deadline)
	}
	return context.WithTimeout(context.Background(), limit)
}

func (g *g3Fixture) cri(args ...string) ([]byte, error) {
	ctx, cancel := g.operationContext(5 * time.Second)
	defer cancel()
	args = append([]string{"--runtime-endpoint", g.criSocket, "--image-endpoint", g.criSocket}, args...)
	out, err := exec.CommandContext(ctx, "crictl", args...).CombinedOutput()
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return out, fmt.Errorf("crictl deadline: %w", ctx.Err())
	}
	return out, err
}

func TestG3CRIDeadlineIsReported(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires a Linux executable")
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "crictl")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec /bin/sleep 5\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	g := &g3Fixture{deadline: time.Now().Add(100 * time.Millisecond)}
	if _, err := g.cri("stop", "test-container"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected distinguishable deadline error, got %v", err)
	}
}

// 筛选双标签后才保存证据或操作容器，避免触碰同机其他工作负载。
func (g *g3Fixture) criObjects(command, key, uid string) ([]object, error) {
	args := []string{command, "-a", "-o", "json"}
	if command == "pods" {
		args = []string{command, "-o", "json"}
	}
	b, err := g.cri(args...)
	if err != nil {
		return nil, fmt.Errorf("crictl %s: %w: %s", command, err, b)
	}
	var result map[string]json.RawMessage
	if err = json.Unmarshal(b, &result); err != nil {
		return nil, err
	}
	var rows []object
	raw, exists := result[key]
	if !exists || len(raw) == 0 || string(raw) == "null" {
		return nil, fmt.Errorf("crictl %s response missing %s list", command, key)
	}
	if err = json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	matched := make([]object, 0)
	for _, row := range rows {
		labels, _ := row["labels"].(map[string]any)
		if labels["mini-k8s.io/managed"] == "true" && labels["mini-k8s.io/pod-uid"] == uid {
			matched = append(matched, row)
		}
	}
	return matched, nil
}

// 记录每个本轮 sandbox 的 host-local IP 租约，后续必须逐个确认释放。
func (g *g3Fixture) observeLease(uid string, sandbox object) error {
	id, ok := sandbox["id"].(string)
	if !ok || id == "" {
		return fmt.Errorf("sandbox ID missing")
	}
	if lease, found := g.leases[id]; found && lease.UID == uid {
		return nil
	}
	data, err := g.cri("inspectp", id)
	if err != nil {
		return fmt.Errorf("inspect sandbox %s: %w", id, err)
	}
	var inspection struct {
		Status struct {
			Network struct {
				IP string `json:"ip"`
			} `json:"network"`
		} `json:"status"`
	}
	if err = json.Unmarshal(data, &inspection); err != nil {
		return err
	}
	ip := inspection.Status.Network.IP
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil || parsedIP.To4() == nil || !g.podCIDR.Contains(parsedIP) {
		return fmt.Errorf("sandbox %s IP %q is outside the declared Pod CIDR", id, ip)
	}
	network := os.Getenv("G3_CNI_NETWORK")
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`).MatchString(network) {
		return fmt.Errorf("G3_CNI_NETWORK is not configured")
	}
	base := os.Getenv("G3_CNI_DATA_DIR")
	if base == "" {
		base = "/var/lib/cni/networks"
	}
	if !filepath.IsAbs(base) {
		return fmt.Errorf("G3_CNI_DATA_DIR must be absolute")
	}
	path := filepath.Join(base, network, ip)
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read lease %s: %w", path, err)
	}
	fields := strings.Fields(string(contents))
	if len(fields) == 0 || fields[0] != id {
		return fmt.Errorf("lease %s does not belong to sandbox %s", path, id)
	}
	g.leases[id] = g3Lease{UID: uid, IP: ip, SandboxID: id, Path: path}
	g.evidence("leases.json", g.leases)
	return nil
}

func (g *g3Fixture) verifyLeasesReleased(uid string) error {
	found := false
	for _, lease := range g.leases {
		if lease.UID != uid {
			continue
		}
		found = true
		data, err := os.ReadFile(lease.Path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read lease %s: %w", lease.Path, err)
		}
		fields := strings.Fields(string(data))
		if len(fields) > 0 && fields[0] == lease.SandboxID {
			return fmt.Errorf("sandbox %s still owns lease %s", lease.SandboxID, lease.Path)
		}
	}
	if !found {
		return fmt.Errorf("no observed IP lease for UID %s; cleanup cannot be verified", uid)
	}
	return nil
}

func (g *g3Fixture) evidence(name string, value any) {
	if g.evidenceDir == "" {
		return
	}
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		g.t.Errorf("encode evidence: %v", err)
		return
	}
	if err = os.WriteFile(filepath.Join(g.evidenceDir, name), b, 0600); err != nil {
		g.t.Errorf("write evidence: %v", err)
	}
}

func (g *g3Fixture) recordIdentities() {
	g.evidence("identities.json", object{"node": g.nodeName, "replicaSet": g.rsName, "replicaSetUID": g.rsUID, "pod": g.podName, "podUID": g.podUID, "extraPods": g.extraPods, "stateDirectory": g.stateRoot, "controlSocketDirectory": g.controlDir})
}

func (g *g3Fixture) createAndBind() {
	g.t.Helper()
	node := g.call("POST", "nodes", admin, object{"apiVersion": "mini.k8s/v1", "kind": "Node", "metadata": object{"name": g.nodeName}, "spec": object{"allocatable": object{"cpuMilli": 2000, "memoryBytes": 1073741824}, "podCIDR": g.podCIDRText}}, 201)
	rs := replicaSetManifest(g.rsName)
	rsspec := rs["spec"].(object)
	template := rsspec["template"].(object)
	podspec := template["spec"].(object)
	podspec["image"] = g.image
	podspec["command"] = []string{"sh", "-c"}
	podspec["args"] = []string{"echo " + g.marker + "; while true; do sleep 1; done"}
	podspec["resources"] = object{"requests": object{"cpuMilli": 50, "memoryBytes": 67108864}, "limits": object{"cpuMilli": 100, "memoryBytes": 134217728}}
	createdRS := g.call("POST", "replicasets", admin, rs, 201)
	g.rsUID = uidOf(createdRS)
	g.recordIdentities()
	createdPod := g.createOwnedPod(g.podName)
	g.podUID = uidOf(createdPod)
	g.recordIdentities()
	// Agent 尚未启动，模拟心跳应紧邻绑定，避免准备工作耗尽节点有效期。
	node = g.call("GET", "nodes/"+g.nodeName, admin, nil, 200)
	g.call("PUT", "nodes/"+g.nodeName+"/status", g3NodeToken, object{"resourceVersion": resourceVersionOf(node), "status": object{"ready": true, "reserved": object{"cpuMilli": 0, "memoryBytes": 0}}}, 200)
	g.call("POST", "pods/"+g.podName+"/bind", scheduler, object{"nodeName": g.nodeName, "nodeUID": uidOf(node), "podUID": g.podUID, "resourceVersion": resourceVersionOf(createdPod)}, 200)
	g.boundAt = time.Now()
}

// createOwnedPod 使用正式创建操作协议，Agent 手工绑定测试不启动调度器。
func (g *g3Fixture) createOwnedPod(name string) object {
	g.t.Helper()
	rs := g.call("GET", "replicasets/"+g.rsName, admin, nil, 200)
	pods := g.call("GET", "pods", admin, nil, 200)
	path := "replicasets/" + g.rsName + "/create-operation"
	operation := g.call("POST", path, g3ControllerToken, object{"uid": g.rsUID, "resourceVersion": resourceVersionOf(rs), "podName": name, "podListRevision": pods["resourceVersion"]}, 200)
	committed := g.call("PUT", path, g3ControllerToken, object{"uid": g.rsUID, "operationID": operation["operationID"], "podName": name}, 200)
	pod := g.call("GET", "pods/"+name, admin, nil, 200)
	if committed["state"] != "COMMITTED" || committed["podUID"] != uidOf(pod) {
		g.t.Fatal("created Pod does not match committed operation")
	}
	return pod
}

// 额外运行时测试使用独立 Pod；Controller 已停止，不能影响 RS 对账。
func (g *g3Fixture) createExtra(name, command string, memory int64) string {
	g.t.Helper()
	pod := podManifest(name)
	spec := pod["spec"].(object)
	spec["image"] = g.image
	spec["command"] = []string{"sh", "-c"}
	spec["args"] = []string{command}
	spec["resources"] = object{"requests": object{"cpuMilli": 50, "memoryBytes": 16777216}, "limits": object{"cpuMilli": 100, "memoryBytes": memory}}
	pod["metadata"].(object)["labels"] = object{"app": g.rsName}
	created := g.call("POST", "pods", admin, pod, 201)
	uid := uidOf(created)
	g.extraPods[name] = uid
	g.recordIdentities()
	node := g.call("GET", "nodes/"+g.nodeName, admin, nil, 200)
	g.call("POST", "pods/"+name+"/bind", scheduler, object{"nodeName": g.nodeName, "nodeUID": uidOf(node), "podUID": uid, "resourceVersion": resourceVersionOf(created)}, 200)
	return uid
}

func (g *g3Fixture) deletePod(name, uid string) error {
	pod, code, err := g.request("GET", "pods/"+name, admin, nil)
	if err != nil || (code != 200 && code != 404) {
		return fmt.Errorf("read %s: HTTP %d: %v", name, code, err)
	}
	if code == 200 {
		if uidOf(pod) != uid {
			return fmt.Errorf("refuse to delete %s: UID changed", name)
		}
		// 独立运行时测试 Pod 使用管理员删除；RS 子 Pod 仍由 Controller 删除。
		identity := g3ControllerToken
		if _, independent := g.extraPods[name]; independent {
			identity = admin
		}
		_, code, err = g.request("DELETE", "pods/"+name+"?uid="+url.QueryEscape(uid), identity, nil)
		if err != nil || (code != 200 && code != 404) {
			return fmt.Errorf("delete %s: HTTP %d: %v", name, code, err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	previous := g.deadline
	g.deadline = deadline
	defer func() { g.deadline = previous }()
	for time.Now().Before(deadline) {
		_, code, err = g.request("GET", "pods/"+name, admin, nil)
		sandboxes, e1 := g.criObjects("pods", "items", uid)
		containers, e2 := g.criObjects("ps", "containers", uid)
		if err == nil && code == 404 && e1 == nil && e2 == nil && len(sandboxes) == 0 && len(containers) == 0 && time.Now().Before(deadline) {
			return g.verifyLeasesReleased(uid)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("UID %s was not fully cleaned in 30 seconds", uid)
}

// 读取 containerd 实际 OCI 运行配置，而非只检查提交到 API 的 limits。
func (g *g3Fixture) runtimeLimits(containerID string) (object, error) {
	b, err := g.cri("inspect", containerID)
	if err != nil {
		return nil, fmt.Errorf("CRI inspect: %w", err)
	}
	var inspection struct {
		Info struct {
			RuntimeSpec struct {
				Linux struct {
					Resources object `json:"resources"`
				} `json:"linux"`
			} `json:"runtimeSpec"`
		} `json:"info"`
	}
	if err = json.Unmarshal(b, &inspection); err != nil {
		return nil, err
	}
	if inspection.Info.RuntimeSpec.Linux.Resources == nil {
		return nil, fmt.Errorf("CRI inspection has no OCI linux.resources")
	}
	return inspection.Info.RuntimeSpec.Linux.Resources, nil
}

// 从 List 版本连续 Watch；任何额外的 RS 子 Pod 均被记录，包括短暂创建后删除的副本。
func (g *g3Fixture) watchUnexpectedOwned() (context.CancelFunc, <-chan error) {
	g.t.Helper()
	list := g.call("GET", "pods", admin, nil, 200)
	count := 0
	for _, raw := range list["items"].([]any) {
		pod := object(raw.(map[string]any))
		if metadataOf(pod)["ownerUID"] == g.rsUID {
			if uidOf(pod) != g.podUID {
				g.t.Fatal("unexpected RS Pod before watch")
			}
			count++
		}
	}
	if count != 1 {
		g.t.Fatalf("expected one RS Pod before watch, got %d", count)
	}
	version := int64(list["resourceVersion"].(float64))
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/mini/v1/pods?watch=true&resourceVersion=%d", g.api.URL, version), nil)
	if err != nil {
		cancel()
		g.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+admin)
	client := &http.Client{Transport: g.client.Transport}
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		g.t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		_ = resp.Body.Close()
		cancel()
		g.t.Fatalf("Pod watch returned HTTP %d", resp.StatusCode)
	}
	violations := make(chan error, 1)
	ownerUID, podUID := g.rsUID, g.podUID
	go func() {
		defer resp.Body.Close()
		decoder := json.NewDecoder(resp.Body)
		for {
			var event struct {
				Type   string `json:"type"`
				Object object `json:"object"`
			}
			if err := decoder.Decode(&event); err != nil {
				if ctx.Err() == nil {
					select {
					case violations <- fmt.Errorf("Pod watch ended: %w", err):
					default:
					}
				}
				return
			}
			if event.Type == "ERROR" {
				select {
				case violations <- fmt.Errorf("Pod watch reported an error"):
				default:
				}
				return
			}
			metadata, _ := event.Object["metadata"].(map[string]any)
			if metadata["ownerUID"] == ownerUID && metadata["uid"] != podUID {
				select {
				case violations <- fmt.Errorf("unexpected RS Pod UID %v", metadata["uid"]):
				default:
				}
				return
			}
		}
	}()
	return cancel, violations
}

func (g *g3Fixture) checkWatch(violations <-chan error) {
	g.t.Helper()
	select {
	case err := <-violations:
		g.t.Fatal(err)
	default:
	}
}

func (g *g3Fixture) waitRunning(uid string, timeout time.Duration) (object, object, object) {
	return g.waitNamedRunning(g.podName, uid, timeout)
}

func (g *g3Fixture) waitNamedRunning(name, uid string, timeout time.Duration) (object, object, object) {
	return g.waitNamedRunningUntil(name, uid, time.Now().Add(timeout), 0)
}

func (g *g3Fixture) waitNamedRunningUntil(name, uid string, deadline time.Time, afterVersion float64) (object, object, object) {
	g.t.Helper()
	previous := g.deadline
	g.deadline = deadline
	defer func() { g.deadline = previous }()
	for time.Now().Before(deadline) {
		g.requireAlive(g.agentProcess, g.agentExit, "Agent")
		pod, code, err := g.request("GET", "pods/"+name, admin, nil)
		sandboxes, e1 := g.criObjects("pods", "items", uid)
		containers, e2 := g.criObjects("ps", "containers", uid)
		if err == nil && code == 200 && uidOf(pod) == uid && e1 == nil && e2 == nil {
			status, _ := pod["status"].(map[string]any)
			ip, _ := status["podIP"].(string)
			var readySandboxes, running []object
			activeContainers := 0
			for _, s := range sandboxes {
				if s["state"] == "SANDBOX_READY" {
					readySandboxes = append(readySandboxes, s)
				}
			}
			for _, c := range containers {
				if c["state"] == "CONTAINER_RUNNING" || c["state"] == "CONTAINER_CREATED" {
					activeContainers++
				}
				if c["state"] == "CONTAINER_RUNNING" {
					running = append(running, c)
				}
			}
			parsedIP := net.ParseIP(ip)
			if status["phase"] == "Running" && parsedIP != nil && parsedIP.To4() != nil && g.podCIDR.Contains(parsedIP) && resourceVersionOf(pod) > afterVersion && len(readySandboxes) == 1 && len(running) == 1 && activeContainers == 1 && g.observeLease(uid, readySandboxes[0]) == nil && g.leases[readySandboxes[0]["id"].(string)].IP == ip && time.Now().Before(deadline) {
				g.evidence("pod-"+uid+".json", pod)
				g.evidence("sandboxes-"+uid+".json", sandboxes)
				g.evidence("containers-"+uid+".json", containers)
				return pod, readySandboxes[0], running[0]
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	g.t.Fatalf("Pod %s did not reach Running with exactly one sandbox and container before %s", uid, deadline)
	return nil, nil, nil
}

func (g *g3Fixture) cleanup() {
	clean := true
	for name, uid := range g.extraPods {
		if err := g.deletePod(name, uid); err != nil {
			g.t.Error(err)
			clean = false
		}
	}
	if g.podUID != "" {
		if err := g.deletePod(g.podName, g.podUID); err != nil {
			g.t.Error(err)
			clean = false
		}
	}
	if !clean {
		g.t.Error("preserving node and ReplicaSet because runtime cleanup is incomplete")
		return
	}
	// 不直接更改 etcd 或删除其他 UID 的资源。
	if g.rsUID != "" {
		_, code, err := g.request("DELETE", "replicasets/"+g.rsName, admin, nil)
		if err != nil || (code != 200 && code != 404) {
			g.t.Errorf("delete ReplicaSet: HTTP %d %v", code, err)
		}
		if code == 200 {
			rs, readCode, readErr := g.request("GET", "replicasets/"+g.rsName, admin, nil)
			if readErr == nil && readCode == 200 {
				_, code, err = g.request("POST", "replicasets/"+g.rsName+"/finalize", g3ControllerToken, object{"uid": g.rsUID, "resourceVersion": resourceVersionOf(rs)})
				if err != nil || code != 200 {
					g.t.Errorf("finalize ReplicaSet: HTTP %d %v", code, err)
				}
			} else if readErr != nil || readCode != 404 {
				g.t.Errorf("read ReplicaSet: HTTP %d %v", readCode, readErr)
			}
		}
		_, code, err = g.request("GET", "replicasets/"+g.rsName, admin, nil)
		if err != nil || code != 404 {
			g.t.Errorf("ReplicaSet remains: HTTP %d %v", code, err)
		}
	}
	node, nodeCode, nodeErr := g.request("GET", "nodes/"+g.nodeName, admin, nil)
	if nodeErr == nil && nodeCode == 200 {
		status, _ := node["status"].(map[string]any)
		reserved, _ := status["reserved"].(map[string]any)
		if reserved["cpuMilli"] != float64(0) || reserved["memoryBytes"] != float64(0) {
			g.t.Errorf("node reservation not released: %v", reserved)
			return
		}
	} else if nodeErr != nil || nodeCode != 404 {
		g.t.Errorf("read Node: HTTP %d %v", nodeCode, nodeErr)
		return
	}
	_, code, err := g.request("DELETE", "nodes/"+g.nodeName, admin, nil)
	if err != nil || (code != 200 && code != 404) {
		g.t.Errorf("delete Node: HTTP %d %v", code, err)
	}
}

func TestG3(t *testing.T) {
	g := setupG3(t)
	run := func(name string, fn func(*testing.T)) bool { return t.Run(name, func(t *testing.T) { g.t = t; fn(t) }) }
	// G3-01：真实 CRI 的 sandbox、容器、IP 和日志必须同时可见。
	if !run("G3-01_real_runtime", func(t *testing.T) {
		g.createAndBind()
		g.startManager()
		g.startAgent()
		pod, sandbox, container := g.waitNamedRunningUntil(g.podName, g.podUID, g.boundAt.Add(30*time.Second), 0)
		g.requireAlive(g.managerProcess, g.managerExit, "manager")
		if uidOf(pod) != g.podUID {
			t.Fatal("Pod UID changed")
		}
		logs, err := g.cri("logs", container["id"].(string))
		if err != nil || !bytes.Contains(logs, []byte(g.marker)) {
			t.Fatalf("CRI log marker missing: %v", err)
		}
		logs, err = exec.Command(g.agentBinary, "-state-dir", g.stateDir, "-control-socket", g.controlSocket, "-logs-uid", g.podUID, "-tail", "100").CombinedOutput()
		if err != nil || !bytes.Contains(logs, []byte(g.marker)) {
			t.Fatalf("Agent log marker missing: %v", err)
		}
		g.evidence("runtime-running.json", object{"pod": pod, "sandbox": sandbox, "container": container, "logMarker": g.marker})
	}) {
		return
	}
	// G3-02：重复对账及 Agent 重启不能替换仍在运行的容器。
	if !run("G3-02_adoption", func(t *testing.T) {
		_, _, container := g.waitRunning(g.podUID, 30*time.Second)
		containerID := container["id"].(string)
		time.Sleep(5 * time.Second)
		_, _, container = g.waitRunning(g.podUID, 5*time.Second)
		if container["id"] != containerID {
			t.Fatal("reconciliation replaced running container")
		}
		g.stopAgent()
		// 让重启前的旧 Running 状态失效；只有新 Agent 成功对账才能写回 Running。
		current := g.call("GET", "pods/"+g.podName, admin, nil, 200)
		status := current["status"].(map[string]any)
		status["phase"] = "Pending"
		updated := g.call("PUT", "pods/"+g.podName+"/status", g3NodeToken, object{"resourceVersion": resourceVersionOf(current), "status": status}, 200)
		deadline := time.Now().Add(30 * time.Second)
		g.startAgent()
		_, _, container = g.waitNamedRunningUntil(g.podName, g.podUID, deadline, resourceVersionOf(updated))
		if container["id"] != containerID {
			t.Fatal("Agent restart failed to adopt container")
		}
		g.requireAlive(g.agentProcess, g.agentExit, "Agent")
	}) {
		return
	}
	// G3-03：同一实例稳定 60 秒后只终止该 UID 的容器，再核对重启及原因。
	if !run("G3-03_restart", func(t *testing.T) {
		cancelWatch, violations := g.watchUnexpectedOwned()
		defer cancelWatch()
		pod, _, container := g.waitRunning(g.podUID, 30*time.Second)
		containerID := container["id"].(string)
		beforeStatus, _ := pod["status"].(map[string]any)
		beforeCount, _ := beforeStatus["restartCount"].(float64)
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			g.requireAlive(g.managerProcess, g.managerExit, "manager")
			g.checkWatch(violations)
			_, _, current := g.waitRunning(g.podUID, 5*time.Second)
			if current["id"] != containerID {
				t.Fatal("container changed before 60-second stability window")
			}
			time.Sleep(time.Second)
		}
		deadline = time.Now().Add(30 * time.Second)
		g.deadline = deadline
		defer func() { g.deadline = time.Time{} }()
		stopOutput, stopErr := g.cri("stop", containerID)
		if stopErr != nil {
			if !errors.Is(stopErr, context.DeadlineExceeded) {
				t.Fatalf("stop selected test container: %v: %s", stopErr, stopOutput)
			}
			// 超时只表示 RPC 的结果未知。停止请求可能已生效；继续观察，
			// 但仍须在原 30 秒期限内证明旧容器被替换及重启计数增长。
			t.Logf("stop RPC timed out; awaiting independently verified restart: %v", stopErr)
		}
		recovered := false
		for time.Now().Before(deadline) {
			g.requireAlive(g.managerProcess, g.managerExit, "manager")
			g.requireAlive(g.agentProcess, g.agentExit, "Agent")
			g.checkWatch(violations)
			current, code, err := g.request("GET", "pods/"+g.podName, admin, nil)
			if err == nil && code == 200 {
				status, _ := current["status"].(map[string]any)
				count, _ := status["restartCount"].(float64)
				reason, _ := status["reason"].(string)
				containers, e := g.criObjects("ps", "containers", g.podUID)
				running := 0
				newID := ""
				for _, c := range containers {
					if c["state"] == "CONTAINER_RUNNING" {
						running++
						newID, _ = c["id"].(string)
					}
				}
				if e == nil && uidOf(current) == g.podUID && status["phase"] == "Running" && count > beforeCount && reason != "" && running == 1 && newID != containerID && time.Now().Before(deadline) {
					g.evidence("restart.json", object{"pod": current, "containers": containers, "stopTimedOut": errors.Is(stopErr, context.DeadlineExceeded)})
					recovered = true
					break
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !recovered {
			t.Fatalf("same UID did not recover with increased restartCount and exit reason; stop error: %v; stop output: %s", stopErr, stopOutput)
		}
		g.checkWatch(violations)
		list := g.call("GET", "pods", admin, nil, 200)
		owned := 0
		for _, raw := range list["items"].([]any) {
			p := object(raw.(map[string]any))
			if metadataOf(p)["ownerUID"] == g.rsUID {
				owned++
			}
		}
		if owned != 1 {
			t.Fatalf("expected one RS Pod after restart, got %d", owned)
		}
	}) {
		return
	}
	// 后续限制与同名重建场景由测试显式操作，先停 manager 避免主动补副本。
	g.stopManager()
	// G3-04：CPU 忙循环检查真实 OCI limits；独立超内存容器保留 CRI OOM 状态证据。
	if !run("G3-04_limits", func(t *testing.T) {
		cpuName := g.podName + "-cpu"
		cpuUID := g.createExtra(cpuName, "while :; do :; done", 67108864)
		_, _, container := g.waitNamedRunning(cpuName, cpuUID, 30*time.Second)
		limits, err := g.runtimeLimits(container["id"].(string))
		if err != nil {
			t.Fatal(err)
		}
		cpu, _ := limits["cpu"].(map[string]any)
		memory, _ := limits["memory"].(map[string]any)
		quota, quotaOK := cpu["quota"].(float64)
		period, periodOK := cpu["period"].(float64)
		if !quotaOK || !periodOK || quota <= 0 || period <= 0 || quota*10 != period || memory["limit"] != float64(67108864) {
			t.Fatalf("unexpected runtime limits: %v", limits)
		}
		g.evidence("cpu-runtime-limits.json", limits)
		if err := g.deletePod(cpuName, cpuUID); err != nil {
			t.Fatal(err)
		}

		oomName := g.podName + "-oom"
		oomUID := g.createExtra(oomName, "x=0123456789abcdef; while :; do x=$x$x; done", 33554432)
		deadline := time.Now().Add(30 * time.Second)
		previousDeadline := g.deadline
		g.deadline = deadline
		defer func() { g.deadline = previousDeadline }()
		observed := false
		leaseObserved := false
		for time.Now().Before(deadline) {
			sandboxes, sandboxErr := g.criObjects("pods", "items", oomUID)
			if sandboxErr == nil {
				for _, sandbox := range sandboxes {
					if g.observeLease(oomUID, sandbox) == nil {
						leaseObserved = true
					}
				}
			}
			containers, err := g.criObjects("ps", "containers", oomUID)
			if err == nil {
				for _, c := range containers {
					if c["state"] != "CONTAINER_EXITED" {
						continue
					}
					id, _ := c["id"].(string)
					data, inspectErr := g.cri("inspect", id)
					var inspected struct {
						Status object `json:"status"`
					}
					if inspectErr == nil && json.Unmarshal(data, &inspected) == nil && inspected.Status["reason"] == "OOMKilled" && inspected.Status["exitCode"] == float64(137) {
						g.evidence("oom-status.json", inspected.Status)
						observed = true
						break
					}
				}
			}
			if observed && leaseObserved && time.Now().Before(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !observed || !leaseObserved || !time.Now().Before(deadline) {
			t.Fatal("no CRI OOMKilled exit and sandbox IP lease observed before deadline")
		}
		g.deadline = previousDeadline
		if err := g.deletePod(oomName, oomUID); err != nil {
			t.Fatal(err)
		}
	}) {
		return
	}

	// G3-05：同名新实例存在时，由持锁 Agent 执行旧 UID 的真实迟到清理。
	run("G3-05_cleanup_uid", func(t *testing.T) {
		network := os.Getenv("G3_CNI_NETWORK")
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`).MatchString(network) {
			g.keepState = true
			t.Skip("NOT_RUN: G3_CNI_NETWORK must identify the local host-local network")
		}
		dataDir := os.Getenv("G3_CNI_DATA_DIR")
		if dataDir == "" {
			dataDir = "/var/lib/cni/networks"
		}
		if !filepath.IsAbs(dataDir) {
			t.Fatal("G3_CNI_DATA_DIR must be an absolute local path")
		}
		oldUID := g.podUID
		pod, sandbox, _ := g.waitRunning(oldUID, 30*time.Second)
		status := pod["status"].(map[string]any)
		ip := status["podIP"].(string)
		sandboxID := sandbox["id"].(string)
		leasePath := filepath.Join(dataDir, network, ip)
		lease, err := os.ReadFile(leasePath)
		if err != nil || !bytes.Contains(lease, []byte(sandboxID)) {
			g.keepState = true
			t.Skip("NOT_RUN: cannot verify original IP lease belongs to the test sandbox")
		}
		if err := g.deletePod(g.podName, oldUID); err != nil {
			t.Fatal(err)
		}
		lease, err = os.ReadFile(leasePath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err == nil && bytes.Contains(lease, []byte(sandboxID)) {
			t.Fatal("old sandbox still owns the host-local IP lease")
		}
		node := g.call("GET", "nodes/"+g.nodeName, admin, nil, 200)
		nodeStatus := node["status"].(map[string]any)
		reserved := nodeStatus["reserved"].(map[string]any)
		if reserved["cpuMilli"] != float64(0) || reserved["memoryBytes"] != float64(0) {
			t.Fatalf("old reservation remains: %v", reserved)
		}
		created := g.createOwnedPod(g.podName)
		g.podUID = uidOf(created)
		g.recordIdentities()
		if g.podUID == oldUID {
			t.Fatal("same-name recreation retained old UID")
		}
		g.call("POST", "pods/"+g.podName+"/bind", scheduler, object{"nodeName": g.nodeName, "nodeUID": uidOf(node), "podUID": g.podUID, "resourceVersion": resourceVersionOf(created)}, 200)
		_, _, newContainer := g.waitRunning(g.podUID, 30*time.Second)
		output, err := exec.Command(g.agentBinary, "-state-dir", g.stateDir, "-control-socket", g.controlSocket, "-cleanup-name", g.podName, "-cleanup-uid", oldUID).CombinedOutput()
		if err != nil {
			t.Fatalf("Agent rejected old UID cleanup: %v: %s", err, output)
		}
		time.Sleep(3 * time.Second)
		_, _, after := g.waitRunning(g.podUID, 5*time.Second)
		if after["id"] != newContainer["id"] {
			t.Fatal("old UID cleanup changed new container")
		}
		oldSandboxes, e1 := g.criObjects("pods", "items", oldUID)
		oldContainers, e2 := g.criObjects("ps", "containers", oldUID)
		if e1 != nil || e2 != nil || len(oldSandboxes) != 0 || len(oldContainers) != 0 {
			t.Fatal("old UID runtime objects remain")
		}
		g.evidence("uid-recreation.json", object{"oldUID": oldUID, "newUID": g.podUID, "oldIP": ip, "oldSandboxID": sandboxID, "newContainerID": newContainer["id"], "oldLeaseReleased": true, "reservedBeforeRecreate": reserved})
	})
}
