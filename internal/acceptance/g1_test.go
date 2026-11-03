// Package acceptance exercises the public API against a real etcd process.
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

type fixture struct {
	t                                                               *testing.T
	url, cert, key, auth, token, node, other, pod, rs, binary, etcd string
	client                                                          *http.Client
	process                                                         *exec.Cmd
	log                                                             *os.File
}

func (f *fixture) start() {
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

func (f *fixture) stop() {
	f.t.Helper()
	if f.process != nil && f.process.Process != nil {
		_ = f.process.Process.Kill()
		_ = f.process.Wait()
		f.process = nil
	}
}

func (f *fixture) request(method, path, token string, payload any) ([]byte, int, error) {
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

func (f *fixture) call(method, path, token string, payload any, code int) object {
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

func meta(o object) object { return object(o["metadata"].(map[string]any)) }
func spec(o object) object { return object(o["spec"].(map[string]any)) }
func rv(o object) float64  { return meta(o)["resourceVersion"].(float64) }
func id(o object) string   { return meta(o)["uid"].(string) }

func pod(name string) object {
	return object{"apiVersion": "mini.k8s/v1", "kind": "Pod", "metadata": object{"name": name}, "spec": object{"image": "example.invalid/demo:fixed", "restartPolicy": "Always", "env": object{"SENSITIVE": sensitive}, "resources": object{"requests": object{"cpuMilli": 100, "memoryBytes": 1024}, "limits": object{"cpuMilli": 200, "memoryBytes": 2048}}, "readiness": object{"port": 8080}}}
}
func replica(name string) object {
	p := pod(name)
	return object{"apiVersion": "mini.k8s/v1", "kind": "ReplicaSet", "metadata": object{"name": name}, "spec": object{"replicas": 1, "selector": object{"app": name}, "template": object{"labels": object{"app": name}, "spec": p["spec"]}}}
}

func (f *fixture) runCLI(args ...string) {
	f.t.Helper()
	var out bytes.Buffer
	args = append([]string{"--server", f.url, "--ca-file", f.cert, "--token-file", f.token}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Run(ctx, args, &out); err != nil {
		f.t.Fatalf("CLI %v failed: %v (%s)", args[6:], err, out.String())
	}
}

func (f *fixture) manifest(v object) string {
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

func (f *fixture) cleanup() {
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
				_, code, err = f.request("POST", "pods/"+name+"/cleanup", agent, object{"uid": id(p), "resourceVersion": rv(p)})
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

func setup(t *testing.T) *fixture {
	t.Helper()
	endpoint, binary := os.Getenv("G1_ETCD_ENDPOINT"), os.Getenv("G1_API_BINARY")
	if endpoint == "" || binary == "" {
		t.Skip("set G1_ETCD_ENDPOINT and G1_API_BINARY to run real-etcd acceptance")
	}
	dir := t.TempDir()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	f := &fixture{t: t, etcd: endpoint, binary: binary, node: "g1-node-" + suffix, other: "g1-other-" + suffix, pod: "g1-pod-" + suffix, rs: "g1-rs-" + suffix}
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
	f := setup(t)
	if !t.Run("G1-01_CRUD", func(t *testing.T) {
		f.t = t
		pm, rm := f.manifest(pod(f.pod)), f.manifest(replica(f.rs))
		f.runCLI("apply", "-f", pm)
		f.runCLI("apply", "-f", rm)
		p := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		oldUID := id(p)
		f.runCLI("apply", "-f", pm)
		if id(f.call("GET", "pods/"+f.pod, admin, nil, 200)) != oldUID {
			t.Fatal("repeat apply changed UID")
		}
		f.runCLI("get", "pod", f.pod)
		f.runCLI("scale", "rs", f.rs, "--replicas", "3")
		rs := f.call("GET", "replicasets/"+f.rs, admin, nil, 200)
		if spec(rs)["replicas"] != float64(3) {
			t.Fatal("scale not persisted")
		}
		f.runCLI("delete", "-f", rm)
		f.call("GET", "replicasets/"+f.rs, admin, nil, 404)
		f.runCLI("apply", "-f", rm)
		f.runCLI("delete", "-f", pm)
		f.runCLI("delete", "-f", pm)
		f.runCLI("apply", "-f", pm)
		if id(f.call("GET", "pods/"+f.pod, admin, nil, 200)) == oldUID {
			t.Fatal("recreated Pod retained UID")
		}
	}) {
		return
	}
	if !t.Run("G1-02_persistence", func(t *testing.T) {
		f.t = t
		before := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		f.stop()
		f.start()
		after := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		if id(before) != id(after) || fmt.Sprint(before["spec"]) != fmt.Sprint(after["spec"]) || fmt.Sprint(before["status"]) != fmt.Sprint(after["status"]) {
			t.Fatal("restart changed persisted state")
		}
	}) {
		return
	}
	if !t.Run("G1-03_CAS_fields", func(t *testing.T) {
		f.t = t
		rs := f.call("GET", "replicasets/"+f.rs, admin, nil, 200)
		data, _ := json.Marshal(rs)
		var a, b object
		_ = json.Unmarshal(data, &a)
		_ = json.Unmarshal(data, &b)
		spec(a)["replicas"] = float64(2)
		spec(b)["replicas"] = float64(4)
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
		spec(p)["unknown"] = true
		f.call("PUT", "pods/"+f.pod, admin, p, 400)
		_ = json.Unmarshal(before, &p)
		spec(p)["image"] = "modified"
		f.call("PUT", "pods/"+f.pod, admin, p, 400)
		_ = json.Unmarshal(before, &p)
		f.call("PUT", "pods/"+f.pod+"/status", admin, object{"resourceVersion": rv(p), "status": object{"phase": "Running"}}, 403)
		after, _ := json.Marshal(f.call("GET", "pods/"+f.pod, admin, nil, 200))
		if !bytes.Equal(before, after) {
			t.Fatal("rejected writes changed resource")
		}
	}) {
		return
	}
	if !t.Run("G1-04_list_watch", func(t *testing.T) {
		f.t = t
		list := f.call("GET", "pods", admin, nil, 200)
		version := int64(list["resourceVersion"].(float64))
		for i := 0; i < 2; i++ {
			p := f.call("GET", "pods/"+f.pod, admin, nil, 200)
			meta(p)["annotations"] = object{"watch": fmt.Sprint(i)}
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
			if !ok || meta(object(observed))["name"] != f.pod || meta(object(observed))["annotations"].(map[string]any)["watch"] != fmt.Sprint(i) {
				t.Fatalf("wrong watch event: %v", event)
			}
			version = int64(event["resourceVersion"].(float64))
		}
	}) {
		return
	}
	if !t.Run("G1-05_binding", func(t *testing.T) {
		f.t = t
		for _, name := range []string{f.node, f.other} {
			f.call("POST", "nodes", admin, object{"apiVersion": "mini.k8s/v1", "kind": "Node", "metadata": object{"name": name}, "spec": object{"allocatable": object{"cpuMilli": 1000, "memoryBytes": 100000}, "podCIDR": "10.8.0.0/24"}}, 201)
		}
		n := f.call("GET", "nodes/"+f.node, admin, nil, 200)
		f.call("PUT", "nodes/"+f.node+"/status", agent, object{"resourceVersion": rv(n), "status": object{"ready": true, "reserved": object{"cpuMilli": 0, "memoryBytes": 0}}}, 200)
		p := f.call("GET", "pods/"+f.pod, admin, nil, 200)
		req := object{"nodeName": f.node, "nodeUID": id(n), "podUID": id(p), "resourceVersion": rv(p)}
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
		f.call("PUT", "pods/"+f.pod+"/status", agent, object{"resourceVersion": rv(p), "status": object{"phase": "Running"}}, 200)
		p = f.call("GET", "pods/"+f.pod, admin, nil, 200)
		if fmt.Sprint(before) != fmt.Sprint(p["spec"]) {
			t.Fatal("status update modified spec")
		}
		f.call("DELETE", "pods/"+f.pod, admin, nil, 200)
		p = f.call("GET", "pods/"+f.pod, admin, nil, 200)
		req["resourceVersion"] = rv(p)
		f.call("POST", "pods/"+f.pod+"/bind", scheduler, req, 409)
	}) {
		return
	}
	t.Run("G1-06_auth_log", func(t *testing.T) {
		f.t = t
		f.call("GET", "pods", "", nil, 401)
		n := f.call("GET", "nodes/"+f.node, admin, nil, 200)
		f.call("PUT", "nodes/"+f.node+"/status", otherAgent, object{"resourceVersion": rv(n), "status": object{"ready": true}}, 403)
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
