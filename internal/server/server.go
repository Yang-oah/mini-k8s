// Package server 提供访问 etcd 资源存储的 HTTP 接口。
package server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const prefix = "/mini-k8s/v1/"

// Credentials 保存各控制面身份的令牌；Agent 令牌按节点名称区分。
type Credentials struct {
	Admin      string            `json:"admin"`
	Controller string            `json:"controller"`
	Scheduler  string            `json:"scheduler"`
	Agents     map[string]string `json:"agents"`
}

// Server 为已配置凭据的请求提供资源 API。
type Server struct {
	Client      *clientv3.Client
	Credentials Credentials
	Log         *slog.Logger
}

type identity struct{ role, node string }

type auditKey struct{}
type auditContext struct{ uid string }

func setAuditUID(ctx context.Context, uid string) {
	if audit, ok := ctx.Value(auditKey{}).(*auditContext); ok {
		audit.uid = uid
	}
}

func (s *Server) authenticate(header string) identity {
	if !strings.HasPrefix(header, "Bearer ") {
		return identity{}
	}
	token := strings.TrimPrefix(header, "Bearer ")
	// 不接受空令牌，并以固定时间比较凭据；日志不记录请求头或令牌。
	match := func(expected string) bool {
		return expected != "" && subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
	}
	if match(s.Credentials.Admin) {
		return identity{role: "admin"}
	}
	if match(s.Credentials.Controller) {
		return identity{role: "controller"}
	}
	if match(s.Credentials.Scheduler) {
		return identity{role: "scheduler"}
	}
	for node, expected := range s.Credentials.Agents {
		if match(expected) {
			return identity{role: "agent", node: node}
		}
	}
	return identity{}
}

func newUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type responseError struct {
	Code      int    `json:"code"`
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
}

func writeAPIError(w http.ResponseWriter, status int, reason, msg, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseError{status, reason, msg, requestID})
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type object struct {
	value any
	meta  *api.Metadata
}

func newObject(kind string) (object, bool) {
	switch kind {
	case "pods":
		p := &api.Pod{}
		return object{p, &p.Metadata}, true
	case "replicasets":
		r := &api.ReplicaSet{}
		return object{r, &r.Metadata}, true
	case "nodes":
		n := &api.Node{}
		return object{n, &n.Metadata}, true
	case "services":
		v := &api.Service{}
		return object{v, &v.Metadata}, true
	}
	return object{}, false
}
func decode(data []byte, kind string, revision int64) (object, error) {
	o, ok := newObject(kind)
	if !ok {
		return object{}, errors.New("unknown kind")
	}
	if err := api.DecodeStrict(strings.NewReader(string(data)), o.value); err != nil {
		return object{}, err
	}
	o.meta.ResourceVersion = revision
	return o, nil
}
func validateObject(o object) error {
	switch v := o.value.(type) {
	case *api.Pod:
		return api.ValidatePod(*v)
	case *api.ReplicaSet:
		return api.ValidateReplicaSet(*v)
	case *api.Node:
		if v.Kind != "Node" || v.APIVersion != api.Version || v.Metadata.Name == "" || strings.ContainsAny(v.Metadata.Name, "/ ") || v.Spec.Allocatable.CPUMilli <= 0 || v.Spec.Allocatable.MemoryBytes <= 0 || v.Spec.PodCIDR == "" {
			return errors.New("invalid node")
		}
	case *api.Service:
		if v.Kind != "Service" || v.APIVersion != api.Version || v.Metadata.Name == "" || strings.ContainsAny(v.Metadata.Name, "/ ") || len(v.Spec.Selector) == 0 || v.Spec.Port < 1 || v.Spec.Port > 65535 || v.Spec.TargetPort < 1 || v.Spec.TargetPort > 65535 {
			return errors.New("invalid service")
		}
	}
	return nil
}
func resourceKey(kind, name string) string { return prefix + kind + "/" + name }

// ServeHTTP 认证请求并按身份及资源类型分派操作。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	id := newUID()
	audit := &auditContext{}
	r = r.WithContext(context.WithValue(r.Context(), auditKey{}, audit))
	who := s.authenticate(r.Header.Get("Authorization"))
	status := http.StatusOK
	logger := s.Log
	if logger == nil {
		logger = slog.Default()
	}
	defer func() {
		logger.Info("api request", "requestId", id, "identity", who.role, "node", who.node, "method", r.Method, "path", r.URL.Path, "uid", audit.uid, "result", status, "durationMs", time.Since(start).Milliseconds())
	}()
	respondError := func(code int, reason, msg string) { status = code; writeAPIError(w, code, reason, msg, id) }
	if who.role == "" {
		respondError(401, "Unauthorized", "authentication required")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/mini/v1/")
	if path == r.URL.Path {
		respondError(404, "NotFound", "unknown path")
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 1 || len(parts) > 3 {
		respondError(404, "NotFound", "unknown path")
		return
	}
	kind := parts[0]
	_, ok := newObject(kind)
	if !ok {
		respondError(404, "NotFound", "unknown resource")
		return
	}
	name := ""
	if len(parts) > 1 {
		name = parts[1]
		if name == "" {
			respondError(404, "NotFound", "empty name")
			return
		}
	}
	action := ""
	if len(parts) > 2 {
		action = parts[2]
	}
	// 状态、绑定和清理走独立入口，用户资源写入不能绕过对应身份检查。
	if who.role == "agent" && ((r.Method != "GET" && action != "status" && action != "cleanup") || (kind != "pods" && kind != "nodes")) {
		respondError(403, "Forbidden", "agent not authorized")
		return
	}
	if action == "bind" {
		if who.role != "scheduler" || r.Method != "POST" || kind != "pods" {
			respondError(403, "Forbidden", "binding requires scheduler identity")
			return
		}
		s.bind(w, r, name, respondError)
		return
	}
	if action == "status" {
		if who.role != "agent" || r.Method != "PUT" {
			respondError(403, "Forbidden", "status requires agent identity")
			return
		}
		s.status(w, r, kind, name, who, respondError)
		return
	}
	if action == "cleanup" {
		if who.role != "agent" || r.Method != "POST" || kind != "pods" {
			respondError(403, "Forbidden", "cleanup requires the assigned agent")
			return
		}
		s.cleanup(w, r, name, who, respondError)
		return
	}
	if action != "" {
		respondError(404, "NotFound", "unknown action")
		return
	}
	if r.Method != "GET" && who.role != "admin" && !(who.role == "controller" && kind == "pods") {
		respondError(403, "Forbidden", "write not authorized")
		return
	}
	if r.Method == "GET" && r.URL.Query().Get("watch") == "true" {
		if name != "" || who.role == "agent" {
			respondError(403, "Forbidden", "watch not authorized")
			return
		}
		s.watch(w, r, kind, respondError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if r.Method == "GET" {
		k := resourceKey(kind, name)
		options := []clientv3.OpOption{}
		if name == "" {
			options = append(options, clientv3.WithPrefix())
		}
		resp, err := s.Client.Get(ctx, k, options...)
		if err != nil {
			respondError(503, "Unavailable", "storage unavailable")
			return
		}
		if name != "" && len(resp.Kvs) == 0 {
			respondError(404, "NotFound", "resource not found")
			return
		}
		items := make([]any, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			o, err := decode(kv.Value, kind, kv.ModRevision)
			if err != nil {
				respondError(503, "Unavailable", "stored resource invalid")
				return
			}
			if who.role == "agent" && !s.agentCanRead(ctx, who, kind, o) {
				continue
			}
			items = append(items, o.value)
		}
		if name != "" {
			if len(items) == 0 {
				respondError(403, "Forbidden", "resource not authorized")
				return
			}
			if len(resp.Kvs) == 1 {
				setAuditUID(ctx, func() string { o, _ := decode(resp.Kvs[0].Value, kind, resp.Kvs[0].ModRevision); return o.meta.UID }())
			}
			writeJSON(w, items[0])
			return
		}
		writeJSON(w, map[string]any{"items": items, "resourceVersion": resp.Header.Revision})
		return
	}
	if r.Method == "POST" && name == "" {
		status = http.StatusCreated
		s.create(ctx, w, r, kind, respondError)
		return
	}
	if r.Method == "PUT" && name != "" {
		s.update(ctx, w, r, kind, name, respondError)
		return
	}
	if r.Method == "DELETE" && name != "" {
		s.delete(ctx, w, kind, name, respondError)
		return
	}
	respondError(405, "MethodNotAllowed", "unsupported method")
}

type errorResponder func(int, string, string)

func decodeBody(r *http.Request, o object) error {
	return api.DecodeStrict(io.LimitReader(r.Body, 1<<20), o.value)
}
func encodeObject(o object) string { b, _ := json.Marshal(o.value); return string(b) }

func selectorsOverlap(a, b map[string]string) bool {
	for k, v := range a {
		if other, ok := b[k]; ok && other != v {
			return false
		}
	}
	return true
}
func (s *Server) agentCanRead(ctx context.Context, who identity, kind string, o object) bool {
	if kind == "nodes" {
		return o.meta.Name == who.node
	}
	if kind != "pods" {
		return false
	}
	n, err := s.Client.Get(ctx, resourceKey("nodes", who.node))
	if err != nil || len(n.Kvs) != 1 {
		return false
	}
	obj, err := decode(n.Kvs[0].Value, "nodes", n.Kvs[0].ModRevision)
	if err != nil {
		return false
	}
	return o.value.(*api.Pod).NodeUID != "" && o.value.(*api.Pod).NodeUID == obj.meta.UID
}

// ValidateCredentials 检查必需凭据，并禁止不同身份共用令牌。
func ValidateCredentials(c Credentials) error {
	seen := map[string]bool{}
	check := func(v string) error {
		if v == "" {
			return nil
		}
		if seen[v] {
			return fmt.Errorf("credential must not be shared between identities")
		}
		seen[v] = true
		return nil
	}
	if c.Admin == "" || c.Scheduler == "" {
		return errors.New("admin and scheduler credentials required")
	}
	for _, v := range []string{c.Admin, c.Scheduler, c.Controller} {
		if err := check(v); err != nil {
			return err
		}
	}
	for n, v := range c.Agents {
		if n == "" || v == "" {
			return errors.New("agent identity and credential required")
		}
		if err := check(v); err != nil {
			return err
		}
	}
	return nil
}
