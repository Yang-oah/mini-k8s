// Package server provides the HTTP boundary to the etcd-backed resource store.
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
	"reflect"
	"strconv"
	"strings"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const prefix = "/mini-k8s/v1/"

type Credentials struct {
	Admin      string            `json:"admin"`
	Controller string            `json:"controller"`
	Scheduler  string            `json:"scheduler"`
	Agents     map[string]string `json:"agents"`
}

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

func uid() string {
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

func fail(w http.ResponseWriter, status int, reason, msg, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseError{status, reason, msg, requestID})
}
func output(w http.ResponseWriter, v any) {
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
func valid(o object) error {
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
func key(kind, name string) string { return prefix + kind + "/" + name }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	id := uid()
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
	bad := func(code int, reason, msg string) { status = code; fail(w, code, reason, msg, id) }
	if who.role == "" {
		bad(401, "Unauthorized", "authentication required")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/mini/v1/")
	if path == r.URL.Path {
		bad(404, "NotFound", "unknown path")
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 1 || len(parts) > 3 {
		bad(404, "NotFound", "unknown path")
		return
	}
	kind := parts[0]
	_, ok := newObject(kind)
	if !ok {
		bad(404, "NotFound", "unknown resource")
		return
	}
	name := ""
	if len(parts) > 1 {
		name = parts[1]
		if name == "" {
			bad(404, "NotFound", "empty name")
			return
		}
	}
	action := ""
	if len(parts) > 2 {
		action = parts[2]
	}
	if who.role == "agent" && ((r.Method != "GET" && action != "status" && action != "cleanup") || (kind != "pods" && kind != "nodes")) {
		bad(403, "Forbidden", "agent not authorized")
		return
	}
	if action == "bind" {
		if who.role != "scheduler" || r.Method != "POST" || kind != "pods" {
			bad(403, "Forbidden", "binding requires scheduler identity")
			return
		}
		s.bind(w, r, name, bad)
		return
	}
	if action == "status" {
		if who.role != "agent" || r.Method != "PUT" {
			bad(403, "Forbidden", "status requires agent identity")
			return
		}
		s.status(w, r, kind, name, who, bad)
		return
	}
	if action == "cleanup" {
		if who.role != "agent" || r.Method != "POST" || kind != "pods" {
			bad(403, "Forbidden", "cleanup requires the assigned agent")
			return
		}
		s.cleanup(w, r, name, who, bad)
		return
	}
	if action != "" {
		bad(404, "NotFound", "unknown action")
		return
	}
	if r.Method != "GET" && who.role != "admin" && !(who.role == "controller" && kind == "pods") {
		bad(403, "Forbidden", "write not authorized")
		return
	}
	if r.Method == "GET" && r.URL.Query().Get("watch") == "true" {
		if name != "" || who.role == "agent" {
			bad(403, "Forbidden", "watch not authorized")
			return
		}
		s.watch(w, r, kind, bad)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if r.Method == "GET" {
		k := key(kind, name)
		options := []clientv3.OpOption{}
		if name == "" {
			options = append(options, clientv3.WithPrefix())
		}
		resp, err := s.Client.Get(ctx, k, options...)
		if err != nil {
			bad(503, "Unavailable", "storage unavailable")
			return
		}
		if name != "" && len(resp.Kvs) == 0 {
			bad(404, "NotFound", "resource not found")
			return
		}
		items := make([]any, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			o, err := decode(kv.Value, kind, kv.ModRevision)
			if err != nil {
				bad(503, "Unavailable", "stored resource invalid")
				return
			}
			if who.role == "agent" && !s.agentCanRead(ctx, who, kind, o) {
				continue
			}
			items = append(items, o.value)
		}
		if name != "" {
			if len(items) == 0 {
				bad(403, "Forbidden", "resource not authorized")
				return
			}
			if len(resp.Kvs) == 1 {
				setAuditUID(ctx, func() string { o, _ := decode(resp.Kvs[0].Value, kind, resp.Kvs[0].ModRevision); return o.meta.UID }())
			}
			output(w, items[0])
			return
		}
		output(w, map[string]any{"items": items, "resourceVersion": resp.Header.Revision})
		return
	}
	if r.Method == "POST" && name == "" {
		status = http.StatusCreated
		s.create(ctx, w, r, kind, bad)
		return
	}
	if r.Method == "PUT" && name != "" {
		s.update(ctx, w, r, kind, name, bad)
		return
	}
	if r.Method == "DELETE" && name != "" {
		s.delete(ctx, w, kind, name, bad)
		return
	}
	bad(405, "MethodNotAllowed", "unsupported method")
}

type failFunc func(int, string, string)

func body(r *http.Request, o object) error {
	return api.DecodeStrict(io.LimitReader(r.Body, 1<<20), o.value)
}
func encode(o object) string { b, _ := json.Marshal(o.value); return string(b) }

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
	n, err := s.Client.Get(ctx, key("nodes", who.node))
	if err != nil || len(n.Kvs) != 1 {
		return false
	}
	obj, err := decode(n.Kvs[0].Value, "nodes", n.Kvs[0].ModRevision)
	if err != nil {
		return false
	}
	return o.value.(*api.Pod).NodeUID != "" && o.value.(*api.Pod).NodeUID == obj.meta.UID
}

func (s *Server) create(ctx context.Context, w http.ResponseWriter, r *http.Request, kind string, bad failFunc) {
	o, _ := newObject(kind)
	if err := body(r, o); err != nil {
		bad(400, "Invalid", err.Error())
		return
	}
	if err := valid(o); err != nil {
		bad(400, "Invalid", err.Error())
		return
	}
	if o.meta.UID != "" || o.meta.ResourceVersion != 0 || o.meta.DeletionTimestamp != nil || o.meta.OwnerUID != "" {
		bad(400, "Invalid", "system metadata must be empty")
		return
	}
	switch v := o.value.(type) {
	case *api.Pod:
		if v.NodeUID != "" || !reflect.DeepEqual(v.Status, api.PodStatus{}) {
			bad(403, "Forbidden", "binding and status are system fields")
			return
		}
		v.Status.Phase = "Pending"
	case *api.ReplicaSet:
		if v.Status != (api.ReplicaSetStatus{}) {
			bad(403, "Forbidden", "status is a system field")
			return
		}
	case *api.Node:
		if !reflect.DeepEqual(v.Status, api.NodeStatus{}) {
			bad(403, "Forbidden", "status is a system field")
			return
		}
	case *api.Service:
		if v.Spec.ClusterIP != "" {
			bad(403, "Forbidden", "clusterIP is a system field")
			return
		}
	}
	o.meta.UID = uid()
	if o.meta.Namespace == "" {
		o.meta.Namespace = "default"
	}
	k := key(kind, o.meta.Name)
	var tx *clientv3.TxnResponse
	var err error
	if rs, ok := o.value.(*api.ReplicaSet); ok {
		// A shared revision marker serializes selector checks with RS creation and deletion.
		marker := prefix + "replicaset-selector-revision"
		for attempt := 0; attempt < 5; attempt++ {
			snapshot, snapshotErr := s.Client.Txn(ctx).Then(clientv3.OpGet(key("replicasets", ""), clientv3.WithPrefix()), clientv3.OpGet(marker)).Commit()
			if snapshotErr != nil {
				bad(503, "Unavailable", "storage unavailable")
				return
			}
			for _, item := range snapshot.Responses[0].GetResponseRange().Kvs {
				existing, decodeErr := decode(item.Value, "replicasets", item.ModRevision)
				if decodeErr != nil {
					bad(503, "Unavailable", "invalid stored ReplicaSet")
					return
				}
				if selectorsOverlap(existing.value.(*api.ReplicaSet).Spec.Selector, rs.Spec.Selector) {
					bad(409, "Conflict", "ReplicaSet selectors overlap")
					return
				}
			}
			var markerRevision int64
			markerKVs := snapshot.Responses[1].GetResponseRange().Kvs
			if len(markerKVs) > 0 {
				markerRevision = markerKVs[0].ModRevision
			}
			tx, err = s.Client.Txn(ctx).If(clientv3.Compare(clientv3.Version(k), "=", 0), clientv3.Compare(clientv3.ModRevision(marker), "=", markerRevision)).Then(clientv3.OpPut(k, encode(o)), clientv3.OpPut(marker, o.meta.UID)).Commit()
			if err != nil || tx.Succeeded {
				break
			}
		}
	} else {
		tx, err = s.Client.Txn(ctx).If(clientv3.Compare(clientv3.Version(k), "=", 0)).Then(clientv3.OpPut(k, encode(o))).Commit()
	}
	if err != nil {
		bad(503, "Unavailable", "storage unavailable")
		return
	}
	if tx == nil || !tx.Succeeded {
		bad(409, "Conflict", "resource already exists or concurrent selector change")
		return
	}
	setAuditUID(ctx, o.meta.UID)
	o.meta.ResourceVersion = tx.Header.Revision
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	output(w, o.value)
}

func (s *Server) update(ctx context.Context, w http.ResponseWriter, r *http.Request, kind, name string, bad failFunc) {
	next, _ := newObject(kind)
	if err := body(r, next); err != nil {
		bad(400, "Invalid", err.Error())
		return
	}
	if next.meta.Name != name || next.meta.ResourceVersion <= 0 {
		bad(400, "Invalid", "name and resourceVersion required")
		return
	}
	get, err := s.Client.Get(ctx, key(kind, name))
	if err != nil {
		bad(503, "Unavailable", "storage unavailable")
		return
	}
	if len(get.Kvs) == 0 {
		bad(404, "NotFound", "resource not found")
		return
	}
	old, err := decode(get.Kvs[0].Value, kind, get.Kvs[0].ModRevision)
	if err != nil {
		bad(503, "Unavailable", "stored resource invalid")
		return
	}
	setAuditUID(ctx, old.meta.UID)
	if next.meta.ResourceVersion != old.meta.ResourceVersion {
		bad(409, "Conflict", "resourceVersion conflict")
		return
	}
	if err := valid(next); err != nil {
		bad(400, "Invalid", err.Error())
		return
	}
	if next.meta.UID != old.meta.UID || next.meta.OwnerUID != old.meta.OwnerUID || next.meta.Namespace != old.meta.Namespace || !reflect.DeepEqual(next.meta.DeletionTimestamp, old.meta.DeletionTimestamp) {
		bad(400, "Invalid", "system metadata is immutable")
		return
	}
	switch v := next.value.(type) {
	case *api.Pod:
		prior := old.value.(*api.Pod)
		if !reflect.DeepEqual(v.Status, prior.Status) {
			bad(403, "Forbidden", "status is a system field")
			return
		}
		if err := api.ValidatePodUpdate(*prior, *v); err != nil {
			bad(400, "Invalid", err.Error())
			return
		}
	case *api.ReplicaSet:
		prior := old.value.(*api.ReplicaSet)
		if v.Status != prior.Status {
			bad(403, "Forbidden", "status is a system field")
			return
		}
		if err := api.ValidateReplicaSetUpdate(*prior, *v); err != nil {
			bad(400, "Invalid", err.Error())
			return
		}
		previousMetadata, newMetadata := prior.Metadata, v.Metadata
		previousMetadata.ResourceVersion, newMetadata.ResourceVersion = 0, 0
		if !reflect.DeepEqual(previousMetadata, newMetadata) {
			bad(400, "Invalid", "ReplicaSet metadata is immutable")
			return
		}
	case *api.Node:
		if !reflect.DeepEqual(v.Status, old.value.(*api.Node).Status) {
			bad(403, "Forbidden", "status is a system field")
			return
		}
	case *api.Service:
		if v.Spec.ClusterIP != old.value.(*api.Service).Spec.ClusterIP {
			bad(403, "Forbidden", "clusterIP is a system field")
			return
		}
	}
	cas, err := s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key(kind, name)), "=", old.meta.ResourceVersion)).Then(clientv3.OpPut(key(kind, name), encode(next))).Commit()
	if err != nil {
		bad(503, "Unavailable", "storage unavailable")
		return
	}
	if !cas.Succeeded {
		bad(409, "Conflict", "resourceVersion conflict")
		return
	}
	next.meta.ResourceVersion = cas.Header.Revision
	output(w, next.value)
}

func (s *Server) delete(ctx context.Context, w http.ResponseWriter, kind, name string, bad failFunc) {
	k := key(kind, name)
	get, err := s.Client.Get(ctx, k)
	if err != nil {
		bad(503, "Unavailable", "storage unavailable")
		return
	}
	if len(get.Kvs) == 0 {
		bad(404, "NotFound", "resource not found")
		return
	}
	old, err := decode(get.Kvs[0].Value, kind, get.Kvs[0].ModRevision)
	if err != nil {
		bad(503, "Unavailable", "stored resource invalid")
		return
	}
	setAuditUID(ctx, old.meta.UID)
	op := clientv3.OpDelete(k)
	if p, ok := old.value.(*api.Pod); ok && p.NodeUID != "" {
		if old.meta.DeletionTimestamp == nil {
			now := time.Now().UTC()
			old.meta.DeletionTimestamp = &now
		}
		op = clientv3.OpPut(k, encode(old))
	}
	ops := []clientv3.Op{op}
	if kind == "replicasets" {
		ops = append(ops, clientv3.OpPut(prefix+"replicaset-selector-revision", uid()))
	}
	res, err := s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(k), "=", old.meta.ResourceVersion)).Then(ops...).Commit()
	if err != nil {
		bad(503, "Unavailable", "storage unavailable")
		return
	}
	if !res.Succeeded {
		bad(409, "Conflict", "resourceVersion conflict")
		return
	}
	output(w, map[string]string{"result": "deleted"})
}

func (s *Server) watch(w http.ResponseWriter, r *http.Request, kind string, bad failFunc) {
	rev, err := strconv.ParseInt(r.URL.Query().Get("resourceVersion"), 10, 64)
	if err != nil || rev < 1 {
		bad(400, "Invalid", "resourceVersion required")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		bad(500, "Internal", "streaming unsupported")
		return
	}
	ch := s.Client.Watch(r.Context(), key(kind, ""), clientv3.WithPrefix(), clientv3.WithRev(rev+1), clientv3.WithPrevKV())
	w.Header().Set("Content-Type", "application/x-ndjson")
	flusher.Flush()
	for response := range ch {
		if response.Err() != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "ERROR", "reason": "watch ended; relist"})
			flusher.Flush()
			return
		}
		for _, event := range response.Events {
			kv := event.Kv
			if event.Type == clientv3.EventTypeDelete {
				kv = event.PrevKv
			}
			if kv == nil {
				continue
			}
			o, err := decode(kv.Value, kind, event.Kv.ModRevision)
			if err != nil {
				continue
			}
			typ := "PUT"
			if event.Type == clientv3.EventTypeDelete {
				typ = "DELETE"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"type": typ, "object": o.value, "resourceVersion": event.Kv.ModRevision})
			flusher.Flush()
		}
	}
}

type bindRequest struct {
	NodeName        string `json:"nodeName"`
	NodeUID         string `json:"nodeUID"`
	PodUID          string `json:"podUID"`
	ResourceVersion int64  `json:"resourceVersion"`
}

func (s *Server) bind(w http.ResponseWriter, r *http.Request, name string, bad failFunc) {
	var req bindRequest
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil {
		bad(400, "Invalid", err.Error())
		return
	}
	if req.NodeName == "" || req.NodeUID == "" || req.PodUID == "" || req.ResourceVersion <= 0 {
		bad(400, "Invalid", "binding identifiers and version required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	for attempt := 0; attempt < 5; attempt++ {
		response, err := s.Client.Txn(ctx).Then(clientv3.OpGet(key("pods", name)), clientv3.OpGet(key("nodes", req.NodeName))).Commit()
		if err != nil {
			bad(503, "Unavailable", "storage unavailable")
			return
		}
		pods, nodes := response.Responses[0].GetResponseRange().Kvs, response.Responses[1].GetResponseRange().Kvs
		if len(pods) == 0 || len(nodes) == 0 {
			bad(404, "NotFound", "pod or node not found")
			return
		}
		po, err := decode(pods[0].Value, "pods", pods[0].ModRevision)
		if err != nil {
			bad(503, "Unavailable", "invalid stored pod")
			return
		}
		no, err := decode(nodes[0].Value, "nodes", nodes[0].ModRevision)
		if err != nil {
			bad(503, "Unavailable", "invalid stored node")
			return
		}
		p, n := po.value.(*api.Pod), no.value.(*api.Node)
		setAuditUID(ctx, p.Metadata.UID)
		if p.Metadata.UID != req.PodUID || n.Metadata.UID != req.NodeUID || p.Metadata.DeletionTimestamp != nil {
			bad(409, "Conflict", "UID mismatch or deleting pod")
			return
		}
		if p.NodeUID == n.Metadata.UID {
			output(w, p)
			return
		}
		if p.NodeUID != "" || p.Metadata.ResourceVersion != req.ResourceVersion || !n.Status.Ready || n.Spec.Unschedulable {
			bad(409, "Conflict", "pod or node cannot be bound")
			return
		}
		request := p.Spec.Resources.Requests
		reserved, capacity := n.Status.Reserved, n.Spec.Allocatable
		if request.CPUMilli > capacity.CPUMilli-reserved.CPUMilli || request.MemoryBytes > capacity.MemoryBytes-reserved.MemoryBytes {
			bad(409, "Conflict", "insufficient resources")
			return
		}
		n.Status.Reserved.CPUMilli += request.CPUMilli
		n.Status.Reserved.MemoryBytes += request.MemoryBytes
		p.NodeUID = n.Metadata.UID
		tx, err := s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key("pods", name)), "=", pods[0].ModRevision), clientv3.Compare(clientv3.ModRevision(key("nodes", req.NodeName)), "=", nodes[0].ModRevision)).Then(clientv3.OpPut(key("pods", name), encode(po)), clientv3.OpPut(key("nodes", req.NodeName), encode(no))).Commit()
		if err != nil {
			bad(503, "Unavailable", "storage unavailable")
			return
		}
		if tx.Succeeded {
			p.Metadata.ResourceVersion = tx.Header.Revision
			output(w, p)
			return
		}
	}
	bad(409, "Conflict", "concurrent binding conflict")
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, kind, name string, who identity, bad failFunc) {
	if kind != "pods" && kind != "nodes" {
		bad(403, "Forbidden", "unsupported status")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	get, err := s.Client.Get(ctx, key(kind, name))
	if err != nil {
		bad(503, "Unavailable", "storage unavailable")
		return
	}
	if len(get.Kvs) == 0 {
		bad(404, "NotFound", "resource not found")
		return
	}
	old, err := decode(get.Kvs[0].Value, kind, get.Kvs[0].ModRevision)
	if err != nil || !s.agentCanRead(ctx, who, kind, old) {
		bad(403, "Forbidden", "resource not authorized")
		return
	}
	setAuditUID(ctx, old.meta.UID)
	var req struct {
		ResourceVersion int64           `json:"resourceVersion"`
		Status          json.RawMessage `json:"status"`
	}
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil {
		bad(400, "Invalid", err.Error())
		return
	}
	if req.ResourceVersion != old.meta.ResourceVersion {
		bad(409, "Conflict", "resourceVersion conflict")
		return
	}
	switch v := old.value.(type) {
	case *api.Pod:
		var st api.PodStatus
		if err := api.DecodeStrict(strings.NewReader(string(req.Status)), &st); err != nil {
			bad(400, "Invalid", err.Error())
			return
		}
		switch st.Phase {
		case "Pending", "Running", "Succeeded", "Failed", "Unknown":
		default:
			bad(400, "Invalid", "unknown Pod phase")
			return
		}
		if st.RestartCount < v.Status.RestartCount || (st.Ready && st.Phase != "Running") {
			bad(400, "Invalid", "invalid readiness or restart count")
			return
		}
		if (v.Status.Phase == "Succeeded" || v.Status.Phase == "Failed") && st.Phase != v.Status.Phase {
			bad(409, "Conflict", "terminal Pod cannot change phase")
			return
		}
		v.Status = st
	case *api.Node:
		var st api.NodeStatus
		if err := api.DecodeStrict(strings.NewReader(string(req.Status)), &st); err != nil {
			bad(400, "Invalid", err.Error())
			return
		}
		if st.Reserved != v.Status.Reserved {
			bad(403, "Forbidden", "reservation is a system field")
			return
		}
		v.Status = st
	}
	res, err := s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key(kind, name)), "=", req.ResourceVersion)).Then(clientv3.OpPut(key(kind, name), encode(old))).Commit()
	if err != nil {
		bad(503, "Unavailable", "storage unavailable")
		return
	}
	if !res.Succeeded {
		bad(409, "Conflict", "resourceVersion conflict")
		return
	}
	old.meta.ResourceVersion = res.Header.Revision
	output(w, old.value)
}

// cleanup confirms that the agent has stopped and removed the matching local
// instance. Releasing its reservation and removing the Pod must be atomic.
func (s *Server) cleanup(w http.ResponseWriter, r *http.Request, name string, who identity, bad failFunc) {
	var req struct {
		UID             string `json:"uid"`
		ResourceVersion int64  `json:"resourceVersion"`
	}
	if err := api.DecodeStrict(io.LimitReader(r.Body, 1<<20), &req); err != nil || req.UID == "" || req.ResourceVersion <= 0 {
		bad(400, "Invalid", "uid and resourceVersion required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	for attempt := 0; attempt < 5; attempt++ {
		res, err := s.Client.Txn(ctx).Then(clientv3.OpGet(key("pods", name)), clientv3.OpGet(key("nodes", who.node))).Commit()
		if err != nil {
			bad(503, "Unavailable", "storage unavailable")
			return
		}
		pods, nodes := res.Responses[0].GetResponseRange().Kvs, res.Responses[1].GetResponseRange().Kvs
		if len(pods) == 0 {
			output(w, map[string]string{"result": "already absent"})
			return
		}
		if len(nodes) == 0 {
			bad(403, "Forbidden", "node not registered")
			return
		}
		po, err := decode(pods[0].Value, "pods", pods[0].ModRevision)
		if err != nil {
			bad(503, "Unavailable", "invalid stored pod")
			return
		}
		no, err := decode(nodes[0].Value, "nodes", nodes[0].ModRevision)
		if err != nil {
			bad(503, "Unavailable", "invalid stored node")
			return
		}
		p, n := po.value.(*api.Pod), no.value.(*api.Node)
		setAuditUID(ctx, p.Metadata.UID)
		if p.Metadata.UID != req.UID || p.Metadata.ResourceVersion != req.ResourceVersion || p.Metadata.DeletionTimestamp == nil || p.NodeUID != n.Metadata.UID {
			bad(409, "Conflict", "pod identity, deletion or binding mismatch")
			return
		}
		requests := p.Spec.Resources.Requests
		if n.Status.Reserved.CPUMilli < requests.CPUMilli || n.Status.Reserved.MemoryBytes < requests.MemoryBytes {
			bad(409, "Conflict", "invalid reservation")
			return
		}
		n.Status.Reserved.CPUMilli -= requests.CPUMilli
		n.Status.Reserved.MemoryBytes -= requests.MemoryBytes
		result, err := s.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key("pods", name)), "=", pods[0].ModRevision), clientv3.Compare(clientv3.ModRevision(key("nodes", who.node)), "=", nodes[0].ModRevision)).Then(clientv3.OpDelete(key("pods", name)), clientv3.OpPut(key("nodes", who.node), encode(no))).Commit()
		if err != nil {
			bad(503, "Unavailable", "storage unavailable")
			return
		}
		if result.Succeeded {
			output(w, map[string]string{"result": "cleaned"})
			return
		}
	}
	bad(409, "Conflict", "concurrent cleanup conflict")
}

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
