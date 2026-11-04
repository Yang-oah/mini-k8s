package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mini-k8s/internal/api"
)

type memoryInstaller struct {
	routes  []Route
	applied bool
}

func (m *memoryInstaller) Apply(_ context.Context, routes []Route) error {
	m.applied = true
	m.routes = routes
	return nil
}
func (*memoryInstaller) Close() error { return nil }

func TestRoutesRequireCurrentReadyProof(t *testing.T) {
	now := time.Now()
	node := api.Node{Metadata: api.Metadata{Name: "worker", UID: "node-1"}, Spec: api.NodeSpec{PodCIDR: "10.88.0.0/24"}, Status: api.NodeStatus{Ready: true, LastHeartbeat: now, ReadinessEpoch: 3}}
	pod := api.Pod{Metadata: api.Metadata{Name: "pod", UID: "pod-1", Labels: map[string]string{"app": "web"}}, NodeUID: "node-1", Status: api.PodStatus{Phase: "Running", Ready: true, ReadinessEpoch: 3, PodIP: "10.88.0.2"}}
	svc := api.Service{Metadata: api.Metadata{Name: "web", UID: "svc-1"}, Spec: api.ServiceSpec{ClusterIP: "10.96.0.1", Port: 80, TargetPort: 8080, Selector: map[string]string{"app": "web"}}}
	routes, err := buildRoutes([]api.Service{svc}, []api.Pod{pod}, []api.Node{node})
	if err != nil || len(routes) != 1 || len(routes[0].Endpoints) != 1 || routes[0].Endpoints[0].Port != 8080 {
		t.Fatalf("eligible endpoint missing: %#v %v", routes, err)
	}

	for _, variant := range []struct {
		name   string
		change func(*api.Pod, *api.Node)
	}{
		{"stale readiness epoch", func(p *api.Pod, n *api.Node) { n.Status.ReadinessEpoch++ }},
		{"node unknown", func(p *api.Pod, n *api.Node) { n.Status.Ready = false }},
		{"missing heartbeat", func(p *api.Pod, n *api.Node) { n.Status.LastHeartbeat = time.Time{} }},
		{"deleting pod", func(p *api.Pod, n *api.Node) { d := now; p.Metadata.DeletionTimestamp = &d }},
		{"label mismatch", func(p *api.Pod, n *api.Node) { p.Metadata.Labels = map[string]string{"app": "other"} }},
		{"not ready", func(p *api.Pod, n *api.Node) { p.Status.Ready = false }},
		{"address outside node CIDR", func(p *api.Pod, n *api.Node) { p.Status.PodIP = "10.89.0.2" }},
		{"noncanonical node CIDR", func(p *api.Pod, n *api.Node) { n.Spec.PodCIDR = "10.88.0.7/24" }},
	} {
		t.Run(variant.name, func(t *testing.T) {
			p, n := pod, node
			variant.change(&p, &n)
			got, err := buildRoutes([]api.Service{svc}, []api.Pod{p}, []api.Node{n})
			if err != nil || len(got) != 1 || len(got[0].Endpoints) != 0 {
				t.Fatalf("unsafe endpoint selected: %#v %v", got, err)
			}
		})
	}
}

func TestRejectInvalidOrDuplicateServiceIP(t *testing.T) {
	svc := api.Service{Metadata: api.Metadata{UID: "svc-1"}, Spec: api.ServiceSpec{ClusterIP: "10.96.0.1", Port: 80, TargetPort: 8080, Selector: map[string]string{"app": "web"}}}
	if _, err := buildRoutes([]api.Service{svc, svc}, nil, nil); err == nil {
		t.Fatal("accepted duplicate cluster IP")
	}
	svc.Spec.ClusterIP = "10.96.0.1\nCOMMIT"
	if _, err := buildRoutes([]api.Service{svc}, nil, nil); err == nil {
		t.Fatal("accepted injected address")
	}
}

func TestSelectorRequiresKeyEvenForEmptyValue(t *testing.T) {
	if matches(map[string]string{"optional": ""}, nil) {
		t.Fatal("a missing label must not match an empty selector value")
	}
	if !matches(map[string]string{"optional": ""}, map[string]string{"optional": ""}) {
		t.Fatal("an explicitly empty label should match")
	}
}

func TestReconcileRemovesPodThatDisappearedAfterList(t *testing.T) {
	now := time.Now().UTC()
	svc := api.Service{Metadata: api.Metadata{Name: "web", UID: "svc", ResourceVersion: 3}, Spec: api.ServiceSpec{ClusterIP: "10.96.0.1", Port: 80, TargetPort: 8080, Selector: map[string]string{"app": "web"}}}
	pod := api.Pod{Metadata: api.Metadata{Name: "gone", UID: "pod", Labels: map[string]string{"app": "web"}}, NodeUID: "node", Status: api.PodStatus{Ready: true, Phase: "Running", PodIP: "10.88.0.2", ReadinessEpoch: 1}}
	node := api.Node{Metadata: api.Metadata{Name: "worker", UID: "node"}, Spec: api.NodeSpec{PodCIDR: "10.88.0.0/24"}, Status: api.NodeStatus{Ready: true, LastHeartbeat: now, ReadinessEpoch: 1}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/mini/v1/services":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []api.Service{svc}})
		case "/api/mini/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []api.Pod{pod}})
		case "/api/mini/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []api.Node{node}})
		case "/api/mini/v1/services/web":
			_ = json.NewEncoder(w).Encode(svc)
		case "/api/mini/v1/nodes/worker":
			_ = json.NewEncoder(w).Encode(node)
		case "/api/mini/v1/pods/gone":
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	installer := &memoryInstaller{}
	p := &Proxy{base: server.URL, client: server.Client(), installer: installer}
	if err := p.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !installer.applied || len(installer.routes) != 1 || len(installer.routes[0].Endpoints) != 0 {
		t.Fatalf("deleted Pod kept as endpoint: %#v", installer.routes)
	}
}
