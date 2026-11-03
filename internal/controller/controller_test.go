package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mini-k8s/internal/api"
)

type fakeAPI struct {
	pods               map[string]api.Pod
	creates            []string
	createAttempts     []string
	deletes            []string
	finalizes          int
	dropCreateResponse bool
	delayCreate        bool
}

func (f *fakeAPI) RoundTrip(r *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	path := strings.TrimPrefix(r.URL.Path, prefix)
	switch {
	case path == "pods" && r.Method == http.MethodGet:
		list := podList{}
		for _, pod := range f.pods {
			list.Items = append(list.Items, pod)
		}
		json.NewEncoder(w).Encode(list)
	case path == "pods" && r.Method == http.MethodPost:
		var p api.Pod
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			return nil, err
		}
		f.createAttempts = append(f.createAttempts, p.Metadata.Name)
		if f.delayCreate {
			f.delayCreate = false
			return nil, errors.New("create outcome unknown")
		}
		if _, exists := f.pods[p.Metadata.Name]; exists {
			w.WriteHeader(http.StatusConflict)
			break
		}
		p.Metadata.UID = "uid-" + p.Metadata.Name
		f.pods[p.Metadata.Name] = p
		f.creates = append(f.creates, p.Metadata.Name)
		if f.dropCreateResponse {
			f.dropCreateResponse = false
			return nil, errors.New("response lost after commit")
		}
		w.WriteHeader(http.StatusCreated)
	case strings.HasPrefix(path, "pods/") && r.Method == http.MethodGet:
		p, ok := f.pods[strings.TrimPrefix(path, "pods/")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			break
		}
		json.NewEncoder(w).Encode(p)
	case strings.HasPrefix(path, "pods/") && r.Method == http.MethodDelete:
		name := strings.TrimPrefix(path, "pods/")
		p, ok := f.pods[name]
		if !ok || p.Metadata.UID != r.URL.Query().Get("uid") {
			w.WriteHeader(http.StatusConflict)
			break
		}
		f.deletes = append(f.deletes, name)
		if p.NodeUID == "" {
			delete(f.pods, name)
		} else {
			now := time.Now()
			p.Metadata.DeletionTimestamp = &now
			f.pods[name] = p
		}
	case strings.HasPrefix(path, "replicasets/") && strings.HasSuffix(path, "/finalize") && r.Method == http.MethodPost:
		f.finalizes++
	default:
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.String())
	}
	return w.Result(), nil
}

func testController(f *fakeAPI) *Controller {
	return &Controller{base: "https://api.example", token: "controller", client: &http.Client{Transport: f}, resync: time.Second}
}

func testReplicaSet() api.ReplicaSet {
	return api.ReplicaSet{APIVersion: api.Version, Kind: "ReplicaSet", Metadata: api.Metadata{Name: "web", UID: "rs-uid", ResourceVersion: 5}, Spec: api.ReplicaSetSpec{Replicas: 1, Selector: map[string]string{"app": "web"}, Template: api.PodTemplate{Labels: map[string]string{"app": "web"}, Spec: api.PodSpec{Image: "image", RestartPolicy: "Always"}}}}
}

func TestLostCreateResponseDoesNotCreateDuplicate(t *testing.T) {
	f := &fakeAPI{pods: map[string]api.Pod{}, dropCreateResponse: true}
	c := testController(f)
	rs := testReplicaSet()
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if len(f.pods) != 1 || len(f.creates) != 1 {
		t.Fatalf("pods=%d creates=%v", len(f.pods), f.creates)
	}
}

func TestUnknownCreateRetriesSameNameAfterAbsentRead(t *testing.T) {
	f := &fakeAPI{pods: map[string]api.Pod{}, delayCreate: true}
	c := testController(f)
	rs := testReplicaSet()
	if err := c.reconcile(context.Background(), rs); err == nil {
		t.Fatal("unconfirmed create must not be reported as successful")
	}
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if len(f.createAttempts) != 2 || f.createAttempts[0] != f.createAttempts[1] {
		t.Fatalf("unconfirmed request changed names: %v", f.createAttempts)
	}
	if len(f.pods) != 1 {
		t.Fatalf("pods = %d, want 1", len(f.pods))
	}
}

func TestReplicaScaleConvergesWithoutAdoptingIndependentPod(t *testing.T) {
	rs := testReplicaSet()
	f := &fakeAPI{pods: map[string]api.Pod{
		"standalone": {Metadata: api.Metadata{Name: "standalone", UID: "standalone-uid", Labels: map[string]string{"app": "web"}}},
	}}
	c := testController(f)
	for _, desired := range []int{1, 3, 1, 0} {
		rs.Spec.Replicas = desired
		for i := 0; i < 5; i++ {
			if err := c.reconcile(context.Background(), rs); err != nil {
				t.Fatal(err)
			}
		}
		owned := 0
		for _, pod := range f.pods {
			if pod.Metadata.OwnerUID == rs.Metadata.UID {
				owned++
			}
		}
		if owned != desired {
			t.Fatalf("replicas = %d, owned = %d", desired, owned)
		}
		if _, ok := f.pods["standalone"]; !ok {
			t.Fatal("同标签独立 Pod 被控制器删除")
		}
	}
}

func TestTerminatingUnknownRemainsCounted(t *testing.T) {
	now := time.Now()
	rs := testReplicaSet()
	p := api.Pod{Metadata: api.Metadata{Name: "old", UID: "old-uid", OwnerUID: rs.Metadata.UID, DeletionTimestamp: &now}, NodeUID: "node-uid", Status: api.PodStatus{Phase: "Unknown"}}
	f := &fakeAPI{pods: map[string]api.Pod{"old": p}}
	c := testController(f)
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if len(f.creates) != 0 {
		t.Fatal("replaced unconfirmed terminating Pod")
	}
	rs.Spec.Replicas = 3
	for i := 0; i < 3; i++ {
		if err := c.reconcile(context.Background(), rs); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.creates) != 2 {
		t.Fatalf("created %d Pods; want 2", len(f.creates))
	}
	delete(f.pods, "old") // Simulate API confirmation that runtime cleanup completed.
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if len(f.creates) != 3 {
		t.Fatalf("created %d Pods after cleanup; want 3", len(f.creates))
	}
}

func TestScaleDownPrefersUnboundAndDeletionWaitsForCleanup(t *testing.T) {
	rs := testReplicaSet()
	rs.Spec.Replicas = 1
	f := &fakeAPI{pods: map[string]api.Pod{
		"bound":       {Metadata: api.Metadata{Name: "bound", UID: "1", OwnerUID: rs.Metadata.UID}, NodeUID: "node"},
		"unbound":     {Metadata: api.Metadata{Name: "unbound", UID: "2", OwnerUID: rs.Metadata.UID}},
		"independent": {Metadata: api.Metadata{Name: "independent", UID: "3"}},
	}}
	c := testController(f)
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if len(f.deletes) != 1 || f.deletes[0] != "unbound" {
		t.Fatalf("deletions=%v", f.deletes)
	}
	now := time.Now()
	rs.Metadata.DeletionTimestamp = &now
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if len(f.creates) != 0 || f.finalizes != 0 {
		t.Fatal("deleted RS created pod or finalized before cleanup")
	}
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if f.finalizes != 0 {
		t.Fatal("finalized while bound Pod remains")
	}
	delete(f.pods, "bound")
	if err := c.reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	if f.finalizes != 1 {
		t.Fatalf("finalizes=%d", f.finalizes)
	}
	if _, ok := f.pods["independent"]; !ok {
		t.Fatal("independent pod removed")
	}
}
