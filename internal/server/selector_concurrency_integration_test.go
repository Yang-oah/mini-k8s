package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"mini-k8s/internal/api"
)

// createSelectorTestRS uses the real API and a bounded request context. The
// caller retains successful identities so cleanup cannot delete another test's
// resource if a name is replaced.
func createSelectorTestRS(s *Server, rs api.ReplicaSet, ctx context.Context) *httptest.ResponseRecorder {
	body, err := json.Marshal(rs)
	if err != nil {
		panic(err) // api.ReplicaSet has no unmarshalable fields.
	}
	req := httptest.NewRequest(http.MethodPost, "/api/mini/v1/replicasets", bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer admin")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	return response
}

func cleanupSelectorTestRS(t *testing.T, s *Server, created []api.ReplicaSet) {
	t.Helper()
	for _, rs := range created {
		path := "replicasets/" + rs.Metadata.Name + "?uid=" + rs.Metadata.UID
		response := apiRequest(t, s, "admin", http.MethodDelete, path, nil)
		if response.Code != http.StatusOK && response.Code != http.StatusNotFound {
			t.Errorf("cleanup %s: HTTP %d: %s", rs.Metadata.Name, response.Code, response.Body.String())
		}
	}
}

func runSelectorCreates(s *Server, objects []api.ReplicaSet, others ...*Server) []*httptest.ResponseRecorder {
	servers := append([]*Server{s}, others...)
	results := make([]*httptest.ResponseRecorder, len(objects))
	start := make(chan struct{})
	var group sync.WaitGroup
	for i := range objects {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			<-start
			results[index] = createSelectorTestRS(servers[index%len(servers)], objects[index], ctx)
		}(i)
	}
	close(start)
	group.Wait()
	return results
}

func successfulSelectorCreates(t *testing.T, responses []*httptest.ResponseRecorder) []api.ReplicaSet {
	t.Helper()
	var created []api.ReplicaSet
	for _, response := range responses {
		if response.Code == http.StatusCreated {
			created = append(created, decodeResponse[api.ReplicaSet](t, response))
		}
	}
	return created
}

func TestSelectorNonOverlappingConcurrentCreates(t *testing.T) {
	s := testServer(t, "")
	other := testServer(t, "") // Independent API instances share only etcd.
	suffix := newUID()[:12]
	objects := make([]api.ReplicaSet, 32)
	for i := range objects {
		objects[i] = testReplicaSet(fmt.Sprintf("selector-%s-%02d", suffix, i))
	}
	responses := runSelectorCreates(s, objects, other)
	created := successfulSelectorCreates(t, responses)
	t.Cleanup(func() { cleanupSelectorTestRS(t, s, created) })
	for i, response := range responses {
		if response.Code != http.StatusCreated {
			t.Errorf("independent selector %s: HTTP %d: %s", objects[i].Metadata.Name, response.Code, response.Body.String())
		}
	}
}

func TestSelectorOverlappingConcurrentCreates(t *testing.T) {
	s := testServer(t, "")
	other := testServer(t, "")
	suffix := newUID()[:12]
	objects := make([]api.ReplicaSet, 8)
	for i := range objects {
		objects[i] = testReplicaSet(fmt.Sprintf("overlap-%s-%02d", suffix, i))
		labels := map[string]string{"selector-test": suffix}
		objects[i].Spec.Selector = labels
		objects[i].Spec.Template.Labels = labels
	}
	responses := runSelectorCreates(s, objects, other)
	created := successfulSelectorCreates(t, responses)
	t.Cleanup(func() { cleanupSelectorTestRS(t, s, created) })
	if len(created) != 1 {
		t.Errorf("overlapping selectors created %d ReplicaSets; want exactly one", len(created))
	}
	for _, response := range responses {
		if response.Code != http.StatusCreated && response.Code != http.StatusConflict {
			t.Errorf("overlapping selector returned HTTP %d: %s", response.Code, response.Body.String())
		}
	}
}

func TestSelectorSameNameConcurrentCreates(t *testing.T) {
	s := testServer(t, "")
	rs := testReplicaSet("same-name-" + newUID()[:12])
	responses := runSelectorCreates(s, []api.ReplicaSet{rs, rs})
	created := successfulSelectorCreates(t, responses)
	t.Cleanup(func() { cleanupSelectorTestRS(t, s, created) })
	if len(created) != 1 {
		t.Errorf("same name created %d ReplicaSets; want exactly one", len(created))
	}
	for _, response := range responses {
		if response.Code != http.StatusCreated && response.Code != http.StatusConflict {
			t.Errorf("same-name create returned HTTP %d: %s", response.Code, response.Body.String())
		}
	}
}

func TestSelectorCancelledCreateDoesNotPersist(t *testing.T) {
	s := testServer(t, "")
	rs := testReplicaSet("cancelled-" + newUID()[:12])
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response := createSelectorTestRS(s, rs, ctx)
	if response.Code == http.StatusCreated {
		created := decodeResponse[api.ReplicaSet](t, response)
		t.Cleanup(func() { cleanupSelectorTestRS(t, s, []api.ReplicaSet{created}) })
		t.Fatal("already-cancelled request created a ReplicaSet")
	}
	lookup := apiRequest(t, s, "admin", http.MethodGet, "replicasets/"+rs.Metadata.Name, nil)
	requireStatus(t, lookup, http.StatusNotFound)
}
