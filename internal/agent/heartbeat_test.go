package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mini-k8s/internal/api"
)

func TestHeartbeatKeepsReservationsAndRejectsRecreatedNode(t *testing.T) {
	node := api.Node{Metadata: api.Metadata{Name: "node-a", UID: "first", ResourceVersion: 12}, Status: api.NodeStatus{Reserved: api.Resources{CPUMilli: 100, MemoryBytes: 128}}}
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(node)
		case http.MethodPut:
			var request struct {
				ResourceVersion int64          `json:"resourceVersion"`
				Status          api.NodeStatus `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.ResourceVersion != 12 || !request.Status.Ready || request.Status.Reserved != node.Status.Reserved {
				t.Errorf("heartbeat overwrote node state: %+v", request)
			}
			puts++
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	a := &Agent{base: server.URL, node: "node-a", client: server.Client()}
	if err := a.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	node.Metadata.UID = "replacement"
	if err := a.heartbeat(context.Background()); err == nil {
		t.Fatal("heartbeat accepted recreated node")
	}
	if puts != 1 {
		t.Fatalf("unexpected status writes: %d", puts)
	}
}
