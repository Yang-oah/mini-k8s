package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
	"mini-k8s/internal/api"
)

func operationTestFixture(t *testing.T, replicas int) (*Server, api.ReplicaSet, string) {
	t.Helper()
	s := testServer(t, "node-"+newUID()[:8])
	name := "op-rs-" + strings.ToLower(newUID()[:12])
	spec := testReplicaSet(name)
	spec.Spec.Replicas = replicas
	response := apiRequest(t, s, "admin", http.MethodPost, "replicasets", spec)
	requireStatus(t, response, http.StatusCreated)
	rs := decodeResponse[api.ReplicaSet](t, response)
	t.Cleanup(func() {
		// 此套件只允许显式指定隔离 etcd；只清理本轮 UID 所有的记录。
		pods := apiRequest(t, s, "admin", http.MethodGet, "pods", nil)
		if pods.Code == http.StatusOK {
			var list struct {
				Items []api.Pod `json:"items"`
			}
			_ = json.Unmarshal(pods.Body.Bytes(), &list)
			for _, pod := range list.Items {
				if pod.Metadata.OwnerUID == rs.Metadata.UID {
					apiRequest(t, s, "admin", http.MethodDelete, "pods/"+pod.Metadata.Name, nil)
				}
			}
		}
		apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+name, nil)
		_, _ = s.Client.Delete(context.Background(), operationPrefix(rs.Metadata.UID), clientv3.WithPrefix())
	})
	return s, rs, "replicasets/" + name + "/create-operation"
}

func operationPodRevision(t *testing.T, s *Server) int64 {
	t.Helper()
	response := apiRequest(t, s, "controller", http.MethodGet, "pods", nil)
	requireStatus(t, response, http.StatusOK)
	var list struct {
		ResourceVersion int64 `json:"resourceVersion"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	return list.ResourceVersion
}

func TestReplicaOperationReplaysAfterCommit(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	revision := operationPodRevision(t, s)
	prepare := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": revision, "podName": "op-pod-" + newUID()[:12]}
	first := apiRequest(t, s, "controller", http.MethodPost, path, prepare)
	requireStatus(t, first, http.StatusOK)
	op := decodeResponse[replicaOperation](t, first)
	commit := map[string]any{"uid": op.UID, "operationID": op.OperationID, "podName": op.PodName}
	var group sync.WaitGroup
	responses := make(chan *httptest.ResponseRecorder, 8)
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() { defer group.Done(); responses <- apiRequest(t, s, "controller", http.MethodPut, path, commit) }()
	}
	group.Wait()
	close(responses)
	var podUID string
	for response := range responses {
		requireStatus(t, response, http.StatusOK)
		result := decodeResponse[replicaOperation](t, response)
		if result.State != "COMMITTED" || result.PodUID == "" {
			t.Fatal("missing durable commit result")
		}
		if podUID == "" {
			podUID = result.PodUID
		} else if podUID != result.PodUID {
			t.Fatal("concurrent retries created different Pod UIDs")
		}
	}
	replay := apiRequest(t, s, "controller", http.MethodPost, path, prepare)
	requireStatus(t, replay, http.StatusOK)
	result := decodeResponse[replicaOperation](t, replay)
	if result.OperationID != op.OperationID || result.PodUID != podUID {
		t.Fatal("replayed prepare did not return original committed operation")
	}
	stale := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": revision, "podName": "another-" + newUID()[:12]}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, stale), http.StatusConflict)
	conflict := map[string]any{"uid": op.UID, "operationID": op.OperationID, "podName": "different-name"}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPut, path, conflict), http.StatusConflict)
}

func TestReplicaOperationPendingAliasAndDelete(t *testing.T) {
	s, rs, path := operationTestFixture(t, 2)
	revision := operationPodRevision(t, s)
	prepare := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": revision, "podName": "op-pod-" + newUID()[:12]}
	first := apiRequest(t, s, "controller", http.MethodPost, path, prepare)
	requireStatus(t, first, http.StatusOK)
	op := decodeResponse[replicaOperation](t, first)
	alias := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": revision, "podName": "alias-" + newUID()[:12]}
	response := apiRequest(t, s, "controller", http.MethodPost, path, alias)
	requireStatus(t, response, http.StatusOK)
	if decodeResponse[replicaOperation](t, response).OperationID != op.OperationID {
		t.Fatal("pending alias got new operation")
	}
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+rs.Metadata.Name, nil), http.StatusOK)
	replay := apiRequest(t, s, "controller", http.MethodPost, path, alias)
	requireStatus(t, replay, http.StatusOK)
	cancelled := decodeResponse[replicaOperation](t, replay)
	if cancelled.OperationID != op.OperationID || cancelled.State != "CANCELLED" {
		t.Fatal("RS deletion must cancel and preserve operation")
	}
	query := path + "?uid=" + rs.Metadata.UID + "&operationID=" + strconv.FormatInt(op.OperationID, 10)
	requireStatus(t, apiRequest(t, s, "controller", http.MethodGet, query, nil), http.StatusOK)
}

func TestAdminDeleteRejectsMismatchedExplicitUID(t *testing.T) {
	s := testServer(t, "")
	name := "uid-pod-" + newUID()[:12]
	pod := api.Pod{APIVersion: api.Version, Kind: "Pod", Metadata: api.Metadata{Name: name}, Spec: testPodSpec()}
	created := apiRequest(t, s, "admin", http.MethodPost, "pods", pod)
	requireStatus(t, created, http.StatusCreated)
	uid := decodeResponse[api.Pod](t, created).Metadata.UID
	t.Cleanup(func() { apiRequest(t, s, "admin", http.MethodDelete, "pods/"+name+"?uid="+uid, nil) })
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "pods/"+name+"?uid="+newUID(), nil), http.StatusConflict)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodGet, "pods/"+name, nil), http.StatusOK)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "pods/"+name+"?uid="+uid, nil), http.StatusOK)
}

// 已绑定的终态 Pod 开始删除后仍占额度；创建事务必须观察到这一变化。
func TestBoundTerminalDeletionInvalidatesReplicaSnapshot(t *testing.T) {
	suffix := newUID()[:12]
	rsName, podName, nodeName := "marker-rs-"+suffix, "marker-pod-"+suffix, "marker-node-"+suffix
	s := testServer(t, nodeName)
	var ownerUID, nodeUID string
	t.Cleanup(func() { cleanupResources(t, s, podName, rsName, nodeName, ownerUID, nodeUID) })
	rsResponse := apiRequest(t, s, "admin", http.MethodPost, "replicasets", testReplicaSet(rsName))
	requireStatus(t, rsResponse, http.StatusCreated)
	rs := decodeResponse[api.ReplicaSet](t, rsResponse)
	ownerUID = rs.Metadata.UID
	created := createOwnedOperation(t, s, rs, podName)
	requireStatus(t, created, http.StatusCreated)
	pod := decodeResponse[api.Pod](t, created)
	nodeResponse := apiRequest(t, s, "admin", http.MethodPost, "nodes", api.Node{
		APIVersion: api.Version, Kind: "Node", Metadata: api.Metadata{Name: nodeName},
		Spec: api.NodeSpec{PodCIDR: "10.3.0.0/24", Allocatable: api.Resources{CPUMilli: 500, MemoryBytes: 512}},
	})
	requireStatus(t, nodeResponse, http.StatusCreated)
	node := decodeResponse[api.Node](t, nodeResponse)
	nodeUID = node.Metadata.UID
	requireStatus(t, apiRequest(t, s, "agent", http.MethodPut, "nodes/"+nodeName+"/status", map[string]any{"resourceVersion": node.Metadata.ResourceVersion, "status": api.NodeStatus{Ready: true}}), http.StatusOK)
	bound := apiRequest(t, s, "scheduler", http.MethodPost, "pods/"+podName+"/bind", map[string]any{"nodeName": nodeName, "nodeUID": nodeUID, "podUID": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion})
	requireStatus(t, bound, http.StatusOK)
	pod = decodeResponse[api.Pod](t, bound)
	requireStatus(t, apiRequest(t, s, "agent", http.MethodPut, "pods/"+podName+"/status", map[string]any{"resourceVersion": pod.Metadata.ResourceVersion, "status": api.PodStatus{Phase: "Failed"}}), http.StatusOK)
	marker := childCreationKey(ownerUID)
	before, err := s.Client.Get(context.Background(), marker)
	if err != nil || len(before.Kvs) != 1 {
		t.Fatalf("read child marker: %v", err)
	}
	oldRevision := before.Kvs[0].ModRevision
	requireStatus(t, apiRequest(t, s, "controller", http.MethodDelete, "pods/"+podName+"?uid="+pod.Metadata.UID, nil), http.StatusOK)
	// 模拟基于删除前快照准备提交的事务，旧标记不能再通过比较。
	result, err := s.Client.Txn(context.Background()).If(clientv3.Compare(clientv3.ModRevision(marker), "=", oldRevision)).Then(clientv3.OpGet(marker)).Commit()
	if err != nil || result.Succeeded {
		t.Fatalf("stale replica snapshot unexpectedly remained valid: %v", err)
	}
}
