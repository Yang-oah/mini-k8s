package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 仅连接显式指定的隔离测试 etcd；所有业务读写都经过正式 HTTP API。
func testServer(t *testing.T, node string) *Server {
	t.Helper()
	endpoint := os.Getenv("G2_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("G2_ETCD_ENDPOINT 未设置，跳过真实 etcd 集成测试")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &Server{Client: client, Credentials: Credentials{
		Admin: "admin", Controller: "controller", Scheduler: "scheduler", Agents: map[string]string{node: "agent"},
	}}
}

func apiRequest(t *testing.T, s *Server, token, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, "/api/mini/v1/"+path, bytes.NewReader(encoded))
	req.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	return response
}

func requireStatus(t *testing.T, response *httptest.ResponseRecorder, want int) {
	t.Helper()
	if response.Code != want {
		t.Fatalf("HTTP 状态为 %d，期望 %d：%s", response.Code, want, response.Body.String())
	}
}

func decodeResponse[T any](t *testing.T, response *httptest.ResponseRecorder) T {
	t.Helper()
	var result T
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func testPodSpec() api.PodSpec {
	return api.PodSpec{Image: "test:v1", RestartPolicy: "Always", Resources: api.ResourceRequirements{
		Requests: api.Resources{CPUMilli: 100, MemoryBytes: 64}, Limits: api.Resources{CPUMilli: 100, MemoryBytes: 64},
	}}
}

func testReplicaSet(name string) api.ReplicaSet {
	labels := map[string]string{"test-run": name}
	return api.ReplicaSet{APIVersion: api.Version, Kind: "ReplicaSet", Metadata: api.Metadata{Name: name}, Spec: api.ReplicaSetSpec{
		Replicas: 1, Selector: labels, Template: api.PodTemplate{Labels: labels, Spec: testPodSpec()},
	}}
}

func testOwnedPod(name, ownerUID string) api.Pod {
	return api.Pod{APIVersion: api.Version, Kind: "Pod", Metadata: api.Metadata{Name: name, OwnerUID: ownerUID}, Spec: testPodSpec()}
}

// 创建受 RS 管理的 Pod 必须经过持久化意图；保留旧测试的 201/Pod 响应形态。
func createOwnedOperation(t *testing.T, s *Server, rs api.ReplicaSet, podName string) *httptest.ResponseRecorder {
	t.Helper()
	list := apiRequest(t, s, "controller", http.MethodGet, "pods", nil)
	if list.Code != http.StatusOK {
		return list
	}
	var snapshot struct {
		ResourceVersion int64 `json:"resourceVersion"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	path := "replicasets/" + rs.Metadata.Name + "/create-operation"
	prepared := apiRequest(t, s, "controller", http.MethodPost, path, map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": snapshot.ResourceVersion, "podName": podName})
	if prepared.Code != http.StatusOK {
		return prepared
	}
	op := decodeResponse[replicaOperation](t, prepared)
	committed := apiRequest(t, s, "controller", http.MethodPut, path, map[string]any{"uid": op.UID, "operationID": op.OperationID, "podName": op.PodName})
	if committed.Code != http.StatusOK {
		return committed
	}
	result := apiRequest(t, s, "admin", http.MethodGet, "pods/"+op.PodName, nil)
	if result.Code == http.StatusOK {
		result.Code = http.StatusCreated
	}
	return result
}

// 故障场景也只尝试清理本次生成的资源名称；清理失败必须显式报告。
func cleanupResources(t *testing.T, s *Server, podName, rsName, nodeName, ownerUID, nodeUID string) {
	t.Helper()
	podPath, rsPath := "pods/"+podName, "replicasets/"+rsName
	podResp := apiRequest(t, s, "admin", "GET", podPath, nil)
	if podResp.Code == http.StatusOK {
		pod := decodeResponse[api.Pod](t, podResp)
		if ownerUID == "" || pod.Metadata.OwnerUID != ownerUID {
			t.Errorf("拒绝清理不属于本次测试 RS 的 Pod %s", podName)
			return
		}
		deleted := apiRequest(t, s, "admin", "DELETE", podPath, nil)
		if deleted.Code != http.StatusOK {
			t.Errorf("清理 Pod 失败: %s", deleted.Body.String())
		}
		if pod.NodeUID != "" {
			current := apiRequest(t, s, "admin", "GET", podPath, nil)
			if current.Code == http.StatusOK {
				pod = decodeResponse[api.Pod](t, current)
				cleaned := apiRequest(t, s, "agent", "POST", podPath+"/cleanup", map[string]any{"uid": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion})
				if cleaned.Code != http.StatusOK {
					t.Errorf("确认 Pod 清理失败: %s", cleaned.Body.String())
				}
			}
		}
	} else if podResp.Code != http.StatusNotFound {
		t.Errorf("读取待清理 Pod 失败: %s", podResp.Body.String())
	}
	rsResp := apiRequest(t, s, "admin", "GET", rsPath, nil)
	if rsResp.Code == http.StatusOK {
		if ownerUID == "" || decodeResponse[api.ReplicaSet](t, rsResp).Metadata.UID != ownerUID {
			t.Errorf("拒绝清理不属于本次测试的 RS %s", rsName)
			return
		}
		deleted := apiRequest(t, s, "admin", "DELETE", rsPath, nil)
		if deleted.Code != http.StatusOK {
			t.Errorf("清理 RS 失败: %s", deleted.Body.String())
		}
	} else if rsResp.Code != http.StatusNotFound {
		t.Errorf("读取待清理 RS 失败: %s", rsResp.Body.String())
	}
	if nodeName != "" {
		nodePath := "nodes/" + nodeName
		nodeResp := apiRequest(t, s, "admin", "GET", nodePath, nil)
		if nodeResp.Code == http.StatusOK {
			if nodeUID == "" || decodeResponse[api.Node](t, nodeResp).Metadata.UID != nodeUID {
				t.Errorf("拒绝清理不属于本次测试的 Node %s", nodeName)
				return
			}
			deleted := apiRequest(t, s, "admin", "DELETE", nodePath, nil)
			if deleted.Code != http.StatusOK {
				t.Errorf("清理 Node 失败: %s", deleted.Body.String())
			}
		} else if nodeResp.Code != http.StatusNotFound {
			t.Errorf("读取待清理 Node 失败: %s", nodeResp.Body.String())
		}
	}
}

func TestReplicaSetDeletionWaitsForBoundPod(t *testing.T) {
	suffix := strings.ToLower(newUID()[:12])
	rsName, podName, nodeName := "rs-"+suffix, "pod-"+suffix, "node-"+suffix
	s := testServer(t, nodeName)
	var ownerUID, nodeUID string
	t.Cleanup(func() {
		cleanupResources(t, s, "late-"+suffix, rsName, "", ownerUID, "")
		cleanupResources(t, s, podName, rsName, nodeName, ownerUID, nodeUID)
	})
	rsResponse := apiRequest(t, s, "admin", "POST", "replicasets", testReplicaSet(rsName))
	requireStatus(t, rsResponse, http.StatusCreated)
	rs := decodeResponse[api.ReplicaSet](t, rsResponse)
	ownerUID = rs.Metadata.UID
	podResponse := createOwnedOperation(t, s, rs, podName)
	requireStatus(t, podResponse, http.StatusCreated)
	pod := decodeResponse[api.Pod](t, podResponse)
	nodeResponse := apiRequest(t, s, "admin", "POST", "nodes", api.Node{APIVersion: api.Version, Kind: "Node", Metadata: api.Metadata{Name: nodeName}, Spec: api.NodeSpec{PodCIDR: "10.3.0.0/24", Allocatable: api.Resources{CPUMilli: 500, MemoryBytes: 512}}})
	requireStatus(t, nodeResponse, http.StatusCreated)
	node := decodeResponse[api.Node](t, nodeResponse)
	nodeUID = node.Metadata.UID
	readyResponse := apiRequest(t, s, "agent", "PUT", "nodes/"+nodeName+"/status", map[string]any{"resourceVersion": node.Metadata.ResourceVersion, "status": api.NodeStatus{Ready: true}})
	requireStatus(t, readyResponse, http.StatusOK)
	bind := apiRequest(t, s, "scheduler", "POST", "pods/"+podName+"/bind", map[string]any{"nodeName": nodeName, "nodeUID": node.Metadata.UID, "podUID": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion})
	requireStatus(t, bind, http.StatusOK)
	pod = decodeResponse[api.Pod](t, bind)
	unknown := apiRequest(t, s, "agent", "PUT", "pods/"+podName+"/status", map[string]any{"resourceVersion": pod.Metadata.ResourceVersion, "status": api.PodStatus{Phase: "Unknown"}})
	requireStatus(t, unknown, http.StatusOK)
	requireStatus(t, apiRequest(t, s, "admin", "DELETE", "replicasets/"+rsName, nil), http.StatusOK)
	rs = decodeResponse[api.ReplicaSet](t, apiRequest(t, s, "admin", "GET", "replicasets/"+rsName, nil))
	if rs.Metadata.DeletionTimestamp == nil {
		t.Fatal("仍有子 Pod 时 RS 未保留删除标记")
	}
	requireStatus(t, createOwnedOperation(t, s, rs, "late-"+suffix), http.StatusConflict)
	requireStatus(t, apiRequest(t, s, "controller", "DELETE", "pods/"+podName+"?uid="+pod.Metadata.UID, nil), http.StatusOK)
	finish := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion}
	requireStatus(t, apiRequest(t, s, "controller", "POST", "replicasets/"+rsName+"/finalize", finish), http.StatusConflict)
	deleting := decodeResponse[api.Pod](t, apiRequest(t, s, "admin", "GET", "pods/"+podName, nil))
	if deleting.Metadata.DeletionTimestamp == nil {
		t.Fatal("已绑定的 Pod 未保留删除标记")
	}
	requireStatus(t, apiRequest(t, s, "agent", "POST", "pods/"+podName+"/cleanup", map[string]any{"uid": deleting.Metadata.UID, "resourceVersion": deleting.Metadata.ResourceVersion}), http.StatusOK)
	requireStatus(t, apiRequest(t, s, "controller", "POST", "replicasets/"+rsName+"/finalize", finish), http.StatusOK)
}

func TestConcurrentChildCreationAndReplicaSetDeletion(t *testing.T) {
	s := testServer(t, "")
	for i := 0; i < 10; i++ {
		suffix := newUID()[:12]
		rsName, podName := "race-rs-"+suffix, "race-pod-"+suffix
		created := apiRequest(t, s, "admin", "POST", "replicasets", testReplicaSet(rsName))
		requireStatus(t, created, http.StatusCreated)
		rs := decodeResponse[api.ReplicaSet](t, created)
		// 每次迭代只清理自己的 UID，即使断言失败也会执行。
		t.Cleanup(func() { cleanupResources(t, s, podName, rsName, "", rs.Metadata.UID, "") })
		var createResponse, deleteResponse *httptest.ResponseRecorder
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			createResponse = createOwnedOperation(t, s, rs, podName)
		}()
		go func() {
			defer workers.Done()
			<-start
			deleteResponse = apiRequest(t, s, "admin", "DELETE", "replicasets/"+rsName, nil)
		}()
		close(start)
		workers.Wait()
		if createResponse.Code != http.StatusCreated && createResponse.Code != http.StatusConflict {
			t.Fatal(fmt.Sprintf("创建子 Pod 失败: %s", createResponse.Body.String()))
		}
		if deleteResponse.Code != http.StatusOK && deleteResponse.Code != http.StatusConflict {
			t.Fatal(fmt.Sprintf("删除 RS 失败: %s", deleteResponse.Body.String()))
		}
		pod := apiRequest(t, s, "admin", "GET", "pods/"+podName, nil)
		parent := apiRequest(t, s, "admin", "GET", "replicasets/"+rsName, nil)
		if pod.Code == http.StatusOK {
			if parent.Code != http.StatusOK || decodeResponse[api.ReplicaSet](t, parent).Metadata.UID != rs.Metadata.UID {
				t.Fatal("并发创建与删除产生了孤儿 Pod")
			}
		} else if pod.Code != http.StatusNotFound {
			t.Fatalf("读取 Pod 失败：%s", pod.Body.String())
		}
	}
}
