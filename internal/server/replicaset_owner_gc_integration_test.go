package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
	"mini-k8s/internal/api"
)

func ownerGC(t *testing.T, s *Server, name, uid, token string) *httptest.ResponseRecorder {
	t.Helper()
	return apiRequest(t, s, token, http.MethodPost, "replicasets/"+name+"/create-operation-owner-gc", map[string]any{"uid": uid})
}

// collectOwnerOperation leaves only the three per-UID reserved metadata keys.
func collectOwnerOperation(t *testing.T, s *Server, rs api.ReplicaSet, path string) (replicaOperation, map[string]any) {
	t.Helper()
	body := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion,
		"podListRevision": operationPodRevision(t, s), "podName": "owner-gc-" + newUID()[:12]}
	first := apiRequest(t, s, "controller", http.MethodPost, path, body)
	requireStatus(t, first, http.StatusOK)
	op := decodeResponse[replicaOperation](t, first)
	requireStatus(t, apiRequest(t, s, "controller", http.MethodDelete, path, map[string]any{"uid": op.UID, "operationID": op.OperationID}), http.StatusOK)
	ageTerminalOperation(t, s, op)
	for i := 0; i < 3; i++ {
		response := gcOperation(t, s, rs.Metadata.Name, op, "admin")
		requireStatus(t, response, http.StatusOK)
		if decodeResponse[map[string]any](t, response)["complete"] == true {
			return op, body
		}
	}
	t.Fatal("operation GC did not finish")
	return op, body
}

func TestReplicaOperationOwnerGCRejectsLiveUIDAndPending(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	requireStatus(t, ownerGC(t, s, rs.Metadata.Name, rs.Metadata.UID, "controller"), http.StatusForbidden)
	// URL 名称不可信；即使传入不存在的名称，也不得回收仍被其他 RS 持有的 UID。
	requireStatus(t, ownerGC(t, s, "different-name", rs.Metadata.UID, "admin"), http.StatusConflict)
	prepare := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion,
		"podListRevision": operationPodRevision(t, s), "podName": "pending-" + newUID()[:12]}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, prepare), http.StatusOK)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+rs.Metadata.Name, nil), http.StatusOK)
	// RS 删除会取消意图，但保留重试记录；记录尚未回收前不能释放元数据。
	requireStatus(t, ownerGC(t, s, rs.Metadata.Name, rs.Metadata.UID, "admin"), http.StatusConflict)
}

func TestReplicaOperationOwnerGCProtectsRemainingPod(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	prepare := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion,
		"podListRevision": operationPodRevision(t, s), "podName": "remaining-" + newUID()[:12]}
	first := apiRequest(t, s, "controller", http.MethodPost, path, prepare)
	requireStatus(t, first, http.StatusOK)
	op := decodeResponse[replicaOperation](t, first)
	committed := apiRequest(t, s, "controller", http.MethodPut, path, map[string]any{"uid": op.UID, "operationID": op.OperationID, "podName": op.PodName})
	requireStatus(t, committed, http.StatusOK)
	// 仅在隔离 etcd 模拟异常遗留：RS 键丢失但旧 UID 的 Pod 仍在。
	if _, err := s.Client.Delete(context.Background(), resourceKey("replicasets", rs.Metadata.Name)); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, ownerGC(t, s, rs.Metadata.Name, rs.Metadata.UID, "admin"), http.StatusConflict)
}

func TestReplicaOperationOwnerGCConcurrentAndSameName(t *testing.T) {
	s, oldRS, path := operationTestFixture(t, 1)
	_, oldPrepare := collectOwnerOperation(t, s, oldRS, path)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+oldRS.Metadata.Name, nil), http.StatusOK)
	newResponse := apiRequest(t, s, "admin", http.MethodPost, "replicasets", testReplicaSet(oldRS.Metadata.Name))
	requireStatus(t, newResponse, http.StatusCreated)
	newRS := decodeResponse[api.ReplicaSet](t, newResponse)
	if newRS.Metadata.UID == oldRS.Metadata.UID {
		t.Fatal("same-name ReplicaSet reused the old UID")
	}
	// 测试目录独立；仍只删除本轮创建、UID 未变化的新对象。
	t.Cleanup(func() {
		current := apiRequest(t, s, "admin", http.MethodGet, "replicasets/"+newRS.Metadata.Name, nil)
		if current.Code == http.StatusOK && decodeResponse[api.ReplicaSet](t, current).Metadata.UID == newRS.Metadata.UID {
			apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+newRS.Metadata.Name+"?uid="+newRS.Metadata.UID, nil)
		}
	})
	before, err := s.Client.Get(context.Background(), operationQuotaKey)
	if err != nil || len(before.Kvs) != 1 {
		t.Fatalf("read quota: %v", err)
	}
	beforeCount, err := operationUsage(before.Kvs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	responses := make(chan *httptest.ResponseRecorder, 2)
	var group sync.WaitGroup
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			responses <- ownerGC(t, s, oldRS.Metadata.Name, oldRS.Metadata.UID, "admin")
		}()
	}
	group.Wait()
	close(responses)
	for response := range responses {
		if response.Code != http.StatusOK && response.Code != http.StatusConflict {
			t.Fatalf("unexpected concurrent owner GC: %d %s", response.Code, response.Body.String())
		}
	}
	requireStatus(t, ownerGC(t, s, oldRS.Metadata.Name, oldRS.Metadata.UID, "admin"), http.StatusOK)
	after, err := s.Client.Get(context.Background(), operationQuotaKey)
	if err != nil || len(after.Kvs) != 1 {
		t.Fatalf("read final quota: %v", err)
	}
	afterCount, err := operationUsage(after.Kvs[0].Value)
	if err != nil || afterCount != beforeCount-3 {
		t.Fatalf("quota was not released exactly once: before=%d after=%d err=%v", beforeCount, afterCount, err)
	}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, oldPrepare), http.StatusConflict)
	current := apiRequest(t, s, "admin", http.MethodGet, "replicasets/"+newRS.Metadata.Name, nil)
	requireStatus(t, current, http.StatusOK)
	if decodeResponse[api.ReplicaSet](t, current).Metadata.UID != newRS.Metadata.UID {
		t.Fatal("old UID collection changed the new ReplicaSet")
	}
}

func TestReplicaOperationOwnerGCRejectsUnaccountedKeys(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	_, _ = collectOwnerOperation(t, s, rs, path)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+rs.Metadata.Name, nil), http.StatusOK)
	unexpected := operationPrefix(rs.Metadata.UID) + "unknown"
	if _, err := s.Client.Put(context.Background(), unexpected, "not-counted"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.Client.Delete(context.Background(), unexpected, clientv3.WithPrefix()) })
	requireStatus(t, ownerGC(t, s, rs.Metadata.Name, rs.Metadata.UID, "admin"), http.StatusConflict)
}

func TestReplicaOperationOwnerGCPaginatesAndRejectsScanLimit(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	_, _ = collectOwnerOperation(t, s, rs, path)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+rs.Metadata.Name, nil), http.StatusOK)
	ctx := context.Background()
	const testLimit = 64
	keys := make([]string, 0, testLimit+1)
	ops := make([]clientv3.Op, 0, testLimit+1)
	for i := 0; i <= testLimit; i++ {
		name := "scan-" + newUID()[:12]
		pod := testOwnedPod(name, newUID())
		key := resourceKey("pods", name)
		keys = append(keys, key)
		ops = append(ops, clientv3.OpPut(key, encodeObject(object{value: &pod, meta: &pod.Metadata})))
	}
	t.Cleanup(func() {
		for _, key := range keys {
			_, _ = s.Client.Delete(ctx, key)
		}
	})
	insert, err := s.Client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		t.Fatal(err)
	}
	// 注入较小总上限，确认跨页面读取第65个对象后保守拒绝。
	if err := s.scanOperationOwnerReferences(ctx, rs.Metadata.UID, insert.Header.Revision, testLimit); !errors.Is(err, errOwnerGCScanLimit) {
		t.Fatalf("expected scan limit error, got %v", err)
	}
	before, err := s.Client.Get(ctx, operationPrefix(rs.Metadata.UID), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Kvs) != 3 {
		t.Fatalf("scan failure changed owner metadata: keys=%d", len(before.Kvs))
	}
	// 生产总上限为 2048，65 个对象必须能跨三页完成而不是永久阻塞。
	requireStatus(t, ownerGC(t, s, rs.Metadata.Name, rs.Metadata.UID, "admin"), http.StatusOK)
	after, err := s.Client.Get(ctx, operationPrefix(rs.Metadata.UID), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Kvs) != 0 {
		t.Fatalf("owner metadata remains after paginated collection: keys=%d", len(after.Kvs))
	}
}

func TestReplicaOperationOwnerGCScanSnapshotFailureIsReadOnly(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	_, _ = collectOwnerOperation(t, s, rs, path)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+rs.Metadata.Name, nil), http.StatusOK)
	ctx := context.Background()
	before, err := s.Client.Get(ctx, operationPrefix(rs.Metadata.UID), clientv3.WithPrefix())
	if err != nil || len(before.Kvs) != 3 {
		t.Fatalf("read owner metadata: %v", err)
	}
	quotaBefore, err := s.Client.Get(ctx, operationQuotaKey)
	if err != nil || len(quotaBefore.Kvs) != 1 {
		t.Fatalf("read quota: %v", err)
	}
	// 模拟分页过程中上下文被取消，不得将不完整扫描当作无引用。
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.scanOperationOwnerReferences(cancelled, rs.Metadata.UID, before.Header.Revision, operationOwnerGCScanLimit); err == nil {
		t.Fatal("cancelled scan unexpectedly succeeded")
	}
	// 压缩快照后的历史读取必须失败，不能用当前版本继续扫描。
	if _, err := s.Client.Compact(ctx, before.Header.Revision); err != nil {
		t.Fatalf("compact isolated etcd: %v", err)
	}
	if err := s.scanOperationOwnerReferences(ctx, rs.Metadata.UID, before.Header.Revision-1, operationOwnerGCScanLimit); err == nil {
		t.Fatal("compacted revision unexpectedly succeeded")
	}
	after, err := s.Client.Get(ctx, operationPrefix(rs.Metadata.UID), clientv3.WithPrefix())
	if err != nil || len(after.Kvs) != 3 {
		t.Fatalf("failed scan changed owner metadata: %v", err)
	}
	quotaAfter, err := s.Client.Get(ctx, operationQuotaKey)
	if err != nil || len(quotaAfter.Kvs) != 1 || string(quotaAfter.Kvs[0].Value) != string(quotaBefore.Kvs[0].Value) {
		t.Fatalf("failed scan changed quota: %v", err)
	}
}

func TestReplicaOperationOwnerGCScanUsesFixedRevision(t *testing.T) {
	s, rs, _ := operationTestFixture(t, 1)
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "replicasets/"+rs.Metadata.Name, nil), http.StatusOK)
	ctx := context.Background()
	pod := testOwnedPod("snapshot-"+newUID()[:12], rs.Metadata.UID)
	key := resourceKey("pods", pod.Metadata.Name)
	t.Cleanup(func() { _, _ = s.Client.Delete(ctx, key) })
	// 仅在隔离 etcd 创建并删除记录，检验历史快照是否仍看见旧 owner。
	created, err := s.Client.Put(ctx, key, encodeObject(object{value: &pod, meta: &pod.Metadata}))
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := s.Client.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.scanOperationOwnerReferences(ctx, rs.Metadata.UID, created.Header.Revision, operationOwnerGCScanLimit); !errors.Is(err, errOwnerGCReferenced) {
		t.Fatalf("historical snapshot lost the existing owner: %v", err)
	}
	if err := s.scanOperationOwnerReferences(ctx, rs.Metadata.UID, deleted.Header.Revision, operationOwnerGCScanLimit); err != nil {
		t.Fatalf("latest snapshot still reports deleted owner: %v", err)
	}
}
