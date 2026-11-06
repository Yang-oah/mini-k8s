package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ageTerminalOperation 仅在显式隔离的 etcd 测试中模拟已过重试窗口。
func ageTerminalOperation(t *testing.T, s *Server, op replicaOperation) {
	t.Helper()
	current, rev, err := s.loadReplicaOperation(context.Background(), op.UID, op.OperationID)
	if err != nil || current == nil {
		t.Fatalf("load operation: %v", err)
	}
	current.CompletedAt = time.Now().Add(-operationRetention - time.Minute).UnixNano()
	// 同时模拟进程内单调观察窗口已过去，避免只改变墙钟绕过保留期。
	key := operationGCObservationKey(current)
	operationGCObserved.Store(key, time.Now().Add(-operationRetention-time.Minute))
	t.Cleanup(func() { operationGCObserved.Delete(key) })
	result, err := s.Client.Txn(context.Background()).If(clientv3.Compare(clientv3.ModRevision(operationKey(op.UID, op.OperationID)), "=", rev)).Then(clientv3.OpPut(operationKey(op.UID, op.OperationID), encodeOperation(*current))).Commit()
	if err != nil || !result.Succeeded {
		t.Fatalf("age operation: %v", err)
	}
}

func gcOperation(t *testing.T, s *Server, rsName string, op replicaOperation, token string) *httptest.ResponseRecorder {
	t.Helper()
	return apiRequest(t, s, token, http.MethodPost, "replicasets/"+rsName+"/create-operation-gc", map[string]any{"uid": op.UID, "operationID": op.OperationID})
}

func TestReplicaOperationGCProtectsRetryAndPod(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	revision := operationPodRevision(t, s)
	prepare := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": revision, "podName": "gc-pod-" + newUID()[:12]}
	first := apiRequest(t, s, "controller", http.MethodPost, path, prepare)
	requireStatus(t, first, http.StatusOK)
	op := decodeResponse[replicaOperation](t, first)
	requireStatus(t, gcOperation(t, s, rs.Metadata.Name, op, "controller"), http.StatusForbidden)
	requireStatus(t, gcOperation(t, s, rs.Metadata.Name, op, "admin"), http.StatusConflict) // PREPARED 不回收
	commit := apiRequest(t, s, "controller", http.MethodPut, path, map[string]any{"uid": op.UID, "operationID": op.OperationID, "podName": op.PodName})
	requireStatus(t, commit, http.StatusOK)
	op = decodeResponse[replicaOperation](t, commit)
	requireStatus(t, gcOperation(t, s, rs.Metadata.Name, op, "admin"), http.StatusConflict) // 重试窗口
	// 即使持久化时间看似已经过期，进程内的单调观察时间不足也不能提前回收。
	current, revisionAfterCommit, err := s.loadReplicaOperation(context.Background(), op.UID, op.OperationID)
	if err != nil || current == nil {
		t.Fatalf("load committed operation: %v", err)
	}
	current.CompletedAt = time.Now().Add(-operationRetention - time.Minute).UnixNano()
	aged, err := s.Client.Txn(context.Background()).If(clientv3.Compare(clientv3.ModRevision(operationKey(op.UID, op.OperationID)), "=", revisionAfterCommit)).Then(clientv3.OpPut(operationKey(op.UID, op.OperationID), encodeOperation(*current))).Commit()
	if err != nil || !aged.Succeeded {
		t.Fatalf("simulate old wall clock: %v", err)
	}
	requireStatus(t, gcOperation(t, s, rs.Metadata.Name, op, "admin"), http.StatusConflict)
	ageTerminalOperation(t, s, op)
	requireStatus(t, gcOperation(t, s, rs.Metadata.Name, op, "admin"), http.StatusConflict) // 关联 Pod 仍在
	requireStatus(t, apiRequest(t, s, "admin", http.MethodDelete, "pods/"+op.PodName+"?uid="+op.PodUID, nil), http.StatusOK)
	firstGC := gcOperation(t, s, rs.Metadata.Name, op, "admin")
	requireStatus(t, firstGC, http.StatusOK)
	if decodeResponse[map[string]any](t, firstGC)["complete"] != false {
		t.Fatal("first GC should only remove alias batch")
	}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, prepare), http.StatusGone)
	lastGC := gcOperation(t, s, rs.Metadata.Name, op, "admin")
	requireStatus(t, lastGC, http.StatusOK)
	if decodeResponse[map[string]any](t, lastGC)["complete"] != true {
		t.Fatal("GC did not finish")
	}
	requireStatus(t, gcOperation(t, s, rs.Metadata.Name, op, "admin"), http.StatusOK) // 重复 GC
	query := path + "?uid=" + op.UID + "&operationID=" + strconv.FormatInt(op.OperationID, 10)
	requireStatus(t, apiRequest(t, s, "controller", http.MethodGet, query, nil), http.StatusGone)
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, prepare), http.StatusGone)
	// 新读取版本可以创建下一操作，ID 不因 GC 回退。
	prepare["podListRevision"] = operationPodRevision(t, s)
	next := apiRequest(t, s, "controller", http.MethodPost, path, prepare)
	requireStatus(t, next, http.StatusOK)
	if decodeResponse[replicaOperation](t, next).OperationID <= op.OperationID {
		t.Fatal("operation ID was reused")
	}
}

func TestReplicaOperationGCBatchesAliases(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	revision := operationPodRevision(t, s)
	var op replicaOperation
	for i := 0; i < operationGCBatch+3; i++ {
		body := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": revision, "podName": "alias-" + strconv.Itoa(i) + "-" + rs.Metadata.UID[:8]}
		response := apiRequest(t, s, "controller", http.MethodPost, path, body)
		requireStatus(t, response, http.StatusOK)
		current := decodeResponse[replicaOperation](t, response)
		if i == 0 {
			op = current
		} else if current.OperationID != op.OperationID {
			t.Fatal("pending aliases produced a second operation")
		}
	}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodDelete, path, map[string]any{"uid": op.UID, "operationID": op.OperationID}), http.StatusOK)
	ageTerminalOperation(t, s, op)
	for attempt := 0; attempt < 4; attempt++ {
		response := gcOperation(t, s, rs.Metadata.Name, op, "admin")
		requireStatus(t, response, http.StatusOK)
		if decodeResponse[map[string]any](t, response)["complete"] == true {
			return
		}
	}
	t.Fatal("GC did not finish in bounded batches")
}

func TestReplicaOperationQuotaAllowsExistingReplay(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	prepare := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": operationPodRevision(t, s), "podName": "quota-" + newUID()[:12]}
	first := apiRequest(t, s, "controller", http.MethodPost, path, prepare)
	requireStatus(t, first, http.StatusOK)
	op := decodeResponse[replicaOperation](t, first)
	old, err := s.Client.Get(context.Background(), operationQuotaKey)
	if err != nil || len(old.Kvs) != 1 {
		t.Fatalf("read quota: %v", err)
	}
	t.Cleanup(func() { _, _ = s.Client.Put(context.Background(), operationQuotaKey, string(old.Kvs[0].Value)) })
	if _, err := s.Client.Put(context.Background(), operationQuotaKey, strconv.FormatInt(operationGlobalLimit, 10)); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, prepare), http.StatusOK)
	alias := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": prepare["podListRevision"], "podName": "other-" + newUID()[:12]}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, alias), http.StatusServiceUnavailable)
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPut, path, map[string]any{"uid": op.UID, "operationID": op.OperationID, "podName": op.PodName}), http.StatusOK)
}

func TestReplicaOperationLegacyQuotaRequiresMigration(t *testing.T) {
	s, rs, path := operationTestFixture(t, 1)
	ctx := context.Background()
	old, err := s.Client.Get(ctx, operationQuotaKey)
	if err != nil {
		t.Fatal(err)
	}
	legacyKey := prefix + "replica-operations/legacy-test-" + rs.Metadata.UID + "/sequence"
	// 仅在隔离 etcd 中模拟旧版本有记录但没有全局用量计数。
	if _, err = s.Client.Txn(ctx).Then(clientv3.OpDelete(operationQuotaKey), clientv3.OpPut(legacyKey, "1")).Commit(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = s.Client.Delete(ctx, legacyKey)
		if len(old.Kvs) == 0 {
			_, _ = s.Client.Delete(ctx, operationQuotaKey)
		} else {
			_, _ = s.Client.Put(ctx, operationQuotaKey, string(old.Kvs[0].Value))
		}
	}()
	prepare := map[string]any{"uid": rs.Metadata.UID, "resourceVersion": rs.Metadata.ResourceVersion, "podListRevision": operationPodRevision(t, s), "podName": "legacy-" + rs.Metadata.UID[:8]}
	requireStatus(t, apiRequest(t, s, "controller", http.MethodPost, path, prepare), http.StatusServiceUnavailable)
	if pending := apiRequest(t, s, "controller", http.MethodGet, path+"?uid="+rs.Metadata.UID, nil); pending.Code != http.StatusNotFound {
		t.Fatalf("legacy migration gate created an unexpected pending operation: %d", pending.Code)
	}
}
