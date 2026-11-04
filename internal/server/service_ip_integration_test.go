package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 必须指向本测试独占的空 etcd；本测试只删除自己创建并核对 UID 的键。
func TestServiceAddressClaimsAreAtomicAndRetained(t *testing.T) {
	endpoint := os.Getenv("G5_SERVICE_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("G5_SERVICE_ETCD_ENDPOINT must point to an isolated empty etcd")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	existing, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(existing.Kvs) != 0 {
		t.Fatal("G5 service test requires an isolated etcd without mini-k8s resources")
	}
	s := &Server{Client: client, ServiceCIDR: "10.244.250.0/30", Credentials: Credentials{Admin: "admin", Scheduler: "scheduler", Proxy: "proxy"}}
	suffix := time.Now().UnixNano()
	names := []string{fmt.Sprintf("g5-svc-a-%d", suffix), fmt.Sprintf("g5-svc-b-%d", suffix)}
	created := make([]api.Service, len(names))
	var createdNode api.Node
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, service := range created {
			if service.Metadata.UID == "" {
				continue
			}
			key := resourceKey("services", service.Metadata.Name)
			get, getErr := client.Get(cleanupCtx, key)
			if getErr == nil && len(get.Kvs) == 1 {
				object, decodeErr := decode(get.Kvs[0].Value, "services", get.Kvs[0].ModRevision)
				if decodeErr == nil && object.meta.UID == service.Metadata.UID {
					_, _ = client.Txn(cleanupCtx).If(clientv3.Compare(clientv3.ModRevision(key), "=", get.Kvs[0].ModRevision)).Then(clientv3.OpDelete(key)).Commit()
				}
			}
			claim := serviceClaimPrefix + service.Spec.ClusterIP
			_, _ = client.Txn(cleanupCtx).If(clientv3.Compare(clientv3.Value(claim), "=", service.Metadata.UID)).Then(clientv3.OpDelete(claim)).Commit()
		}
		if createdNode.Metadata.UID != "" {
			key := resourceKey("nodes", createdNode.Metadata.Name)
			get, getErr := client.Get(cleanupCtx, key)
			if getErr == nil && len(get.Kvs) == 1 {
				object, decodeErr := decode(get.Kvs[0].Value, "nodes", get.Kvs[0].ModRevision)
				if decodeErr == nil && object.meta.UID == createdNode.Metadata.UID {
					_, _ = client.Txn(cleanupCtx).If(clientv3.Compare(clientv3.ModRevision(key), "=", get.Kvs[0].ModRevision)).Then(clientv3.OpDelete(key)).Commit()
				}
			}
			_, _ = client.Txn(cleanupCtx).If(clientv3.Compare(clientv3.Value(nodeCIDRRevisionKey), "=", createdNode.Metadata.UID)).Then(clientv3.OpDelete(nodeCIDRRevisionKey)).Commit()
		}
		// 仅在独占测试 etcd 中，且已经没有任何地址声明时移除测试池。
		claims, getErr := client.Get(cleanupCtx, serviceClaimPrefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
		if getErr == nil && len(claims.Kvs) == 0 {
			_, _ = client.Txn(cleanupCtx).If(clientv3.Compare(clientv3.Value(serviceCIDRKey), "=", s.ServiceCIDR)).Then(clientv3.OpDelete(serviceCIDRKey)).Commit()
		}
	})
	var wg sync.WaitGroup
	codes := make([]int, len(names))
	for i := range names {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			obj := api.Service{APIVersion: api.Version, Kind: "Service", Metadata: api.Metadata{Name: names[i]}, Spec: api.ServiceSpec{Selector: map[string]string{"app": "x"}, Port: 80, TargetPort: 8080}}
			response := apiRequest(t, s, "admin", http.MethodPost, "services", obj)
			codes[i] = response.Code
			if response.Code == http.StatusCreated {
				created[i] = decodeResponse[api.Service](t, response)
			}
		}(i)
	}
	wg.Wait()
	for i, code := range codes {
		if code != http.StatusCreated {
			t.Fatalf("service %d: HTTP %d", i, code)
		}
	}
	if created[0].Spec.ClusterIP == "" || created[0].Spec.ClusterIP == created[1].Spec.ClusterIP {
		t.Fatalf("duplicate or missing addresses: %+v", created)
	}
	requireStatus(t, apiRequest(t, s, "proxy", http.MethodGet, "services", nil), http.StatusOK)
	changedIP := created[1]
	changedIP.Spec.ClusterIP = "10.244.250.99"
	requireStatus(t, apiRequest(t, s, "admin", http.MethodPut, "services/"+changedIP.Metadata.Name, changedIP), http.StatusForbidden)
	node := api.Node{APIVersion: api.Version, Kind: "Node", Metadata: api.Metadata{Name: fmt.Sprintf("g5-node-%d", suffix)}, Spec: api.NodeSpec{Allocatable: api.Resources{CPUMilli: 1000, MemoryBytes: 100000}, PodCIDR: "10.244.250.0/24"}}
	requireStatus(t, apiRequest(t, s, "admin", http.MethodPost, "nodes", node), http.StatusConflict)
	node.Spec.PodCIDR = "10.88.0.0/24"
	nodeResponse := apiRequest(t, s, "admin", http.MethodPost, "nodes", node)
	requireStatus(t, nodeResponse, http.StatusCreated)
	createdNode = decodeResponse[api.Node](t, nodeResponse)
	currentNode := decodeResponse[api.Node](t, apiRequest(t, s, "admin", http.MethodGet, "nodes/"+node.Metadata.Name, nil))
	currentNode.Spec.PodCIDR = "10.244.250.0/24"
	requireStatus(t, apiRequest(t, s, "admin", http.MethodPut, "nodes/"+node.Metadata.Name, currentNode), http.StatusConflict)
	response := apiRequest(t, s, "admin", http.MethodDelete, "services/"+created[0].Metadata.Name, nil)
	requireStatus(t, response, http.StatusOK)
	third := api.Service{APIVersion: api.Version, Kind: "Service", Metadata: api.Metadata{Name: fmt.Sprintf("g5-svc-c-%d", suffix)}, Spec: api.ServiceSpec{Selector: map[string]string{"app": "x"}, Port: 80, TargetPort: 8080}}
	requireStatus(t, apiRequest(t, s, "admin", http.MethodPost, "services", third), http.StatusConflict)
	status := apiRequest(t, s, "admin", http.MethodGet, "service-pool", nil)
	requireStatus(t, status, http.StatusOK)
	pool := decodeResponse[struct {
		Capacity  int `json:"capacity"`
		Available int `json:"available"`
		Claims    []struct {
			State string `json:"state"`
		} `json:"claims"`
	}](t, status)
	if pool.Capacity != 2 || pool.Available != 0 || len(pool.Claims) != 2 || pool.Claims[0].State == pool.Claims[1].State {
		t.Fatalf("incorrect active/retained pool state: %+v", pool)
	}
	requireStatus(t, apiRequest(t, s, "proxy", http.MethodGet, "service-pool", nil), http.StatusForbidden)
	requireStatus(t, apiRequest(t, s, "proxy", http.MethodPost, "services", third), http.StatusForbidden)
}
