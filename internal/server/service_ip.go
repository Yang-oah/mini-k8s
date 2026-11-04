package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"sort"
	"strings"
	"time"

	"mini-k8s/internal/api"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	serviceCIDRKey      = prefix + "service-address-pool"
	serviceClaimPrefix  = prefix + "service-address-claims/"
	nodeCIDRRevisionKey = prefix + "node-address-revision"
)

var (
	errServicePoolUnavailable = errors.New("service address pool is not configured")
	errServicePoolConflict    = errors.New("service address pool differs from stored configuration")
	errServicePoolExhausted   = errors.New("service address pool is exhausted")
	errServiceCIDROverlap     = errors.New("service and pod address pools overlap")
	errInvalidNodeNetwork     = errors.New("node PodCIDR must be a canonical IPv4 network when a service pool exists")
	errServiceConflict        = errors.New("service already exists or address allocation conflicted")
	// 这些网络不能作为普通 ClusterIP 目的地址使用；不限制 RFC1918 私网。
	unusableServiceNetworks = []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),
		netip.MustParsePrefix("127.0.0.0/8"),
		netip.MustParsePrefix("169.254.0.0/16"),
		netip.MustParsePrefix("224.0.0.0/4"),
		netip.MustParsePrefix("240.0.0.0/4"),
	}
)

// ValidateServiceCIDR 限制池大小以保证逐地址分配有界；空值表示未启用 Service。
// Service 网段与宿主机路由的隔离仍须部署时独立检查。
func ValidateServiceCIDR(value string) error {
	if value == "" {
		return nil
	}
	p, err := netip.ParsePrefix(value)
	if err != nil || !p.Addr().Is4() || p != p.Masked() || p.Bits() < 16 || p.Bits() > 30 {
		return errors.New("service CIDR must be a canonical IPv4 network between /16 and /30")
	}
	for _, reserved := range unusableServiceNetworks {
		if p.Overlaps(reserved) {
			return fmt.Errorf("service CIDR overlaps unusable network %s", reserved)
		}
	}
	return nil
}

func servicePool(value string) (netip.Prefix, error) {
	if value == "" {
		return netip.Prefix{}, errServicePoolUnavailable
	}
	if err := ValidateServiceCIDR(value); err != nil {
		return netip.Prefix{}, err
	}
	return netip.ParsePrefix(value)
}

func checkPodNetwork(pool netip.Prefix, cidr string) error {
	pod, err := netip.ParsePrefix(cidr)
	if err != nil || !pod.Addr().Is4() || pod != pod.Masked() {
		return errInvalidNodeNetwork
	}
	if pool.Overlaps(pod) {
		return errServiceCIDROverlap
	}
	return nil
}

func respondNodeCIDRError(err error, respond errorResponder) {
	if errors.Is(err, errServiceCIDROverlap) || errors.Is(err, errInvalidNodeNetwork) || errors.Is(err, errServicePoolConflict) {
		respond(409, "Conflict", err.Error())
		return
	}
	respond(503, "Unavailable", "cannot validate node address pool")
}

// 节点写入必须比较返回的池版本：并发 Service 创建改变池时，旧检查不能提交。
func (s *Server) checkNodeServiceCIDR(ctx context.Context, cidr string) (int64, error) {
	result, err := s.Client.Get(ctx, serviceCIDRKey)
	if err != nil {
		return 0, err
	}
	value := s.ServiceCIDR
	var revision int64
	if len(result.Kvs) > 0 {
		stored := string(result.Kvs[0].Value)
		revision = result.Kvs[0].ModRevision
		if value != "" && value != stored {
			return 0, errServicePoolConflict
		}
		value = stored
	}
	if value == "" {
		return revision, nil
	}
	pool, err := servicePool(value)
	if err != nil {
		return 0, err
	}
	return revision, checkPodNetwork(pool, cidr)
}

// createService 同一事务写入对象和永久地址声明。删除不会复用地址，
// 因为离线 Proxy 上的旧规则可能仍指向被删除 Service 的旧端点。
func (s *Server) createService(ctx context.Context, key string, o object) (*clientv3.TxnResponse, error) {
	pool, err := servicePool(s.ServiceCIDR)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 32; attempt++ {
		snapshot, err := s.Client.Txn(ctx).Then(
			clientv3.OpGet(resourceKey("nodes", ""), clientv3.WithPrefix()),
			clientv3.OpGet(nodeCIDRRevisionKey), clientv3.OpGet(serviceCIDRKey),
			clientv3.OpGet(serviceClaimPrefix, clientv3.WithPrefix()), clientv3.OpGet(key),
		).Commit()
		if err != nil {
			return nil, err
		}
		if len(snapshot.Responses[4].GetResponseRange().Kvs) != 0 {
			return nil, errServiceConflict
		}
		for _, kv := range snapshot.Responses[0].GetResponseRange().Kvs {
			node, err := decode(kv.Value, "nodes", kv.ModRevision)
			if err != nil {
				return nil, fmt.Errorf("stored node is invalid: %w", err)
			}
			if err := checkPodNetwork(pool, node.value.(*api.Node).Spec.PodCIDR); err != nil {
				return nil, err
			}
		}
		var markerRev, poolRev int64
		if kvs := snapshot.Responses[1].GetResponseRange().Kvs; len(kvs) > 0 {
			markerRev = kvs[0].ModRevision
		}
		if kvs := snapshot.Responses[2].GetResponseRange().Kvs; len(kvs) > 0 {
			poolRev = kvs[0].ModRevision
			if string(kvs[0].Value) != pool.String() {
				return nil, errServicePoolConflict
			}
		}
		claims := make(map[string]struct{})
		for _, kv := range snapshot.Responses[3].GetResponseRange().Kvs {
			claims[string(kv.Key)] = struct{}{}
		}
		var ip netip.Addr
		for candidate := pool.Addr().Next(); candidate.IsValid() && pool.Contains(candidate) && pool.Contains(candidate.Next()); candidate = candidate.Next() {
			if _, exists := claims[serviceClaimPrefix+candidate.String()]; !exists {
				ip = candidate
				break
			}
		}
		if !ip.IsValid() {
			return nil, errServicePoolExhausted
		}
		o.value.(*api.Service).Spec.ClusterIP = ip.String()
		claimKey := serviceClaimPrefix + ip.String()
		writes := []clientv3.Op{clientv3.OpPut(key, encodeObject(o)), clientv3.OpPut(claimKey, o.meta.UID)}
		if poolRev == 0 {
			writes = append(writes, clientv3.OpPut(serviceCIDRKey, pool.String()))
		}
		result, err := s.Client.Txn(ctx).If(
			clientv3.Compare(clientv3.Version(key), "=", 0),
			clientv3.Compare(clientv3.ModRevision(nodeCIDRRevisionKey), "=", markerRev),
			clientv3.Compare(clientv3.ModRevision(serviceCIDRKey), "=", poolRev),
			clientv3.Compare(clientv3.Version(claimKey), "=", 0),
		).Then(writes...).Commit()
		if err != nil || result.Succeeded {
			return result, err
		}
	}
	return nil, errServiceConflict
}

// servicePoolStatus 只读展示地址声明，其中 retained 表示不会自动复用的旧地址。
func (s *Server) servicePoolStatus(w http.ResponseWriter, r *http.Request, respond errorResponder) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	snapshot, err := s.Client.Txn(ctx).Then(clientv3.OpGet(serviceCIDRKey), clientv3.OpGet(serviceClaimPrefix, clientv3.WithPrefix()), clientv3.OpGet(resourceKey("services", ""), clientv3.WithPrefix())).Commit()
	if err != nil {
		respond(503, "Unavailable", "cannot inspect service address pool")
		return
	}
	var cidr string
	if kvs := snapshot.Responses[0].GetResponseRange().Kvs; len(kvs) > 0 {
		cidr = string(kvs[0].Value)
	}
	claims := snapshot.Responses[1].GetResponseRange().Kvs
	var capacity int
	effectiveCIDR := cidr
	if effectiveCIDR == "" {
		effectiveCIDR = s.ServiceCIDR
	}
	if effectiveCIDR != "" {
		pool, err := servicePool(effectiveCIDR)
		if err != nil {
			respond(503, "Unavailable", "stored service address pool is invalid")
			return
		}
		capacity = (1 << (32 - pool.Bits())) - 2
	}
	if cidr == "" && len(claims) != 0 {
		respond(503, "Unavailable", "address claims exist without a configured pool")
		return
	}
	active := make(map[string]string)
	for _, kv := range snapshot.Responses[2].GetResponseRange().Kvs {
		o, err := decode(kv.Value, "services", kv.ModRevision)
		if err != nil {
			respond(503, "Unavailable", "stored service is invalid")
			return
		}
		svc := o.value.(*api.Service)
		active[svc.Spec.ClusterIP] = svc.Metadata.UID
	}
	type claim struct {
		IP    string `json:"ip"`
		UID   string `json:"uid"`
		State string `json:"state"`
	}
	entries := make([]claim, 0, len(claims))
	for _, kv := range claims {
		ip := strings.TrimPrefix(string(kv.Key), serviceClaimPrefix)
		uid := string(kv.Value)
		state := "retained"
		if current := active[ip]; current != "" && current != uid {
			respond(503, "Unavailable", "service address claim does not match service UID")
			return
		}
		if active[ip] == uid && uid != "" {
			state = "active"
		}
		entries = append(entries, claim{IP: ip, UID: uid, State: state})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].IP < entries[j].IP })
	writeJSON(w, map[string]any{"cidr": cidr, "configuredCIDR": s.ServiceCIDR, "initialized": cidr != "", "capacity": capacity, "available": capacity - len(entries), "claims": entries})
}
