//go:build linux

package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const natSvc = "MK8S-SVC"
const natPost = "MK8S-POST"
const filterReject = "MK8S-REJECT"
const mangleGate = "MK8S-GATE"

// IPTables 只在持久化 owner 标识与既存规则完全相符时维护专用链。
type IPTables struct {
	owner      string
	lock       *os.File
	hostLock   *os.File
	lastRoutes []Route
	applied    bool
	runner     func(context.Context, string, []byte, ...string) ([]byte, error)
	lockTables func(context.Context) (func(), error)
	innerLock  string
	lastRules  string
}

// NewIPTables 锁定私有状态目录；不会在此阶段修改宿主机转发规则。
func NewIPTables(stateDir string) (*IPTables, error) {
	if !filepath.IsAbs(stateDir) {
		return nil, errors.New("proxy state directory must be absolute")
	}
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(stateDir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("proxy state directory must be private")
	}
	var stat unix.Stat_t
	if err := unix.Lstat(stateDir, &stat); err != nil || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("proxy state directory must be owned by the current user")
	}
	// 同一台机器的不同状态目录也必须互斥，防止检查链归属后被另一个 Proxy 替换。
	hostFD, err := unix.Open("/run/mini-k8s-proxy.lock", unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	hostLock := os.NewFile(uintptr(hostFD), "proxy host lock")
	var lockStat unix.Stat_t
	if err := unix.Fstat(hostFD, &lockStat); err != nil || lockStat.Mode&unix.S_IFMT != unix.S_IFREG || lockStat.Uid != uint32(os.Geteuid()) || lockStat.Mode&0077 != 0 {
		hostLock.Close()
		return nil, errors.New("invalid proxy host lock")
	}
	if err := unix.Flock(hostFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		hostLock.Close()
		return nil, errors.New("another proxy owns this host")
	}
	fd, err := unix.Open(filepath.Join(stateDir, "lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		hostLock.Close()
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "proxy lock")
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		hostLock.Close()
		return nil, errors.New("another proxy owns this state directory")
	}
	ownerPath := filepath.Join(stateDir, "owner")
	owner, err := readOwner(ownerPath)
	if errors.Is(err, os.ErrNotExist) {
		var id [16]byte
		if _, err = rand.Read(id[:]); err == nil {
			owner = "mini-k8s-" + hex.EncodeToString(id[:])
			var f *os.File
			f, err = os.OpenFile(ownerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err == nil {
				_, err = io.WriteString(f, owner+"\n")
				if err == nil {
					err = f.Sync()
				}
				closeErr := f.Close()
				if err == nil {
					err = closeErr
				}
			}
		}
	}
	if err != nil {
		_ = unix.Flock(fd, unix.LOCK_UN)
		lock.Close()
		hostLock.Close()
		return nil, fmt.Errorf("read or create proxy owner: %w", err)
	}
	innerLock := filepath.Join(stateDir, "xtables-child.lock")
	innerFD, err := unix.Open(innerLock, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		_ = unix.Flock(fd, unix.LOCK_UN)
		lock.Close()
		hostLock.Close()
		return nil, err
	}
	var innerStat unix.Stat_t
	if err := unix.Fstat(innerFD, &innerStat); err != nil || innerStat.Mode&unix.S_IFMT != unix.S_IFREG || innerStat.Uid != uint32(os.Geteuid()) || innerStat.Mode&0077 != 0 {
		unix.Close(innerFD)
		_ = unix.Flock(fd, unix.LOCK_UN)
		lock.Close()
		hostLock.Close()
		return nil, errors.New("invalid proxy child lock")
	}
	unix.Close(innerFD)
	return &IPTables{owner: owner, lock: lock, hostLock: hostLock, innerLock: innerLock}, nil
}

func readOwner(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || stat.Uid != uint32(os.Geteuid()) {
		return "", errors.New("invalid proxy owner file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(data))
	if len(value) != len("mini-k8s-")+32 || !strings.HasPrefix(value, "mini-k8s-") {
		return "", errors.New("invalid proxy owner marker")
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(value, "mini-k8s-")); err != nil {
		return "", errors.New("invalid proxy owner marker")
	}
	return value, nil
}

// Close 仅释放锁，控制面中断或进程退出不会主动删除转发规则。
func (i *IPTables) Close() error {
	if i.lock == nil {
		return nil
	}
	err := unix.Flock(int(i.lock.Fd()), unix.LOCK_UN)
	closeErr := i.lock.Close()
	i.lock = nil
	if i.hostLock != nil {
		_ = unix.Flock(int(i.hostLock.Fd()), unix.LOCK_UN)
		_ = i.hostLock.Close()
		i.hostLock = nil
	}
	if err != nil {
		return err
	}
	return closeErr
}

type savedTable struct {
	exists map[string]bool
	hooks  map[string]bool
}

// inspectTable 拒绝接管同名未知链、未标记规则和未标记引用。
func inspectTable(data, owner string, chains []string, parents map[string]string) (savedTable, error) {
	result := savedTable{exists: make(map[string]bool), hooks: make(map[string]bool)}
	sentinels := make(map[string]bool)
	set := make(map[string]bool)
	for _, name := range chains {
		set[name] = true
	}
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.HasPrefix(fields[0], ":") {
			name := strings.TrimPrefix(fields[0], ":")
			if set[name] {
				result.exists[name] = true
			}
		}
		if len(fields) < 3 || fields[0] != "-A" {
			continue
		}
		source := fields[1]
		target := ""
		jumpType := ""
		for pos := 2; pos < len(fields); pos++ {
			if fields[pos] == "-j" || fields[pos] == "--jump" || fields[pos] == "-g" || fields[pos] == "--goto" {
				if pos+1 >= len(fields) {
					return result, errors.New("invalid chain reference")
				}
				jumpType = fields[pos]
				target = fields[pos+1]
				break
			}
			if strings.HasPrefix(fields[pos], "--jump=") || strings.HasPrefix(fields[pos], "--goto=") {
				parts := strings.SplitN(fields[pos], "=", 2)
				jumpType, target = parts[0], parts[1]
				break
			}
		}
		if set[source] || set[target] {
			if !hasOwnerComment(fields, owner) {
				return result, errors.New("unowned rule or reference uses proxy chain")
			}
			if set[source] && target == "RETURN" && isSimpleOwnedJump(fields, owner, "RETURN") {
				sentinels[source] = true
			}
			if set[target] {
				if (jumpType != "-j" && jumpType != "--jump") || !isSimpleOwnedJump(fields, owner, target) || parents[source] != target || result.hooks[source] {
					return result, errors.New("unexpected or duplicate proxy chain reference")
				}
				result.hooks[source] = true
			}
		}
	}
	for _, chain := range chains {
		if result.exists[chain] && !sentinels[chain] {
			return result, errors.New("existing proxy chain has no ownership marker")
		}
	}
	return result, nil
}

func isSimpleOwnedJump(fields []string, owner, target string) bool {
	return len(fields) == 8 && fields[0] == "-A" && fields[2] == "-m" && fields[3] == "comment" && fields[4] == "--comment" && strings.Trim(fields[5], `"`) == owner && (fields[6] == "-j" || fields[6] == "--jump") && fields[7] == target
}

func hasOwnerComment(fields []string, owner string) bool {
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == "--comment" && strings.Trim(fields[i+1], `"`) == owner {
			return true
		}
	}
	return false
}

func command(ctx context.Context, privateLock, program string, input []byte, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, program, args...)
	// 父进程已持有系统 xtables 锁；仅子进程使用私有锁，避免重复加锁死等。
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "XTABLES_LOCKFILE=") {
			cmd.Env = append(cmd.Env, variable)
		}
	}
	cmd.Env = append(cmd.Env, "XTABLES_LOCKFILE="+privateLock)
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w", program, err)
	}
	return out, nil
}

func (i *IPTables) run(ctx context.Context, program string, input []byte, args ...string) ([]byte, error) {
	if i.runner != nil {
		return i.runner(ctx, program, input, args...)
	}
	return command(ctx, i.innerLock, program, input, args...)
}

// 持有 iptables 默认锁贯穿规则读取与写入；直接操作 nft/netlink 的工具不受此锁约束。
func acquireTableLock(parent context.Context) (func(), error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	fd, err := unix.Open("/run/xtables.lock", unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "xtables lock")
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG {
		file.Close()
		return nil, errors.New("invalid system xtables lock")
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() { _ = unix.Flock(fd, unix.LOCK_UN); _ = file.Close() }, nil
		}
		if err != unix.EAGAIN && err != unix.EWOULDBLOCK {
			file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			file.Close()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (i *IPTables) Apply(ctx context.Context, routes []Route) error {
	if i.lock == nil {
		return errors.New("proxy rule lock is closed")
	}
	// 整轮都受限，避免长时间占用共享 xtables 锁阻塞其他网络组件。
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lock := i.lockTables
	if lock == nil {
		lock = acquireTableLock
	}
	release, err := lock(ctx)
	if err != nil {
		return fmt.Errorf("cannot lock xtables: %w", err)
	}
	defer release()
	// 再次校验所有进入 restore 的值，禁止通过规则文本注入。
	for _, r := range routes {
		ip, err := netip.ParseAddr(r.IP)
		if err != nil || !ip.Is4() || r.Port < 1 || r.Port > 65535 {
			return errors.New("invalid service route")
		}
		for _, e := range r.Endpoints {
			ip, err := netip.ParseAddr(e.IP)
			if err != nil || !ip.Is4() || e.Port < 1 || e.Port > 65535 {
				return errors.New("invalid endpoint")
			}
		}
	}
	nat, err := i.run(ctx, "iptables-save", nil, "-t", "nat")
	if err != nil {
		return err
	}
	filter, err := i.run(ctx, "iptables-save", nil, "-t", "filter")
	if err != nil {
		return err
	}
	mangle, err := i.run(ctx, "iptables-save", nil, "-t", "mangle")
	if err != nil {
		return err
	}
	natState, err := inspectTable(string(nat), i.owner, []string{natSvc, natPost}, map[string]string{"PREROUTING": natSvc, "OUTPUT": natSvc, "POSTROUTING": natPost})
	if err != nil {
		return err
	}
	filterState, err := inspectTable(string(filter), i.owner, []string{filterReject}, map[string]string{"FORWARD": filterReject, "OUTPUT": filterReject})
	if err != nil {
		return err
	}
	mangleState, err := inspectTable(string(mangle), i.owner, []string{mangleGate}, map[string]string{"PREROUTING": mangleGate, "OUTPUT": mangleGate})
	if err != nil {
		return err
	}
	// 同一快照不重复关闸，避免正常轮询引入周期性连接中断。
	currentRules := ownedRules(string(nat), string(filter), string(mangle))
	if i.applied && reflect.DeepEqual(i.lastRoutes, routes) && i.lastRules == currentRules && completeHooks(natState, filterState, mangleState) && len(gateAddresses(string(mangle))) == 0 {
		return nil
	}
	addresses := make(map[string]bool)
	for _, r := range routes {
		addresses[r.IP] = true
	}
	// 保留中断前已安装的旧目的地址及上次未能打开的闸门；不依赖内存状态。
	for _, address := range append(append(ruleAddresses(string(nat), natSvc), ruleAddresses(string(filter), filterReject)...), gateAddresses(string(mangle))...) {
		addresses[address] = true
	}
	blocked := make([]string, 0, len(addresses))
	for address := range addresses {
		blocked = append(blocked, address)
	}
	sort.Strings(blocked)
	// 闸门位于 DNAT 之前。中途任一步失败时保留闸门，避免继续流向旧端点。
	if _, err = i.run(ctx, "iptables-restore", []byte(renderGate(i.owner, blocked, mangleState)), "--noflush", "--wait", "2"); err != nil {
		return fmt.Errorf("cannot close service gate: %w", err)
	}
	mangleState.hooks["PREROUTING"], mangleState.hooks["OUTPUT"] = true, true
	filterRules := renderFilter(i.owner, routes, filterState)
	natRules := renderNAT(i.owner, routes, natState)
	// --noflush 仅重建显式声明的专用链，不清空已有表或宿主机其他链。
	if _, err = i.run(ctx, "iptables-restore", []byte(filterRules), "--noflush", "--wait", "2"); err != nil {
		return err
	}
	if _, err = i.run(ctx, "iptables-restore", []byte(natRules), "--noflush", "--wait", "2"); err != nil {
		return err
	}
	if _, err = i.run(ctx, "iptables-restore", []byte(renderGate(i.owner, nil, mangleState)), "--noflush", "--wait", "2"); err != nil {
		return fmt.Errorf("cannot reopen service gate: %w", err)
	}
	// 记录内核实际规则（而不是预期文本），下一轮检测外部丢失或意外篡改。
	nat, err = i.run(ctx, "iptables-save", nil, "-t", "nat")
	if err != nil {
		i.applied = false
		return err
	}
	filter, err = i.run(ctx, "iptables-save", nil, "-t", "filter")
	if err != nil {
		i.applied = false
		return err
	}
	mangle, err = i.run(ctx, "iptables-save", nil, "-t", "mangle")
	if err != nil {
		i.applied = false
		return err
	}
	i.lastRules = ownedRules(string(nat), string(filter), string(mangle))
	i.lastRoutes = cloneRoutes(routes)
	i.applied = true
	return nil
}

// 只比较专用链及到这些链的挂钩，避免宿主机无关规则改变造成中断。
func ownedRules(tables ...string) string {
	var b strings.Builder
	for _, table := range tables {
		for _, line := range strings.Split(table, "\n") {
			fields := strings.Fields(line)
			if len(fields) < 3 || fields[0] != "-A" {
				continue
			}
			if fields[1] == natSvc || fields[1] == natPost || fields[1] == filterReject || fields[1] == mangleGate || strings.Contains(line, "-j "+natSvc) || strings.Contains(line, "-j "+natPost) || strings.Contains(line, "-j "+filterReject) || strings.Contains(line, "-j "+mangleGate) {
				b.WriteString(line)
				b.WriteByte('\n')
			}
		}
		b.WriteByte('|')
	}
	return b.String()
}

func completeHooks(natState, filterState, mangleState savedTable) bool {
	return natState.exists[natSvc] && natState.exists[natPost] && natState.hooks["PREROUTING"] && natState.hooks["OUTPUT"] && natState.hooks["POSTROUTING"] &&
		filterState.exists[filterReject] && filterState.hooks["FORWARD"] && filterState.hooks["OUTPUT"] &&
		mangleState.exists[mangleGate] && mangleState.hooks["PREROUTING"] && mangleState.hooks["OUTPUT"]
}

func cloneRoutes(routes []Route) []Route {
	result := make([]Route, len(routes))
	for i, route := range routes {
		result[i] = route
		result[i].Endpoints = append([]Endpoint(nil), route.Endpoints...)
	}
	return result
}

// 仅提取已验证的专用链中的 /32 目的地址，用于进程重启后的保守关闸。
func ruleAddresses(table, chain string) []string {
	var addresses []string
	for _, line := range strings.Split(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[0] != "-A" || fields[1] != chain {
			continue
		}
		for i := 2; i+1 < len(fields); i++ {
			if fields[i] != "-d" && fields[i] != "--destination" {
				continue
			}
			prefix, err := netip.ParsePrefix(fields[i+1])
			if err == nil && prefix.Addr().Is4() && prefix.Bits() == 32 {
				addresses = append(addresses, prefix.Addr().String())
			}
		}
	}
	return addresses
}

func gateAddresses(table string) []string { return ruleAddresses(table, mangleGate) }

func renderGate(owner string, addresses []string, state savedTable) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*mangle\n:%s - [0:0]\n", mangleGate)
	for _, parent := range []string{"PREROUTING", "OUTPUT"} {
		if !state.hooks[parent] {
			fmt.Fprintf(&b, "-I %s 1 %s -j %s\n", parent, marker(owner), mangleGate)
		}
	}
	for _, ip := range addresses {
		fmt.Fprintf(&b, "-A %s -d %s/32 -p tcp %s -j DROP\n", mangleGate, ip, marker(owner))
	}
	fmt.Fprintf(&b, "-A %s %s -j RETURN\nCOMMIT\n", mangleGate, marker(owner))
	return b.String()
}

func marker(owner string) string { return "-m comment --comment " + owner }

func renderFilter(owner string, routes []Route, state savedTable) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*filter\n:%s - [0:0]\n", filterReject)
	for _, parent := range []string{"FORWARD", "OUTPUT"} {
		if !state.hooks[parent] {
			fmt.Fprintf(&b, "-I %s 1 %s -j %s\n", parent, marker(owner), filterReject)
		}
	}
	for _, r := range routes {
		if len(r.Endpoints) == 0 {
			fmt.Fprintf(&b, "-A %s -d %s/32 -p tcp --dport %d %s -j REJECT --reject-with tcp-reset\n", filterReject, r.IP, r.Port, marker(owner))
		}
	}
	fmt.Fprintf(&b, "-A %s %s -j RETURN\nCOMMIT\n", filterReject, marker(owner))
	return b.String()
}

func renderNAT(owner string, routes []Route, state savedTable) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*nat\n:%s - [0:0]\n:%s - [0:0]\n", natSvc, natPost)
	for _, pair := range [][2]string{{"PREROUTING", natSvc}, {"OUTPUT", natSvc}, {"POSTROUTING", natPost}} {
		if !state.hooks[pair[0]] {
			fmt.Fprintf(&b, "-I %s 1 %s -j %s\n", pair[0], marker(owner), pair[1])
		}
	}
	for _, r := range routes {
		for index, e := range r.Endpoints {
			fmt.Fprintf(&b, "-A %s -d %s/32 -p tcp --dport %d %s", natSvc, r.IP, r.Port, marker(owner))
			if remain := len(r.Endpoints) - index; remain > 1 {
				fmt.Fprintf(&b, " -m statistic --mode random --probability %s", strconv.FormatFloat(1/float64(remain), 'f', 10, 64))
			}
			fmt.Fprintf(&b, " -j DNAT --to-destination %s:%d\n", e.IP, e.Port)
		}
		if len(r.Endpoints) > 0 {
			// 仅为原目的地是本 Service 的连接做回程 SNAT。
			fmt.Fprintf(&b, "-A %s -p tcp -m conntrack --ctdir ORIGINAL --ctorigdst %s/32 --ctorigdstport %d %s -j MASQUERADE\n", natPost, r.IP, r.Port, marker(owner))
		}
	}
	fmt.Fprintf(&b, "-A %s %s -j RETURN\n-A %s %s -j RETURN\nCOMMIT\n", natSvc, marker(owner), natPost, marker(owner))
	return b.String()
}
