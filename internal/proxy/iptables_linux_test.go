//go:build linux

package proxy

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestTableOwnershipRejectsUnmarkedChain(t *testing.T) {
	const owner = "mini-k8s-0123456789abcdef0123456789abcdef"
	chains := []string{natSvc, natPost}
	parents := map[string]string{"PREROUTING": natSvc, "OUTPUT": natSvc, "POSTROUTING": natPost}
	foreign := "*nat\n:MK8S-SVC - [0:0]\n-A MK8S-SVC -j RETURN\nCOMMIT\n"
	if _, err := inspectTable(foreign, owner, chains, parents); err == nil {
		t.Fatal("accepted chain without owner marker")
	}
	spoof := "*nat\n:MK8S-SVC - [0:0]\n-A MK8S-SVC -m comment --comment other-" + owner + " -j RETURN\nCOMMIT\n"
	if _, err := inspectTable(spoof, owner, chains, parents); err == nil {
		t.Fatal("accepted owner embedded in another comment")
	}
	foreignGoto := "*nat\n:MK8S-SVC - [0:0]\n-A OTHER --goto=MK8S-SVC\nCOMMIT\n"
	if _, err := inspectTable(foreignGoto, owner, chains, parents); err == nil {
		t.Fatal("accepted unknown goto reference")
	}
	partial := "*nat\n:MK8S-SVC - [0:0]\n-A MK8S-SVC -m comment --comment " + owner + " -j RETURN\n-A PREROUTING -s 10.0.0.0/8 -m comment --comment " + owner + " -j MK8S-SVC\nCOMMIT\n"
	if _, err := inspectTable(partial, owner, chains, parents); err == nil {
		t.Fatal("accepted a restricted hook as the full proxy hook")
	}
	legit := "*nat\n:MK8S-SVC - [0:0]\n-A MK8S-SVC -m comment --comment \"" + owner + "\" -j RETURN\n-A PREROUTING -m comment --comment \"" + owner + "\" -j MK8S-SVC\nCOMMIT\n"
	got, err := inspectTable(legit, owner, chains, parents)
	if err != nil || !got.exists[natSvc] || !got.hooks["PREROUTING"] {
		t.Fatalf("legitimate ownership was rejected: %#v %v", got, err)
	}
}

func TestApplyKeepsGateClosedWhenRuleUpdateFails(t *testing.T) {
	owner := "mini-k8s-0123456789abcdef0123456789abcdef"
	var restores []string
	i := &IPTables{owner: owner, lock: &os.File{}}
	i.lockTables = func(context.Context) (func(), error) { return func() {}, nil }
	i.runner = func(_ context.Context, program string, input []byte, _ ...string) ([]byte, error) {
		if program == "iptables-save" {
			return []byte(""), nil
		}
		restores = append(restores, string(input))
		if strings.HasPrefix(string(input), "*filter") {
			return nil, errors.New("injected failure")
		}
		return nil, nil
	}
	err := i.Apply(context.Background(), []Route{{IP: "10.96.0.1", Port: 80}})
	if err == nil || len(restores) != 2 {
		t.Fatalf("expected gate then failed filter only: %v %#v", err, restores)
	}
	if !strings.HasPrefix(restores[0], "*mangle") || !strings.Contains(restores[0], "10.96.0.1/32") || !strings.Contains(restores[0], "-j DROP") {
		t.Fatal("service gate was not closed before filter update")
	}
	if i.applied {
		t.Fatal("failed update must not be recorded as applied")
	}
}

func TestOwnedRuleSnapshotDetectsDriftButIgnoresHostRules(t *testing.T) {
	base := "*nat\n-A MK8S-SVC -d 10.96.0.1/32 -j DNAT --to-destination 10.88.0.2:80\n-A MK8S-SVC -j RETURN\n-A PREROUTING -j MK8S-SVC\n-A OTHER -j ACCEPT\nCOMMIT\n"
	missing := strings.Replace(base, "-A MK8S-SVC -d 10.96.0.1/32 -j DNAT --to-destination 10.88.0.2:80\n", "", 1)
	if ownedRules(base) == ownedRules(missing) {
		t.Fatal("did not detect deleted DNAT rule")
	}
	unrelated := strings.Replace(base, "-A OTHER -j ACCEPT", "-A OTHER -j DROP", 1)
	if ownedRules(base) != ownedRules(unrelated) {
		t.Fatal("unrelated host rule caused false drift")
	}
}

func TestGatePreservesExistingAddressesForRecovery(t *testing.T) {
	owner := "mini-k8s-0123456789abcdef0123456789abcdef"
	state := savedTable{hooks: map[string]bool{}}
	gate := renderGate(owner, []string{"10.96.0.1", "10.96.0.2"}, state)
	if !strings.Contains(gate, "-I PREROUTING 1") || !strings.Contains(gate, "-I OUTPUT 1") || !strings.Contains(gate, "-j DROP") {
		t.Fatal("gate must protect forwarded and host traffic before DNAT")
	}
	addresses := gateAddresses(gate)
	if len(addresses) != 2 || addresses[0] != "10.96.0.1" || addresses[1] != "10.96.0.2" {
		t.Fatalf("gate addresses lost across restart: %#v", addresses)
	}
}

func TestRenderNoEndpointsAndReturnPath(t *testing.T) {
	owner := "mini-k8s-0123456789abcdef0123456789abcdef"
	state := savedTable{hooks: map[string]bool{}}
	routes := []Route{{IP: "10.96.0.1", Port: 80}, {IP: "10.96.0.2", Port: 80, Endpoints: []Endpoint{{IP: "10.88.0.2", Port: 8080}, {IP: "10.89.0.3", Port: 8080}}}}
	filter := renderFilter(owner, routes, state)
	nat := renderNAT(owner, routes, state)
	if !strings.Contains(filter, "10.96.0.1/32 -p tcp --dport 80") || !strings.Contains(filter, "--reject-with tcp-reset") {
		t.Fatal("missing fast failure for empty service")
	}
	if !strings.Contains(nat, "--to-destination 10.88.0.2:8080") || !strings.Contains(nat, "--to-destination 10.89.0.3:8080") || !strings.Contains(nat, "--ctorigdst 10.96.0.2/32 --ctorigdstport 80") {
		t.Fatal("missing destination or scoped return-path SNAT")
	}
	if strings.Contains(nat, "-F ") || strings.Contains(filter, "-F ") {
		t.Fatal("must not flush host chains")
	}
}
