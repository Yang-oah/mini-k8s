package api

import (
	"strings"
	"testing"
)

func validPod() Pod {
	return Pod{
		APIVersion: Version, Kind: "Pod", Metadata: Metadata{Name: "web", Namespace: "default", UID: "uid-1"},
		Spec: PodSpec{Image: "example:1", RestartPolicy: "Always", Resources: ResourceRequirements{
			Requests: Resources{CPUMilli: 100, MemoryBytes: 64}, Limits: Resources{CPUMilli: 200, MemoryBytes: 128},
		}},
	}
}

func TestValidatePod(t *testing.T) {
	tests := []struct {
		name  string
		edit  func(*Pod)
		valid bool
	}{
		{"valid", func(*Pod) {}, true},
		{"wrong namespace", func(p *Pod) { p.Metadata.Namespace = "other" }, false},
		{"bad name", func(p *Pod) { p.Metadata.Name = "Web" }, false},
		{"no image", func(p *Pod) { p.Spec.Image = "" }, false},
		{"zero request", func(p *Pod) { p.Spec.Resources.Requests.CPUMilli = 0 }, false},
		{"request exceeds limit", func(p *Pod) { p.Spec.Resources.Requests.MemoryBytes = 129 }, false},
		{"restart policy", func(p *Pod) { p.Spec.RestartPolicy = "Never" }, false},
		{"invalid port", func(p *Pod) { p.Spec.Readiness = &TCPProbe{Port: 65536} }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validPod()
			tt.edit(&p)
			if err := ValidatePod(p); (err == nil) != tt.valid {
				t.Fatalf("ValidatePod() = %v, valid = %v", err, tt.valid)
			}
		})
	}
}

func TestStrictJSON(t *testing.T) {
	for _, input := range []string{
		`{"spec":{"hostNetwork":true}}`,
		`{"spec":{"resources":{"requests":{"cpuMilli":1,"unexpected":2}}}}`,
		`{} {}`,
	} {
		var p Pod
		if err := DecodeStrict(strings.NewReader(input), &p); err == nil {
			t.Errorf("accepted invalid JSON %s", input)
		}
	}
}

func TestImmutableUpdates(t *testing.T) {
	old := validPod()
	next := old
	next.Metadata.Annotations = map[string]string{"note": "updated"}
	if err := ValidatePodUpdate(old, next); err != nil {
		t.Fatalf("annotation update rejected: %v", err)
	}
	next.Spec.Image = "example:2"
	if err := ValidatePodUpdate(old, next); err == nil {
		t.Fatal("Pod spec change accepted")
	}

	rs := ReplicaSet{APIVersion: Version, Kind: "ReplicaSet", Metadata: Metadata{Name: "web"}, Spec: ReplicaSetSpec{
		Replicas: 1, Selector: map[string]string{"app": "web"}, Template: PodTemplate{Labels: map[string]string{"app": "web"}, Spec: old.Spec},
	}}
	updated := rs
	updated.Spec.Replicas = 3
	if err := ValidateReplicaSetUpdate(rs, updated); err != nil {
		t.Fatalf("scaling rejected: %v", err)
	}
	updated.Spec.Selector = map[string]string{"app": "other"}
	updated.Spec.Template.Labels = map[string]string{"app": "other"}
	if err := ValidateReplicaSetUpdate(rs, updated); err == nil {
		t.Fatal("selector and template change accepted")
	}
}
