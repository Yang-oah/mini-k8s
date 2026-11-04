package server

import (
	"testing"
	"time"

	"mini-k8s/internal/api"
)

func TestNodeHeartbeatExpiration(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		last      time.Time
		available bool
	}{
		{"never reported", time.Time{}, false},
		{"future timestamp", now.Add(time.Second), false},
		{"recent", now.Add(-12 * time.Second), true},
		{"binding safety margin", now.Add(-14 * time.Second), false},
		{"expired at boundary", now.Add(-18 * time.Second), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := api.Node{Status: api.NodeStatus{Ready: true, LastHeartbeat: tc.last}}
			if nodeAvailable(&node, now) != tc.available {
				t.Fatal("unexpected availability")
			}
			projectNodeHealth(&node, now)
			if !nodeHeartbeatFresh(&node, now) && (node.Status.Ready || node.Status.Condition != "Unknown") {
				t.Fatalf("expired heartbeat was not marked Unknown: %+v", node.Status)
			}
		})
	}
}

func TestSchedulerViewRespectsBindingSafetyMargin(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		age             time.Duration
		adminReady      bool
		schedulerReady  bool
		schedulerStatus string
	}{
		{12 * time.Second, true, true, "Ready"},
		{13 * time.Second, true, false, "NotReady"},
		{17 * time.Second, true, false, "NotReady"},
		{18 * time.Second, false, false, "Unknown"},
	} {
		original := api.Node{Status: api.NodeStatus{Ready: true, Condition: "Ready", LastHeartbeat: now.Add(-tc.age)}}
		admin, scheduler := original, original
		projectNodeHealth(&admin, now)
		projectNodeSchedulingHealth(&scheduler, now)
		if admin.Status.Ready != tc.adminReady || scheduler.Status.Ready != tc.schedulerReady || scheduler.Status.Condition != tc.schedulerStatus {
			t.Errorf("age %v: admin=%+v scheduler=%+v", tc.age, admin.Status, scheduler.Status)
		}
	}
}
