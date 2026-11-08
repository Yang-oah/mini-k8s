package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Admission holds independent request budgets for regular, watch and recovery traffic.
type Admission struct {
	regular, watch, recovery chan struct{}
	WatchWriteTimeout        time.Duration
}

// NewAdmission creates fixed-size, non-queueing request budgets.
func NewAdmission(regular, watch, recovery int, writeTimeout time.Duration) (*Admission, error) {
	if regular <= 0 || watch <= 0 || recovery <= 0 || writeTimeout <= 0 {
		return nil, fmt.Errorf("request budgets and watch write timeout must be positive")
	}
	return &Admission{make(chan struct{}, regular), make(chan struct{}, watch), make(chan struct{}, recovery), writeTimeout}, nil
}

func admissionCategory(r *http.Request, who identity) string {
	if !strings.HasPrefix(r.URL.Path, "/api/mini/v1/") {
		return "regular"
	}
	if isWatchRequest(r) {
		return "watch"
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/mini/v1/"), "/")
	parts := strings.Split(path, "/")
	if who.role == "agent" {
		if r.Method == http.MethodGet && ((len(parts) == 2 && parts[0] == "nodes" && parts[1] == who.node) || (parts[0] == "pods" && (len(parts) == 1 || (len(parts) == 2 && parts[1] != "")))) {
			return "recovery"
		}
		if len(parts) == 3 && parts[1] != "" && ((parts[0] == "nodes" && parts[1] == who.node && parts[2] == "status" && r.Method == http.MethodPut) || (parts[0] == "pods" && parts[2] == "cleanup" && r.Method == http.MethodPost)) {
			return "recovery"
		}
	}
	return "regular"
}

func (a *Admission) acquire(category string) (func(), bool) {
	budget := a.regular
	if category == "watch" {
		budget = a.watch
	} else if category == "recovery" {
		budget = a.recovery
	}
	select {
	case budget <- struct{}{}:
		return func() { <-budget }, true
	default:
		return nil, false
	}
}
