package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidateServiceCIDR(t *testing.T) {
	for _, tc := range []struct {
		cidr  string
		valid bool
	}{
		{"", true},
		{"10.96.0.0/16", true},
		{"10.96.0.0/24", true},
		{"10.96.0.0/30", true},
		{"172.16.0.0/16", true},
		{"192.168.1.0/24", true},
		{"126.255.0.0/16", true},
		{"128.0.0.0/16", true},
		{"169.253.0.0/16", true},
		{"223.255.0.0/16", true},
		{"0.0.0.0/16", false},
		{"127.0.0.0/24", false},
		{"127.255.0.0/16", false},
		{"169.254.0.0/16", false},
		{"224.0.0.0/24", false},
		{"239.255.0.0/16", false},
		{"240.0.0.0/24", false},
		{"255.255.255.0/24", false},
		{"10.96.0.1/24", false},
		{"10.96.0.0/15", false},
		{"10.96.0.0/31", false},
		{"2001:db8::/64", false},
		{"not-a-cidr", false},
	} {
		t.Run(tc.cidr, func(t *testing.T) {
			if err := ValidateServiceCIDR(tc.cidr); (err == nil) != tc.valid {
				t.Fatalf("valid=%t: %v", tc.valid, err)
			}
		})
	}
}

func TestProxyCredentialIsOptionalAndIndependent(t *testing.T) {
	base := Credentials{Admin: "admin", Scheduler: "scheduler"}
	if err := ValidateCredentials(base); err != nil {
		t.Fatal(err)
	}
	base.Proxy = "admin"
	if err := ValidateCredentials(base); err == nil {
		t.Fatal("proxy reused the administrator credential")
	}
	base.Proxy = "proxy"
	if err := ValidateCredentials(base); err != nil {
		t.Fatal(err)
	}
	s := Server{Credentials: base}
	if got := s.authenticate("Bearer proxy"); got.role != "proxy" {
		t.Fatalf("proxy role = %q", got.role)
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/api/mini/v1/services"},
		{http.MethodGet, "/api/mini/v1/replicasets"},
		{http.MethodGet, "/api/mini/v1/service-pool"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", "Bearer proxy")
		response := httptest.NewRecorder()
		s.ServeHTTP(response, req)
		if response.Code != http.StatusForbidden {
			t.Fatalf("proxy %s %s returned %d", tc.method, tc.path, response.Code)
		}
	}
}
