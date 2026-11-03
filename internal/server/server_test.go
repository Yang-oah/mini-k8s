package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthenticationAndWriteAuthorization(t *testing.T) {
	s := &Server{Credentials: Credentials{Admin: "admin-token", Scheduler: "scheduler-token", Agents: map[string]string{"node-a": "agent-token"}}}
	tests := []struct {
		token, method, path string
		want                int
	}{
		{"", "GET", "/api/mini/v1/pods", 401},
		{"wrong", "GET", "/api/mini/v1/pods", 401},
		{"agent-token", "POST", "/api/mini/v1/pods", 403},
		{"admin-token", "POST", "/api/mini/v1/pods/p/bind", 403},
		{"admin-token", "PUT", "/api/mini/v1/pods/p/status", 403},
		{"scheduler-token", "PUT", "/api/mini/v1/nodes/n/status", 403},
		{"agent-token", "GET", "/api/mini/v1/pods?watch=true&resourceVersion=1", 403},
	}
	for _, tt := range tests {
		t.Run(tt.token+tt.method+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			w := httptest.NewRecorder()
			s.ServeHTTP(w, req)
			if w.Code != tt.want {
				t.Fatalf("got %d, want %d", w.Code, tt.want)
			}
			var result map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result["requestId"] == "" {
				t.Fatalf("invalid error response %q: %v", w.Body.String(), err)
			}
			if strings.Contains(w.Body.String(), tt.token) && tt.token != "" {
				t.Fatal("credential leaked in error response")
			}
		})
	}
}

func TestCredentialConfiguration(t *testing.T) {
	if err := ValidateCredentials(Credentials{Admin: "same", Scheduler: "same"}); err == nil {
		t.Fatal("shared credential accepted")
	}
	if err := ValidateCredentials(Credentials{Admin: "a", Scheduler: "s", Agents: map[string]string{"node-a": "a"}}); err == nil {
		t.Fatal("agent credential shared with admin")
	}
	if err := ValidateCredentials(Credentials{Admin: "a", Scheduler: "s", Agents: map[string]string{"node-a": "n"}}); err != nil {
		t.Fatal(err)
	}
}

func TestAuthHeaderRequiresExactToken(t *testing.T) {
	s := Server{Credentials: Credentials{Admin: "secret"}}
	for _, h := range []string{"secret", "Basic secret", "Bearer secret-extra", "Bearer "} {
		if s.authenticate(h).role != "" {
			t.Fatalf("accepted %q", h)
		}
	}
	if got := s.authenticate("Bearer secret").role; got != "admin" {
		t.Fatalf("got %q", got)
	}
}

var _ http.Handler = (*Server)(nil)
