package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"
)

// An unauthenticated HTTP/1 client must receive 401 without finishing its body.
func TestUnauthorizedStalledBodyReturnsPromptly(t *testing.T) {
	server := httptest.NewTLSServer(&Server{Credentials: Credentials{Admin: "admin"}})
	defer server.Close()
	conn, err := tls.Dial("tcp", strings.TrimPrefix(server.URL, "https://"), &tls.Config{InsecureSkipVerify: true}) // Test server certificate is ephemeral.
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprint(conn, "POST /api/mini/v1/pods HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer invalid\r\nContent-Length: 64\r\n\r\n{")
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("401 was blocked by incomplete request body: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || !response.Close {
		t.Fatalf("got HTTP %d, close=%t; want 401 and connection close", response.StatusCode, response.Close)
	}
}

func TestUnauthorizedBodyClosesOnlyHTTP1Connection(t *testing.T) {
	s := &Server{Credentials: Credentials{Admin: "admin"}}
	for _, major := range []int{1, 2} {
		req := httptest.NewRequest(http.MethodPost, "/api/mini/v1/pods", strings.NewReader("{"))
		req.ProtoMajor = major
		req.Header.Set("Authorization", "Bearer invalid")
		response := httptest.NewRecorder()
		s.ServeHTTP(response, req)
		if response.Code != http.StatusUnauthorized || (response.Header().Get("Connection") == "close") != (major == 1) {
			t.Fatalf("HTTP/%d: status=%d Connection=%q", major, response.Code, response.Header().Get("Connection"))
		}
	}
}

type failingWatchWriter struct{ *httptest.ResponseRecorder }

func (w *failingWatchWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *failingWatchWriter) FlushError() error                { return errors.New("simulated write failure") }

func TestAdmissionBudgetsAreIndependentAndReleased(t *testing.T) {
	a, err := NewAdmission(1, 1, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, category := range []string{"regular", "watch", "recovery"} {
		release, ok := a.acquire(category)
		if !ok {
			t.Fatal(category)
		}
		if _, ok := a.acquire(category); ok {
			t.Fatalf("%s admitted over limit", category)
		}
		release()
		release, ok = a.acquire(category)
		if !ok {
			t.Fatalf("%s not released", category)
		}
		release()
	}
}

func TestAdmissionRecoveryRequiresAgentAndMatchingRoute(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "/api/mini/v1/nodes/n/status", nil)
	if got := admissionCategory(r, identity{role: "agent", node: "n"}); got != "recovery" {
		t.Fatal(got)
	}
	if got := admissionCategory(r, identity{role: "admin"}); got != "regular" {
		t.Fatal(got)
	}
	r = httptest.NewRequest(http.MethodPost, "/api/mini/v1/pods/p/cleanup", nil)
	if got := admissionCategory(r, identity{role: "agent"}); got != "recovery" {
		t.Fatal(got)
	}
}

func TestAgentRecoveryReadsAndWatchClassification(t *testing.T) {
	agent := identity{role: "agent", node: "n"}
	tests := []struct{ method, path, want string }{
		{"GET", "/api/mini/v1/nodes/n", "recovery"},
		{"GET", "/api/mini/v1/nodes/n/", "recovery"},
		{"GET", "/api/mini/v1/nodes/other", "regular"},
		{"GET", "/api/mini/v1/pods", "recovery"},
		{"GET", "/api/mini/v1/pods/p", "recovery"},
		{"GET", "/api/mini/v1/pods?watch=true&resourceVersion=1", "watch"},
		{"GET", "/api/mini/v1/pods/p/status", "regular"},
		{"POST", "/api/mini/v1/pods/p/cleanup/", "recovery"},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if got := admissionCategory(req, agent); got != tc.want {
			t.Errorf("%s %s: got %s, want %s", tc.method, tc.path, got, tc.want)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/mini/v1/pods", nil)
	if got := admissionCategory(req, identity{role: "admin"}); got != "regular" {
		t.Fatalf("admin pod list classified as %s", got)
	}
}

func TestServeHTTPRejectsSaturatedAuthenticatedRequests(t *testing.T) {
	a, err := NewAdmission(1, 1, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Admission: a, Credentials: Credentials{Admin: "admin"}}
	release, ok := a.acquire("regular")
	if !ok {
		t.Fatal("could not reserve budget")
	}
	defer release()
	req := httptest.NewRequest(http.MethodGet, "/api/mini/v1/pods", nil)
	req.Header.Set("Authorization", "Bearer admin")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("got HTTP %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "regular") {
		t.Fatal("missing budget category")
	}
	watchRelease, ok := a.acquire("watch")
	if !ok {
		t.Fatal("regular saturation consumed watch budget")
	}
	watchRelease()
}

func TestWatchFlushFailureReleasesAdmission(t *testing.T) {
	s := testServer(t, "")
	var err error
	s.Admission, err = NewAdmission(1, 1, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/mini/v1/pods?watch=true&resourceVersion=1", nil)
	req.Header.Set("Authorization", "Bearer admin")
	w := &failingWatchWriter{httptest.NewRecorder()}
	s.ServeHTTP(w, req)
	release, ok := s.Admission.acquire("watch")
	if !ok {
		t.Fatal("watch budget was not released after flush failure")
	}
	release()
}

// A stalled authenticated body must not hold an admission slot forever.
func TestStalledBodyReleasesAdmission(t *testing.T) {
	a, err := NewAdmission(1, 1, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(&Server{Admission: a, Credentials: Credentials{Admin: "admin"}})
	defer server.Close()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(requestBodyTimeout + 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn, "POST /api/mini/v1/pods HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer admin\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{")
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("server did not finish stalled body: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", response.StatusCode)
	}
	release, ok := a.acquire("regular")
	if !ok {
		t.Fatal("stalled request retained admission slot")
	}
	release()
}

func TestSaturatedBodyClosesOnlyHTTP1Connection(t *testing.T) {
	a, _ := NewAdmission(1, 1, 1, time.Second)
	release, _ := a.acquire("regular")
	defer release()
	s := &Server{Admission: a, Credentials: Credentials{Admin: "admin"}}
	for _, major := range []int{1, 2} {
		r := httptest.NewRequest(http.MethodPost, "/api/mini/v1/pods", strings.NewReader("{"))
		r.ProtoMajor = major
		r.Header.Set("Authorization", "Bearer admin")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != http.StatusTooManyRequests || (w.Header().Get("Connection") == "close") != (major == 1) {
			t.Fatalf("HTTP/%d: status=%d Connection=%q", major, w.Code, w.Header().Get("Connection"))
		}
	}
}

// HTTP/2 must time out a stalled stream without exhausting recovery traffic or
// closing the multiplexed connection.
func TestHTTP2StalledBodyReleasesAdmission(t *testing.T) {
	a, err := NewAdmission(1, 1, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewUnstartedServer(&Server{Admission: a, Credentials: Credentials{Admin: "admin", Agents: map[string]string{"node": "agent"}}})
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	client := s.Client()
	client.Timeout = requestBodyTimeout + 5*time.Second
	transport := client.Transport.(*http.Transport)
	transport.MaxConnsPerHost = 1
	reader, writer := io.Pipe()
	defer writer.Close()
	request, err := http.NewRequest(http.MethodPost, s.URL+"/api/mini/v1/pods", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer admin")
	initialConn := make(chan net.Conn, 1)
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { initialConn <- info.Conn }}))
	type result struct {
		response *http.Response
		err      error
	}
	done := make(chan result, 1)
	go func() { response, err := client.Do(request); done <- result{response, err} }()
	if _, err := writer.Write([]byte("{")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(a.regular) != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(a.regular) != 1 {
		t.Fatal("stalled stream did not acquire regular admission")
	}
	conn := <-initialConn

	check := func(method, path, token, body string, want int) {
		t.Helper()
		req, err := http.NewRequest(method, s.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		gotConn := make(chan net.Conn, 1)
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { gotConn <- info.Conn }}))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if <-gotConn != conn {
			t.Fatal("request used a different connection instead of multiplexing HTTP/2 streams")
		}
		if resp.ProtoMajor != 2 || resp.StatusCode != want {
			t.Fatalf("%s: got %s HTTP %d, want HTTP/2 %d", path, resp.Proto, resp.StatusCode, want)
		}
	}
	check(http.MethodPost, "/api/mini/v1/pods", "admin", "{}", http.StatusTooManyRequests)
	check(http.MethodPost, "/api/mini/v1/pods/p/cleanup", "agent", "{}", http.StatusBadRequest)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("stalled HTTP/2 request failed: %v", got.err)
		}
		defer got.response.Body.Close()
		if got.response.ProtoMajor != 2 || got.response.StatusCode != http.StatusBadRequest {
			t.Fatalf("stalled stream: got %s HTTP %d", got.response.Proto, got.response.StatusCode)
		}
	case <-time.After(requestBodyTimeout + 3*time.Second):
		t.Fatal("stalled HTTP/2 request did not finish")
	}
	deadline = time.Now().Add(time.Second)
	for len(a.regular) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(a.regular) != 0 {
		t.Fatal("stalled stream retained regular admission")
	}
	check(http.MethodPost, "/api/mini/v1/pods", "admin", "{", http.StatusBadRequest)
}
