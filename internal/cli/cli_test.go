package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const podManifest = `apiVersion: mini.k8s/v1
kind: Pod
metadata:
  name: web
spec:
  image: example:1
  restartPolicy: Always
  resources:
    requests: {cpuMilli: 100, memoryBytes: 64}
    limits: {cpuMilli: 200, memoryBytes: 128}
`

type handlerTransport struct{ handler http.Handler }

func (h handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, req)
	return recorder.Result(), nil
}

func TestReadManifestsStrict(t *testing.T) {
	if _, err := readManifests(strings.NewReader(podManifest)); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	cases := []string{
		podManifest + "  hostNetwork: true\n",
		strings.Replace(podManifest, "  name: web", "  name: web\n  uid: forged", 1),
		podManifest + "status: {ready: true}\n",
		strings.Replace(podManifest, "  name: web", "  name: web\n  name: duplicate", 1),
		strings.Replace(podManifest, "kind: Pod", "kind: Mystery", 1),
	}
	for _, input := range cases {
		if _, err := readManifests(strings.NewReader(input)); err == nil {
			t.Errorf("invalid manifest was accepted:\n%s", input)
		}
	}
}

func TestApplyUsesResourceVersionAndPreservesStatus(t *testing.T) {
	var received map[string]any
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("authorization = %q", got)
		}
		if r.URL.Path != apiPath+"pods/web" {
			t.Errorf("path = %q", r.URL.Path)
		}
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, `{"apiVersion":"mini.k8s/v1","kind":"Pod","metadata":{"name":"web","uid":"existing-uid","resourceVersion":42},"spec":{},"status":{"phase":"Running"},"nodeUID":"node-1"}`)
		case http.MethodPut:
			if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
				t.Error(err)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	})
	c := &client{base: "https://api.example", token: "secret", http: &http.Client{Transport: handlerTransport{handler}}}
	manifests, err := readManifests(strings.NewReader(podManifest))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.apply(context.Background(), manifests[0]); err != nil {
		t.Fatal(err)
	}
	meta := received["metadata"].(map[string]any)
	if meta["uid"] != "existing-uid" || meta["resourceVersion"] != float64(42) {
		t.Errorf("system metadata lost: %#v", meta)
	}
	if received["nodeUID"] != "node-1" || received["status"].(map[string]any)["phase"] != "Running" {
		t.Errorf("system state lost: %#v", received)
	}
}

func TestApplyCreateAndConflict(t *testing.T) {
	writes := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPost:
			writes++
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"reason":"Conflict"}`)
		default:
			t.Errorf("unexpected request: %s", r.Method)
		}
	})
	c := &client{base: "https://api.example", token: "secret", http: &http.Client{Transport: handlerTransport{handler}}}
	manifests, err := readManifests(strings.NewReader(podManifest))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.apply(context.Background(), manifests[0]); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("expected conflict, got %v", err)
	}
	if writes != 1 {
		t.Fatalf("write attempts = %d", writes)
	}
}
