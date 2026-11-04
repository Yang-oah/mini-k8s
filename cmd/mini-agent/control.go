package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"mini-k8s/internal/agent"
)

// startControl 只监听本地 Unix socket。持有运行时锁后才能移除遗留 socket。
func startControl(ctx context.Context, path, stateDir string, a *agent.Agent) error {
	// socket 创建与 chmod 之间存在短暂窗口，父目录必须始终仅限当前用户访问。
	dir, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return err
	}
	if !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
		return errors.New("control socket directory must be private (0700)")
	}
	listener, cleanup, err := prepareControlSocket(path, stateDir)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		tail, err := strconv.Atoi(r.URL.Query().Get("tail"))
		if err != nil || tail < 0 {
			http.Error(w, "invalid tail", http.StatusBadRequest)
			return
		}
		logs, err := a.Logs(r.Context(), r.URL.Query().Get("uid"), tail)
		if err != nil {
			http.Error(w, "cannot read logs", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, logs)
	})
	mux.HandleFunc("/cleanup", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		}
		decoder := json.NewDecoder(io.LimitReader(r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := a.CleanupStale(r.Context(), req.Name, req.UID); err != nil {
			http.Error(w, "cannot confirm stale UID or cleanup", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = server.Close() }()
	go func() { _ = server.Serve(listener); cleanup() }()
	return nil
}

func controlRequest(path, logsUID, name, cleanupUID string, tail int) error {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	method, target := http.MethodGet, "http://agent/logs?uid="+url.QueryEscape(logsUID)+"&tail="+strconv.Itoa(tail)
	var body io.Reader
	if cleanupUID != "" {
		method, target = http.MethodPost, "http://agent/cleanup"
		data, err := json.Marshal(map[string]string{"name": name, "uid": cleanupUID})
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("local agent returned HTTP %d", resp.StatusCode)
	}
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}
