package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (s *Server) watch(w http.ResponseWriter, r *http.Request, kind string, respondError errorResponder) {
	rev, err := strconv.ParseInt(r.URL.Query().Get("resourceVersion"), 10, 64)
	if err != nil || rev < 1 {
		respondError(400, "Invalid", "resourceVersion required")
		return
	}
	_, ok := w.(http.Flusher)
	if !ok {
		respondError(500, "Internal", "streaming unsupported")
		return
	}
	controller := http.NewResponseController(w)
	// Check deadline support before committing a streaming response.
	if s.Admission != nil {
		if err := controller.SetWriteDeadline(time.Now().Add(s.Admission.WatchWriteTimeout)); err != nil {
			respondError(500, "Internal", "watch write deadline unsupported")
			return
		}
		if err := controller.SetWriteDeadline(time.Time{}); err != nil {
			respondError(500, "Internal", "watch write deadline unsupported")
			return
		}
	}
	// List 已覆盖 rev；从下一版本开始订阅，避免 List/Watch 交界遗漏。
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	ch := s.Client.Watch(ctx, resourceKey(kind, ""), clientv3.WithPrefix(), clientv3.WithRev(rev+1), clientv3.WithPrevKV())
	flush := func(value any) error {
		if s.Admission != nil {
			if err := controller.SetWriteDeadline(time.Now().Add(s.Admission.WatchWriteTimeout)); err != nil {
				return err
			}
			defer controller.SetWriteDeadline(time.Time{})
		}
		if value != nil {
			if err := json.NewEncoder(w).Encode(value); err != nil {
				return err
			}
		}
		return controller.Flush()
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	if err := flush(nil); err != nil {
		return
	}
	for response := range ch {
		if response.Err() != nil {
			_ = flush(map[string]any{"type": "ERROR", "reason": "watch ended; relist"})
			return
		}
		for _, event := range response.Events {
			kv := event.Kv
			if event.Type == clientv3.EventTypeDelete {
				kv = event.PrevKv
			}
			if kv == nil {
				continue
			}
			o, err := decode(kv.Value, kind, event.Kv.ModRevision)
			if err != nil {
				continue
			}
			typ := "PUT"
			if event.Type == clientv3.EventTypeDelete {
				typ = "DELETE"
			}
			if err := flush(map[string]any{"type": typ, "object": o.value, "resourceVersion": event.Kv.ModRevision}); err != nil {
				return
			}
		}
	}
}
