package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (s *Server) watch(w http.ResponseWriter, r *http.Request, kind string, respondError errorResponder) {
	rev, err := strconv.ParseInt(r.URL.Query().Get("resourceVersion"), 10, 64)
	if err != nil || rev < 1 {
		respondError(400, "Invalid", "resourceVersion required")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		respondError(500, "Internal", "streaming unsupported")
		return
	}
	// List 已覆盖 rev；从下一版本开始订阅，避免 List/Watch 交界遗漏。
	ch := s.Client.Watch(r.Context(), resourceKey(kind, ""), clientv3.WithPrefix(), clientv3.WithRev(rev+1), clientv3.WithPrevKV())
	w.Header().Set("Content-Type", "application/x-ndjson")
	flusher.Flush()
	for response := range ch {
		if response.Err() != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "ERROR", "reason": "watch ended; relist"})
			flusher.Flush()
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
			_ = json.NewEncoder(w).Encode(map[string]any{"type": typ, "object": o.value, "resourceVersion": event.Kv.ModRevision})
			flusher.Flush()
		}
	}
}
