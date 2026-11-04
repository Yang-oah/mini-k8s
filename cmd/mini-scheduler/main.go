// mini-scheduler 将未绑定 Pod 调度到满足资源请求的健康节点。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mini-k8s/internal/scheduler"
)

func main() {
	server := flag.String("server", "https://127.0.0.1:8443", "API HTTPS origin")
	token := flag.String("token-file", "", "scheduler token file")
	ca := flag.String("ca-file", "", "additional CA PEM file")
	resync := flag.Duration("resync", 2*time.Second, "full reconciliation interval")
	flag.Parse()
	if *token == "" {
		log.Fatal("token-file is required")
	}
	s, err := scheduler.New(*server, *token, *ca, *resync)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := s.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
