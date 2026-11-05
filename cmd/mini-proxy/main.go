// mini-proxy 将 Service 就绪端点同步到本机专用转发链。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mini-k8s/internal/proxy"
)

func main() {
	server := flag.String("server", "https://127.0.0.1:8443", "API HTTPS origin")
	ca := flag.String("ca-file", "", "additional API CA PEM file")
	token := flag.String("token-file", "", "read-only proxy token file")
	state := flag.String("state-dir", "/var/lib/mini-k8s/proxy", "private proxy ownership directory")
	resync := flag.Duration("resync", 2*time.Second, "full reconciliation interval, at most 2s")
	flag.Parse()
	if *token == "" {
		log.Fatal("token-file is required")
	}
	installer, err := proxy.NewIPTables(*state)
	if err != nil {
		log.Fatal(err)
	}
	p, err := proxy.New(*server, *ca, *token, *resync, installer)
	if err != nil {
		_ = installer.Close()
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := p.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
