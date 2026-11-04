package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"mini-k8s/internal/agent"
	containerruntime "mini-k8s/internal/runtime"
)

func main() {
	server := flag.String("server", "https://127.0.0.1:8443", "API HTTPS origin")
	node := flag.String("node", "", "pre-registered node name")
	token := flag.String("token-file", "", "node token file")
	ca := flag.String("ca-file", "", "additional CA PEM file")
	endpoint := flag.String("cri-endpoint", "unix:///run/containerd/containerd.sock", "local CRI endpoint")
	stateDir := flag.String("state-dir", "/var/lib/mini-k8s/agent", "runtime identity and lock directory")
	logDir := flag.String("log-dir", "/var/log/mini-k8s", "container log directory")
	resync := flag.Duration("resync", 2*time.Second, "full reconciliation interval")
	controlSocket := flag.String("control-socket", "", "local control socket (default: state-dir/control.sock)")
	logsUID := flag.String("logs-uid", "", "read logs for a Pod UID through the local Agent")
	cleanupUID := flag.String("cleanup-uid", "", "retry cleanup for an absent Pod UID through the local Agent")
	cleanupName := flag.String("cleanup-name", "", "name of the absent or recreated Pod")
	tail := flag.Int("tail", 100, "number of log lines")
	flag.Parse()
	if *controlSocket == "" {
		*controlSocket = filepath.Join(*stateDir, "control.sock")
	}
	if *logsUID != "" || *cleanupUID != "" {
		if *logsUID != "" && *cleanupUID != "" {
			log.Fatal("logs-uid and cleanup-uid are mutually exclusive")
		}
		if err := controlRequest(*controlSocket, *logsUID, *cleanupName, *cleanupUID, *tail); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *node == "" || *token == "" {
		log.Fatal("node and token-file are required")
	}
	runtime, err := containerruntime.New(*endpoint, *stateDir, *logDir)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()
	a, err := agent.New(*server, *node, *token, *ca, *resync, runtime, *stateDir)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := startControl(ctx, *controlSocket, *stateDir, a); err != nil {
		log.Fatal(err)
	}
	if err := a.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
