package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"mini-k8s/internal/server"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8443", "HTTPS listen address")
	etcd := flag.String("etcd", "http://127.0.0.1:2379", "comma-separated etcd endpoints")
	cert := flag.String("tls-cert", "", "server certificate PEM")
	key := flag.String("tls-key", "", "server private key PEM")
	auth := flag.String("auth-file", "", "JSON identity credentials file")
	serviceCIDR := flag.String("service-cidr", "", "IPv4 CIDR for Service addresses (must not overlap Pod or host networks)")
	regularLimit := flag.Int("max-inflight", 64, "maximum concurrent ordinary API requests")
	watchLimit := flag.Int("max-watches", 32, "maximum concurrent watch requests")
	recoveryLimit := flag.Int("max-recovery-inflight", 16, "maximum concurrent agent recovery requests")
	watchWriteTimeout := flag.Duration("watch-write-timeout", 2*time.Second, "deadline for each watch write")
	flag.Parse()
	admission, err := server.NewAdmission(*regularLimit, *watchLimit, *recoveryLimit, *watchWriteTimeout)
	if err != nil {
		log.Fatal(err)
	}
	if err := server.ValidateServiceCIDR(*serviceCIDR); err != nil {
		log.Fatalf("invalid service-cidr: %v", err)
	}
	if *cert == "" || *key == "" || *auth == "" {
		log.Fatal("tls-cert, tls-key, and auth-file are required")
	}
	// 凭据从文件加载，避免将令牌放在进程参数中。
	data, err := os.ReadFile(*auth)
	if err != nil {
		log.Fatal(err)
	}
	var credentials server.Credentials
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&credentials); err != nil {
		log.Fatal(err)
	}
	if err := server.ValidateCredentials(credentials); err != nil {
		log.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*etcd, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	apiServer := &server.Server{Client: client, Credentials: credentials, ServiceCIDR: *serviceCIDR, Admission: admission}
	monitorCtx, cancelMonitor := context.WithCancel(context.Background())
	defer cancelMonitor()
	go apiServer.RunNodeHealthMonitor(monitorCtx)
	httpServer := &http.Server{Addr: *listen, Handler: apiServer, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(httpServer.ListenAndServeTLS(*cert, *key))
}
