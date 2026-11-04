// Command g5-service-fixture is a disposable, locally controlled Service test workload.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type event struct {
	At       time.Time `json:"at"`
	Kind     string    `json:"kind"`
	Identity string    `json:"identity,omitempty"`
	Source   string    `json:"source,omitempty"`
	Error    string    `json:"error,omitempty"`
}

type fixture struct {
	address  string
	identity string
	listener net.Listener // 控制连接串行处理，只有控制循环修改此字段。
	logMu    sync.Mutex
}

func (f *fixture) log(e event) {
	f.logMu.Lock()
	defer f.logMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(e)
}

func (f *fixture) set(state string) event {
	e := event{Kind: state, Identity: f.identity}
	switch state {
	case "on":
		if f.listener == nil {
			listener, err := net.Listen("tcp", f.address)
			if err != nil {
				e.Error = err.Error()
				break
			}
			f.listener = listener
			go f.accept(listener)
		}
	case "off":
		if f.listener != nil {
			if err := f.listener.Close(); err != nil {
				e.Error = err.Error()
			} else {
				f.listener = nil
			}
		}
	default:
		e.Error = "state must be on or off"
	}
	e.At = time.Now().UTC()
	f.log(e)
	return e
}

func (f *fixture) accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(3 * time.Second))
			line, err := bufio.NewReader(io.LimitReader(c, 32)).ReadString('\n')
			if err != nil || line != "probe\n" { // readiness 的空连接不计为业务访问。
				return
			}
			if err := json.NewEncoder(c).Encode(map[string]string{"identity": f.identity}); err != nil {
				return
			}
			f.log(event{At: time.Now().UTC(), Kind: "request", Identity: f.identity, Source: c.RemoteAddr().String()})
		}(conn)
	}
}

func serve(args []string) error {
	f := flag.NewFlagSet("serve", flag.ContinueOnError)
	control := f.String("control", "/g5-service/control.sock", "container-local Unix control socket")
	address := f.String("listen", ":8080", "business TCP address")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !filepath.IsAbs(*control) {
		return errors.New("absolute control socket path required")
	}
	id, err := os.Hostname()
	if err != nil || id == "" {
		return errors.New("container hostname is required")
	}
	dir := filepath.Dir(*control)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("control directory must be private")
	}
	listener, err := net.Listen("unix", *control) // 不覆盖已存在的 socket。
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(*control, 0600); err != nil {
		return err
	}
	server := &fixture{address: *address, identity: id}
	defer func() {
		if server.listener != nil {
			_ = server.listener.Close()
		}
	}()
	server.log(event{At: time.Now().UTC(), Kind: "off", Identity: id})
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		line, err := bufio.NewReader(io.LimitReader(conn, 17)).ReadString('\n')
		if err == nil && len(line) <= 16 {
			_ = json.NewEncoder(conn).Encode(server.set(strings.TrimSpace(line)))
		}
		_ = conn.Close()
	}
}

func set(args []string) error {
	f := flag.NewFlagSet("set", flag.ContinueOnError)
	control := f.String("control", "/g5-service/control.sock", "container-local Unix control socket")
	state := f.String("state", "", "on or off")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !filepath.IsAbs(*control) || (*state != "on" && *state != "off") {
		return errors.New("absolute control socket and state on/off required")
	}
	conn, err := net.DialTimeout("unix", *control, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(conn, *state); err != nil {
		return err
	}
	var response event
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
		return err
	}
	if response.Error != "" || response.Kind != *state || response.At.IsZero() {
		return errors.New("invalid or failed control response")
	}
	return nil
}

type attempt struct {
	Number   int       `json:"number"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Local    string    `json:"local,omitempty"`
	Remote   string    `json:"remote,omitempty"`
	Identity string    `json:"identity,omitempty"`
	Outcome  string    `json:"outcome,omitempty"`
	Detail   string    `json:"detail,omitempty"`
	Error    string    `json:"error,omitempty"`
}

func connect(target string, allowed map[string]bool, timeout time.Duration, expectFailure bool, number int) (out attempt) {
	started := time.Now()
	out = attempt{Number: number, Start: started.UTC()}
	defer func() {
		out.End = time.Now().UTC()
		if expectFailure && out.Error == "" && time.Since(started) >= 3*time.Second {
			out.Error = "refusal exceeded three seconds"
		}
	}()
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		// 超时、无路由等无法证明是 Service 无端点后的主动拒绝。
		if expectFailure && errors.Is(err, syscall.ECONNREFUSED) {
			out.Outcome, out.Detail = "refused", err.Error()
			return out
		}
		out.Error = err.Error()
		return out
	}
	defer conn.Close()
	out.Local, out.Remote = conn.LocalAddr().String(), conn.RemoteAddr().String()
	if expectFailure {
		out.Error = "connection unexpectedly succeeded"
		return out
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		out.Error = err.Error()
		return out
	}
	if _, err := io.WriteString(conn, "probe\n"); err != nil {
		out.Error = err.Error()
		return out
	}
	var reply struct {
		Identity string `json:"identity"`
	}
	decoder := json.NewDecoder(io.LimitReader(conn, 1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		out.Error = err.Error()
		return out
	}
	out.Identity = reply.Identity
	if !allowed[reply.Identity] {
		out.Error = "unexpected endpoint identity"
	} else {
		out.Outcome = "success"
	}
	return out
}

func check(args []string) error {
	f := flag.NewFlagSet("check", flag.ContinueOnError)
	target := f.String("target", "", "ClusterIP:port")
	expected := f.String("expect", "", "comma-separated allowed Pod identities")
	count := f.Int("count", 20, "number of new TCP connections")
	timeout := f.Duration("timeout", 2500*time.Millisecond, "per-connection deadline")
	noEndpoint := f.Bool("expect-no-endpoint", false, "require immediate connection failure")
	if err := f.Parse(args); err != nil {
		return err
	}
	if _, _, err := net.SplitHostPort(*target); err != nil || f.NArg() != 0 || *count < 1 || *count > 1000 || *timeout <= 0 || *timeout >= 3*time.Second {
		return errors.New("target, count 1..1000, and timeout between 0 and 3s are required")
	}
	allowed := make(map[string]bool)
	for _, id := range strings.Split(*expected, ",") {
		if id = strings.TrimSpace(id); id != "" {
			allowed[id] = true
		}
	}
	if !*noEndpoint && len(allowed) == 0 {
		return errors.New("at least one expected identity is required")
	}
	enc := json.NewEncoder(os.Stdout)
	failed := 0
	for i := 1; i <= *count; i++ {
		result := connect(*target, allowed, *timeout, *noEndpoint, i)
		if result.Error != "" {
			failed++
		}
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d attempts failed", failed, *count)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: g5-service-fixture serve|set|check [options]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "set":
		err = set(os.Args[2:])
	case "check":
		err = check(os.Args[2:])
	default:
		err = errors.New("unknown subcommand")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "g5-service-fixture:", err)
		os.Exit(1)
	}
}
