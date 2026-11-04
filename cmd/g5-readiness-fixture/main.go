// Command g5-readiness-fixture is a disposable TCP readiness test workload.
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
	"time"
)

type event struct {
	At    time.Time `json:"at"`
	State string    `json:"state"`
	Error string    `json:"error,omitempty"`
}

// fixture 的控制平面只监听容器内 Unix socket；业务 TCP 不接收控制命令。
type fixture struct {
	address  string
	listener net.Listener
}

func (f *fixture) set(state string) event {
	result := event{State: state}
	switch state {
	case "on":
		if f.listener == nil {
			listener, err := net.Listen("tcp", f.address)
			if err != nil {
				result.Error = err.Error()
				break
			}
			f.listener = listener
			// readiness 只需要成功连接；立即关闭连接，不保留业务状态。
			go func() {
				for {
					c, err := listener.Accept()
					if err != nil {
						return
					}
					_ = c.Close()
				}
			}()
		}
	case "off":
		if f.listener != nil {
			if err := f.listener.Close(); err != nil {
				result.Error = err.Error()
			} else {
				f.listener = nil
			}
		}
	default:
		result.Error = "state must be on or off"
	}
	result.At = time.Now().UTC()
	return result
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	control := flags.String("control", "/g5-control/control.sock", "absolute Unix control socket path")
	address := flags.String("listen", ":8080", "TCP readiness address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*control) {
		return errors.New("absolute control socket path required")
	}
	// 父目录在绑定前就必须是私有目录，避免 chmod 之前暴露控制 socket。
	controlDir := filepath.Dir(*control)
	if err := os.MkdirAll(controlDir, 0700); err != nil {
		return fmt.Errorf("create control directory: %w", err)
	}
	info, err := os.Lstat(controlDir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("control socket directory must be private")
	}
	// 不删除已有文件或 socket，避免影响其他进程。
	listener, err := net.Listen("unix", *control)
	if err != nil {
		return fmt.Errorf("listen on control socket: %w", err)
	}
	defer listener.Close()
	if err := os.Chmod(*control, 0600); err != nil {
		return fmt.Errorf("protect control socket: %w", err)
	}
	f := &fixture{address: *address}
	defer func() {
		if f.listener != nil {
			_ = f.listener.Close()
		}
	}()
	// 初始不监听：容器 Running 不能自动等同 Ready。
	if err := json.NewEncoder(os.Stdout).Encode(event{At: time.Now().UTC(), State: "off"}); err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		line, err := bufio.NewReader(io.LimitReader(conn, 17)).ReadString('\n')
		if err == nil && len(line) <= 16 {
			result := f.set(strings.TrimSpace(line))
			_ = json.NewEncoder(conn).Encode(result)
			_ = json.NewEncoder(os.Stdout).Encode(result)
		}
		_ = conn.Close()
	}
}

func set(args []string) error {
	flags := flag.NewFlagSet("set", flag.ContinueOnError)
	control := flags.String("control", "/g5-control/control.sock", "absolute Unix control socket path")
	state := flags.String("state", "", "on or off")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*control) || (*state != "on" && *state != "off") {
		return errors.New("absolute control path and state on or off required")
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
	var result event
	if err := json.NewDecoder(conn).Decode(&result); err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return err
	}
	if result.Error != "" || result.State != *state || result.At.IsZero() {
		return errors.New("control operation failed or returned invalid response")
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: g5-readiness-fixture serve|set [options]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "set":
		err = set(os.Args[2:])
	default:
		err = errors.New("unknown subcommand")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "g5-readiness-fixture:", err)
		os.Exit(1)
	}
}
