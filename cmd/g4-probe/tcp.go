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
	"strings"
	"time"
)

type probeReply struct {
	Identity string `json:"identity"`
}

// serve 只提供一次请求、一次响应的 TCP 协议，不依赖容器内安装 shell。
func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	address := flags.String("listen", ":8080", "TCP listen address")
	identity := flags.String("identity", "", "unique instance identity (default: hostname)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *identity == "" {
		var err error
		*identity, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("read hostname: %w", err)
		}
	}
	if *identity == "" || len(*identity) > 128 || flags.NArg() != 0 {
		return errors.New("identity is required and must not exceed 128 bytes")
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			line, err := bufio.NewReader(io.LimitReader(c, 32)).ReadString('\n')
			if err != nil || line != "probe\n" {
				return
			}
			_ = json.NewEncoder(c).Encode(probeReply{Identity: *identity})
		}(conn)
	}
}

type probeAttempt struct {
	Attempt  int       `json:"attempt"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Local    string    `json:"local,omitempty"`
	Remote   string    `json:"remote,omitempty"`
	Identity string    `json:"identity,omitempty"`
	Error    string    `json:"error,omitempty"`
}

func probeOnce(target, expected string, timeout time.Duration, attempt int) probeAttempt {
	result := probeAttempt{Attempt: attempt, Started: time.Now().UTC()}
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer conn.Close()
	result.Local, result.Remote = conn.LocalAddr().String(), conn.RemoteAddr().String()
	if err = conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		result.Error = err.Error()
		return result
	}
	if _, err = io.WriteString(conn, "probe\n"); err != nil {
		result.Error = err.Error()
		return result
	}
	var reply probeReply
	decoder := json.NewDecoder(io.LimitReader(conn, 1024))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&reply); err != nil {
		result.Error = err.Error()
		return result
	}
	result.Identity = reply.Identity
	if reply.Identity != expected {
		result.Error = fmt.Sprintf("identity mismatch: expected %q", expected)
	}
	return result
}

// 每次调用 Dial 都新建连接，单方向失败也会继续记录剩余尝试。
func check(args []string) error {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	target := flags.String("target", "", "target Pod IP and TCP port")
	expected := flags.String("expect", "", "expected target identity")
	count := flags.Int("count", 10, "number of new connections")
	timeout := flags.Duration("timeout", 3*time.Second, "per-connection timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if _, _, err := net.SplitHostPort(*target); err != nil || strings.TrimSpace(*expected) == "" || *count < 1 || *count > 1000 || *timeout <= 0 || flags.NArg() != 0 {
		return errors.New("valid target, expected identity, positive timeout and count (1..1000) required")
	}
	encoder := json.NewEncoder(os.Stdout)
	failures := 0
	for i := 1; i <= *count; i++ {
		result := probeOnce(*target, *expected, *timeout, i)
		result.Finished = time.Now().UTC()
		if result.Error != "" {
			failures++
		}
		if err := encoder.Encode(result); err != nil {
			return err
		}
	}
	if failures != 0 {
		return fmt.Errorf("%d of %d TCP connections failed", failures, *count)
	}
	return nil
}
