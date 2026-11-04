package acceptance

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// g3GuardConfig 中的路径均来自本轮私有目录，避免仅按进程名匹配。
type g3GuardConfig struct{ test, agent, manager, evidence string }

func g3Arg(args []string, name string) string {
	value := ""
	for i := 0; i < len(args); i++ {
		if args[i] == name {
			if value != "" || i+1 >= len(args) {
				return ""
			}
			value = args[i+1]
		}
		if strings.HasPrefix(args[i], name+"=") {
			return ""
		}
	}
	return value
}

// g3ProcessRole 只识别本轮二进制及其预期参数；同路径但身份不明时拒绝操作。
func g3ProcessRole(exe string, args []string, c g3GuardConfig) (string, error) {
	if exe != c.test && exe != c.agent && exe != c.manager {
		return "", nil
	}
	if len(args) == 0 || args[0] != exe {
		return "", fmt.Errorf("binary %s has an unexpected argv[0]", exe)
	}
	if exe == c.test {
		for _, arg := range args[1:] {
			if arg == "-test.run=^TestG3$" {
				return "test", nil
			}
		}
		// 守护检查自身和其他 go test 调用不应被终止。
		return "", nil
	}
	var stateRoot string
	if exe == c.agent {
		state := g3Arg(args, "-state-dir")
		if filepath.Base(state) != "state" {
			return "", fmt.Errorf("agent state path does not match")
		}
		stateRoot = filepath.Dir(state)
	} else {
		token := g3Arg(args, "-token-file")
		if filepath.Base(token) != "controller-token" {
			return "", fmt.Errorf("manager token path does not match")
		}
		stateRoot = filepath.Dir(token)
	}
	if filepath.Dir(stateRoot) != c.evidence || !strings.HasPrefix(filepath.Base(stateRoot), "g3-state-") {
		return "", fmt.Errorf("state path is not owned by this run")
	}
	if exe == c.agent {
		return "agent", nil
	}
	return "manager", nil
}

func g3PidfdExited(fd int, timeout time.Duration) (bool, error) {
	items := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	count, err := unix.Poll(items, int(timeout.Milliseconds()))
	if err != nil || count == 0 {
		return false, err
	}
	if items[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0 {
		return true, nil
	}
	return false, fmt.Errorf("unexpected pidfd poll events: %#x", items[0].Revents)
}

// pidfd 在读取 /proc 前绑定 PID；校验期间身份不会因 PID 复用而指向另一进程。
func g3StopMatching(c g3GuardConfig, wanted string) (int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	stopped := 0
	var problems []string
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || pid == os.Getpid() {
			continue
		}
		fd, err := unix.PidfdOpen(pid, 0)
		if errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			problems = append(problems, fmt.Sprintf("pidfd_open(%d): %v", pid, err))
			continue
		}
		func() {
			defer unix.Close(fd)
			if exited, err := g3PidfdExited(fd, 0); err != nil || exited {
				if err != nil {
					problems = append(problems, err.Error())
				}
				return
			}
			proc := filepath.Join("/proc", entry.Name())
			exe, err := os.Readlink(filepath.Join(proc, "exe"))
			if errors.Is(err, os.ErrNotExist) {
				return
			}
			if err != nil {
				problems = append(problems, fmt.Sprintf("read exe %d: %v", pid, err))
				return
			}
			if strings.HasSuffix(exe, " (deleted)") {
				original := strings.TrimSuffix(exe, " (deleted)")
				if original == c.test || original == c.agent || original == c.manager {
					problems = append(problems, fmt.Sprintf("PID %d executable was deleted; identity uncertain", pid))
				}
				return
			}
			if exe != c.test && exe != c.agent && exe != c.manager {
				return
			}
			cmdline, err := os.ReadFile(filepath.Join(proc, "cmdline"))
			if err != nil {
				problems = append(problems, fmt.Sprintf("read args %d: %v", pid, err))
				return
			}
			args := strings.Split(string(bytes.TrimSuffix(cmdline, []byte{0})), "\x00")
			role, err := g3ProcessRole(exe, args, c)
			if err != nil {
				problems = append(problems, fmt.Sprintf("PID %d: %v", pid, err))
				return
			}
			if role != wanted {
				return
			}
			if err = unix.PidfdSendSignal(fd, unix.SIGTERM, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
				problems = append(problems, fmt.Sprintf("signal %s PID %d: %v", role, pid, err))
				return
			}
			exited, err := g3PidfdExited(fd, 5*time.Second)
			if err != nil {
				problems = append(problems, fmt.Sprintf("wait %s PID %d: %v", role, pid, err))
				return
			}
			if !exited {
				if err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
					problems = append(problems, fmt.Sprintf("kill %s PID %d: %v", role, pid, err))
					return
				}
				exited, err = g3PidfdExited(fd, 5*time.Second)
				if err != nil || !exited {
					problems = append(problems, fmt.Sprintf("%s PID %d did not exit: %v", role, pid, err))
					return
				}
			}
			fmt.Printf("G3 guard stopped %s PID %d\n", role, pid)
			stopped++
		}()
	}
	if len(problems) > 0 {
		return stopped, errors.New(strings.Join(problems, "; "))
	}
	return stopped, nil
}

// 由入口脚本在测试进程退出或被中断后单独运行；未发现残留也要实际扫描。
func TestG3ProcessGuard(t *testing.T) {
	if os.Getenv("G3_PROCESS_GUARD") != "1" {
		t.Skip("only executed by the G3 entry script")
	}
	c := g3GuardConfig{test: os.Getenv("G3_PROCESS_TEST_BINARY"), agent: os.Getenv("G3_AGENT_BINARY"), manager: os.Getenv("G3_MANAGER_BINARY"), evidence: os.Getenv("G3_EVIDENCE_DIR")}
	for _, path := range []string{c.test, c.agent, c.manager, c.evidence} {
		if !filepath.IsAbs(path) {
			t.Fatal("guard paths must be absolute")
		}
	}
	// 先停止测试本体，避免扫描期间继续创建子进程，再检查 Agent 和 manager。
	total := 0
	for _, role := range []string{"test", "agent", "manager"} {
		count, err := g3StopMatching(c, role)
		total += count
		if err != nil {
			t.Errorf("%s process identity or cleanup uncertain: %v", role, err)
		}
	}
	if total > 0 {
		t.Errorf("%d leftover test processes required termination", total)
	}
}

func TestG3GuardIdentity(t *testing.T) {
	c := g3GuardConfig{test: "/tmp/run/test", agent: "/tmp/run/mini-agent", manager: "/tmp/run/mini-manager", evidence: "/tmp/evidence"}
	for _, test := range []struct {
		name, exe string
		args      []string
		role      string
		bad       bool
	}{
		{"own agent", c.agent, []string{c.agent, "-state-dir", "/tmp/evidence/g3-state-123/state"}, "agent", false},
		{"other state", c.agent, []string{c.agent, "-state-dir", "/tmp/other/state"}, "", true},
		{"manager", c.manager, []string{c.manager, "-token-file", "/tmp/evidence/g3-state-123/controller-token"}, "manager", false},
		{"other binary", "/tmp/other/mini-agent", []string{"/tmp/other/mini-agent"}, "", false},
		{"guard itself", c.test, []string{c.test, "-test.run=^TestG3ProcessGuard$"}, "", false},
		{"test runner", c.test, []string{c.test, "-test.run=^TestG3$"}, "test", false},
		{"spoof argv", c.agent, []string{"mini-agent", "-state-dir", "/tmp/evidence/g3-state-123/state"}, "", true},
		{"duplicate state flag", c.agent, []string{c.agent, "-state-dir", "/tmp/evidence/g3-state-123/state", "-state-dir", "/tmp/other/state"}, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			role, err := g3ProcessRole(test.exe, test.args, c)
			if role != test.role || (err != nil) != test.bad {
				t.Fatalf("got role %q, error %v", role, err)
			}
		})
	}
}
