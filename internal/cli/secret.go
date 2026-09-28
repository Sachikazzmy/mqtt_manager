package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
)

var errSecretInputInterrupted = errors.New("密钥输入被中断")

type SecretReader interface {
	ReadSecret() (string, error)
}

type SecretReaderFunc func() (string, error)

func (f SecretReaderFunc) ReadSecret() (string, error) {
	return f()
}

type terminalState struct {
	tty      *os.File
	original string

	mu       sync.Mutex
	restored bool
}

func captureTerminalState(tty *os.File) (*terminalState, error) {
	var output bytes.Buffer
	command := exec.Command("stty", "-g")
	command.Stdin = tty
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("读取终端状态失败: %w", err)
	}
	original := strings.TrimSpace(output.String())
	if original == "" {
		return nil, fmt.Errorf("读取终端状态失败: 输出为空")
	}
	return &terminalState{tty: tty, original: original}, nil
}

func (s *terminalState) disableEcho() error {
	return runStty(s.tty, "-echo")
}

func (s *terminalState) restore() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restored {
		return nil
	}
	if err := runStty(s.tty, s.original); err != nil {
		return err
	}
	s.restored = true
	return nil
}

func runStty(tty *os.File, args ...string) error {
	command := exec.Command("stty", args...)
	command.Stdin = tty
	return command.Run()
}

// NewTerminalSecretReader 使用控制终端读取密钥，并保存、恢复完整的原始终端状态。
func NewTerminalSecretReader(out io.Writer) SecretReader {
	return SecretReaderFunc(func() (secret string, err error) {
		tty, openErr := os.OpenFile("/dev/tty", os.O_RDWR, 0)
		closeTTY := openErr == nil
		if openErr != nil {
			// 受限容器可能不能打开 /dev/tty；交互式标准输入仍可关闭回显。
			tty = os.Stdin
		}
		if closeTTY {
			defer tty.Close()
		}

		state, err := captureTerminalState(tty)
		if err != nil {
			return "", err
		}
		defer func() {
			if restoreErr := state.restore(); restoreErr != nil {
				restoreErr = fmt.Errorf("恢复终端状态失败: %w", restoreErr)
				if err == nil {
					err = restoreErr
				} else {
					err = errors.Join(err, restoreErr)
				}
			}
		}()

		interrupts := make(chan os.Signal, 1)
		signal.Notify(interrupts, os.Interrupt)
		defer signal.Stop(interrupts)

		if _, err := fmt.Fprint(out, "密钥（输入不回显）: "); err != nil {
			return "", err
		}
		if err := state.disableEcho(); err != nil {
			select {
			case <-interrupts:
				return "", handleSecretInterrupt(state, interrupts)
			default:
			}
			return "", fmt.Errorf("关闭终端回显失败: %w", err)
		}

		result := make(chan secretResult, 1)
		go func() {
			value, readErr := readSecretLine(tty)
			result <- secretResult{value: value, err: readErr}
		}()

		select {
		case readResult := <-result:
			return readResult.value, readResult.err
		case <-interrupts:
			return "", handleSecretInterrupt(state, interrupts)
		}
	})
}

func handleSecretInterrupt(state *terminalState, interrupts chan os.Signal) error {
	if restoreErr := state.restore(); restoreErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "恢复终端状态失败: %v\n", restoreErr)
	}
	signal.Stop(interrupts)
	if process, findErr := os.FindProcess(os.Getpid()); findErr == nil {
		// 恢复后交回默认 SIGINT 行为，通常以 130 退出。
		if signalErr := process.Signal(os.Interrupt); signalErr == nil {
			return errSecretInputInterrupted
		}
	}
	return errSecretInputInterrupted
}

type secretResult struct {
	value string
	err   error
}

func readSecretLine(tty *os.File) (string, error) {
	const maxSecretBytes = 4096
	secret := make([]byte, 0, 64)
	one := make([]byte, 1)
	for {
		n, err := tty.Read(one)
		if n > 0 {
			if one[0] == '\n' {
				return strings.TrimSpace(string(secret)), nil
			}
			secret = append(secret, one[0])
			if len(secret) > maxSecretBytes {
				return "", fmt.Errorf("读取设备密钥失败: 输入超过 %d 字节", maxSecretBytes)
			}
		}
		if err != nil {
			return "", fmt.Errorf("读取设备密钥失败: %w", err)
		}
	}
}
