package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"Project/internal/cli"
	"Project/internal/device"
	"Project/internal/receive"
	"Project/internal/storage"
	"Project/internal/telemetry"
	"golang.org/x/term"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "运行失败:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := storage.NewMemoryStore()
	telemetryService := telemetry.NewService(store)
	brokerConfig, configErr := receive.LoadConfigFromEnv()
	var broker device.BrokerLifecycle
	var receiver *receive.Receiver
	if configErr != nil {
		broker = receive.NewUnavailableManager(configErr)
		fmt.Fprintln(os.Stderr, "MQTT 配置不可用，CLI 保持运行但 add/启停/删除/重置不会成功:", configErr)
	} else {
		manager := receive.NewManager(brokerConfig)
		broker = manager
		receiver, configErr = receive.NewReceiver(brokerConfig, telemetryService)
		if configErr != nil {
			broker = receive.NewUnavailableManager(configErr)
			fmt.Fprintln(os.Stderr, "MQTT 接收器配置失败，CLI 保持运行但 Broker 操作不可用:", configErr)
		} else {
			receiver.Start(ctx)
			defer receiver.Close()
		}
	}
	devices := device.NewManagedService(store, broker)
	commands := cli.New(devices, telemetryService, os.Stdout, cli.NewTerminalSecretReader(os.Stdout))

	fmt.Println(cli.Help)
	if receiver == nil {
		fmt.Println("[MQTT] 状态=offline；本机 CLI 可用，Broker 生命周期操作会失败")
		return runInput(ctx, stop, commands, nil, nil)
	}
	return runInput(ctx, stop, commands, receiver.Events(), receiver.DroppedEvents)
}

func runInput(ctx context.Context, stop context.CancelFunc, commands *cli.CLI, events <-chan receive.Event, dropped func() uint64) error {
	if term.IsTerminal(int(os.Stdin.Fd())) {
		return runTerminalInput(ctx, stop, commands, events, dropped)
	}
	return runPipedInput(ctx, commands, events, dropped)
}

type lineInput struct {
	line string
	err  error
	eof  bool
}

func runPipedInput(ctx context.Context, commands *cli.CLI, events <-chan receive.Event, dropped func() uint64) error {
	lines := make(chan lineInput, 1)
	continueReading := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), 8192)
		for scanner.Scan() {
			lines <- lineInput{line: scanner.Text()}
			<-continueReading
		}
		lines <- lineInput{err: scanner.Err(), eof: true}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-events:
			if ok {
				if err := commands.PrintEvent(event); err != nil {
					return err
				}
			} else {
				events = nil
			}
			printDroppedEvents(commands, dropped)
		case input := <-lines:
			if input.eof && input.err == nil {
				return nil
			}
			if input.err != nil {
				return fmt.Errorf("读取命令（每行上限约 8 KiB）: %w", input.err)
			}
			quit, err := commands.Execute(ctx, input.line)
			if err != nil {
				fmt.Fprintln(os.Stderr, "命令失败:", err)
			}
			if quit {
				return nil
			}
			printDroppedEvents(commands, dropped)
			continueReading <- struct{}{}
		}
	}
}

type terminalInput struct {
	value byte
	err   error
}

func runTerminalInput(ctx context.Context, stop context.CancelFunc, commands *cli.CLI, events <-chan receive.Event, dropped func() uint64) error {
	fd := int(os.Stdin.Fd())
	state, err := term.MakeRaw(fd)
	if err != nil {
		return fmt.Errorf("启用终端输入模式失败: %w", err)
	}
	defer func() {
		_ = term.Restore(fd, state)
	}()

	input := make(chan terminalInput, 1)
	permitRead := make(chan struct{})
	go func() {
		buffer := make([]byte, 1)
		for range permitRead {
			n, err := os.Stdin.Read(buffer)
			if n > 0 {
				input <- terminalInput{value: buffer[0]}
			}
			if err != nil {
				input <- terminalInput{err: err}
				return
			}
		}
	}()

	var line []byte
	var escapeState byte
	fmt.Fprint(os.Stdout, "> ")
	permitRead <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stdout)
			return nil
		case event, ok := <-events:
			if ok {
				fmt.Fprint(os.Stdout, "\r\n")
				if err := commands.PrintEvent(event); err != nil {
					return err
				}
				printDroppedEvents(commands, dropped)
				redrawInput(line)
			} else {
				events = nil
			}
		case next := <-input:
			if next.err != nil {
				if errors.Is(next.err, io.EOF) {
					return nil
				}
				return fmt.Errorf("读取终端命令失败: %w", next.err)
			}
			continueInput := true
			if escapeState == 1 {
				escapeState = 2
			} else if escapeState == 2 {
				if next.value >= 0x40 && next.value <= 0x7e {
					escapeState = 0
				}
			} else {
				switch next.value {
				case 0x1b:
					escapeState = 1
				case '\r', '\n':
					continueInput = false
					fmt.Fprint(os.Stdout, "\r\n")
					if restoreErr := term.Restore(fd, state); restoreErr != nil {
						return fmt.Errorf("恢复终端输入模式失败: %w", restoreErr)
					}
					quit, commandErr := commands.Execute(ctx, string(line))
					if commandErr != nil {
						fmt.Fprintln(os.Stderr, "命令失败:", commandErr)
					}
					if quit {
						return nil
					}
					printDroppedEvents(commands, dropped)
					line = line[:0]
					state, err = term.MakeRaw(fd)
					if err != nil {
						return fmt.Errorf("恢复终端输入模式失败: %w", err)
					}
					fmt.Fprint(os.Stdout, "> ")
					permitRead <- struct{}{}
				case 0x03:
					continueInput = false
					_ = term.Restore(fd, state)
					fmt.Fprintln(os.Stdout)
					stop()
					return nil
				case 0x04:
					if len(line) == 0 {
						continueInput = false
						_ = term.Restore(fd, state)
						fmt.Fprintln(os.Stdout)
						return nil
					}
				case 0x7f, 0x08:
					if len(line) > 0 {
						line = removeLastRune(line)
						fmt.Fprint(os.Stdout, "\b \b")
					}
				default:
					line = append(line, next.value)
					_, _ = os.Stdout.Write([]byte{next.value})
				}
			}
			if continueInput {
				permitRead <- struct{}{}
			}
		}
	}
}

func redrawInput(line []byte) {
	fmt.Fprint(os.Stdout, "\r\033[2K> ")
	_, _ = os.Stdout.Write(line)
}

func removeLastRune(line []byte) []byte {
	for len(line) > 0 {
		line = line[:len(line)-1]
		if len(line) == 0 || line[len(line)]&0xc0 != 0x80 {
			return line
		}
	}
	return line
}

func printDroppedEvents(commands *cli.CLI, dropped func() uint64) {
	if dropped == nil {
		return
	}
	count := dropped()
	if count == 0 {
		return
	}
	_ = commands.PrintEvent(receive.Event{Type: "notice", Reason: fmt.Sprintf("终端事件缓冲已满，省略 %d 条显示；请用 get/list/history 查询当前状态", count)})
}
