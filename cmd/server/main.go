package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"Project/internal/cli"
	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "运行失败:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	// main 只负责创建对象并连接依赖。
	store := storage.NewMemoryStore()
	devices := device.NewService(store)
	telemetryService := telemetry.NewService(store)
	commands := cli.New(devices, telemetryService, os.Stdout, cli.NewTerminalSecretReader(os.Stdout))

	fmt.Println(cli.Help)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 8192)

	// 一条命令执行完成后，再读取下一条命令。
	for scanner.Scan() {
		quit, err := commands.Execute(ctx, scanner.Text())
		if err != nil {
			fmt.Fprintln(os.Stderr, "命令失败:", err)
			continue
		}
		if quit {
			return nil
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取命令（每行上限约 8 KiB）: %w", err)
	}

	return nil
}
