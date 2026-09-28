package cli_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"Project/internal/cli"
	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
)

func newCommands(out *bytes.Buffer, secretReader cli.SecretReader) (*cli.CLI, *device.Service, *telemetry.Service) {
	store := storage.NewMemoryStore()
	devices := device.NewService(store)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	telemetryService := telemetry.NewServiceWithClock(store, func() time.Time {
		return now
	}, telemetry.Config{
		MaxFutureSkew: 5 * time.Minute,
		MetricRules:   telemetry.DefaultMetricRules(),
	})
	return cli.New(devices, telemetryService, out, secretReader), devices, telemetryService
}

func TestCommandLifecycle(t *testing.T) {
	var out bytes.Buffer
	commands, service, _ := newCommands(&out, nil)
	ctx := context.Background()

	inputs := []string{
		"add device-001 车间 温度传感器",
		"get device-001",
		"update device-001 一号车间温度传感器",
		"list",
		"delete device-001",
	}

	for _, input := range inputs {
		quit, err := commands.Execute(ctx, input)
		if err != nil {
			t.Fatalf("执行 %q: %v", input, err)
		}
		if quit {
			t.Fatalf("命令 %q 意外退出", input)
		}
	}

	if !strings.Contains(out.String(), "一号车间温度传感器") {
		t.Fatalf("输出中没有修改后的设备名称:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "密钥（仅显示一次）:") {
		t.Fatalf("add 没有展示一次性密钥:\n%s", out.String())
	}

	if _, err := service.Get(ctx, "device-001"); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("删除后查询错误 = %v，期望包含 %v", err, device.ErrNotFound)
	}
}

func TestReceiveCommandUsesSecretReader(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	devices := device.NewService(store)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	telemetryService := telemetry.NewServiceWithClock(store, func() time.Time {
		return now
	}, telemetry.Config{
		MaxFutureSkew: 5 * time.Minute,
		MetricRules:   telemetry.DefaultMetricRules(),
	})
	secret, err := devices.Create(ctx, "device-001", "test")
	if err != nil {
		t.Fatal(err)
	}
	reader := cli.SecretReaderFunc(func() (string, error) { return secret, nil })
	var out bytes.Buffer
	commands := cli.New(devices, telemetryService, &out, reader)

	payload := `{"version":"1","device_id":"device-001","message_id":"m-1","sampled_at":"2026-09-27T11:59:00Z","metrics":{"temperature":{"value":0,"unit":"C"}}}`
	file := t.TempDir() + "/sample.json"
	if err := os.WriteFile(file, []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := commands.Execute(ctx, "receive device-001 "+file); err != nil {
		t.Fatal(err)
	}
	samples, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 || samples[0].Metrics["temperature"].Value != 0 {
		t.Fatalf("历史记录 = %#v", samples)
	}
}

func TestReceiveRejectsOversizedSpecialFileBeforeSecretInput(t *testing.T) {
	var secretCalls int
	reader := cli.SecretReaderFunc(func() (string, error) {
		secretCalls++
		return "unused", nil
	})
	commands, _, _ := newCommands(&bytes.Buffer{}, reader)
	result := make(chan error, 1)
	go func() {
		_, err := commands.Execute(context.Background(), "receive device-001 /dev/zero")
		result <- err
	}()

	select {
	case err := <-result:
		if !errors.Is(err, telemetry.ErrPayloadTooLarge) {
			t.Fatalf("超大文件错误 = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("超大特殊文件读取没有在大小上限处停止")
	}
	if secretCalls != 0 {
		t.Fatalf("超大文件不应触发密钥输入，调用次数 = %d", secretCalls)
	}
}

func TestInvalidCommandDoesNotStopCLI(t *testing.T) {
	commands, _, _ := newCommands(&bytes.Buffer{}, nil)

	quit, err := commands.Execute(context.Background(), "unknown")
	if err == nil {
		t.Fatal("未知命令应该返回错误")
	}
	if quit {
		t.Fatal("普通命令错误不应该退出 CLI")
	}
}

func TestQuit(t *testing.T) {
	commands, _, _ := newCommands(&bytes.Buffer{}, nil)

	quit, err := commands.Execute(context.Background(), "quit")
	if err != nil {
		t.Fatal(err)
	}
	if !quit {
		t.Fatal("quit 应该要求调用方退出")
	}
}
