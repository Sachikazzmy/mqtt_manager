package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"Project/internal/cli"
	"Project/internal/command"
	"Project/internal/device"
	"Project/internal/receive"
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
	})
	return cli.New(devices, telemetryService, out, secretReader, command.NewService(devices, store)), devices, telemetryService
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
	})
	secret, err := devices.Create(ctx, "device-001", "test")
	if err != nil {
		t.Fatal(err)
	}
	reader := cli.SecretReaderFunc(func() (string, error) { return secret, nil })
	var out bytes.Buffer
	commands := cli.New(devices, telemetryService, &out, reader)

	payload := `{"version":"1","device_id":"device-001","message_id":"m-1","sampled_at":"2026-09-27T11:59:00Z","metrics":{"segment-1":{"value":0,"unit":"V"}}}`
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
	if len(samples) != 1 || samples[0].Metrics["segment-1"].Value != 0 {
		t.Fatalf("历史记录 = %#v", samples)
	}
}

func TestMetricManagementCommandIsRemovedAndDeviceOutputHidesDefinitions(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	commands, devices, _ := newCommands(&out, nil)
	if _, err := devices.Create(ctx, "device-001", "测试设备"); err != nil {
		t.Fatal(err)
	}

	if _, err := commands.Execute(ctx, "metric list device-001"); err == nil || !strings.Contains(err.Error(), "未知命令") {
		t.Fatalf("指标定义管理暂不提供 CLI 命令: %v", err)
	}
	if strings.Contains(cli.Help, "metric ") {
		t.Fatalf("帮助中不应出现延期的指标管理命令: %s", cli.Help)
	}
	if _, err := commands.Execute(ctx, "list"); err != nil {
		t.Fatal(err)
	}
	output := out.String()
	if strings.Contains(output, "metric_definitions") || strings.Contains(output, "temperature") || strings.Contains(output, "pressure") || strings.Contains(output, "current") {
		t.Fatalf("新设备 list 输出不应包含定义管理字段或旧默认项: %s", output)
	}
}

func TestCommandCLISetClearGetAndBoundedHistory(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	commands, devices, telemetryService := newCommands(&out, nil)
	secret, err := devices.Create(ctx, "linux-01", "测试设备")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"version":"1","device_id":"linux-01","message_id":"first","sampled_at":"2026-09-27T11:59:00Z","metrics":{"segment-1":{"value":23.6,"unit":"V","modifiable":true},"segment-2":{"value":0,"unit":"V","modifiable":true},"segment-3":{"value":1,"unit":"V"}}}`)
	if err := telemetryService.Receive(ctx, "linux-01", secret, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := commands.Execute(ctx, "set linux-01 segment-1 120"); err != nil {
		t.Fatal(err)
	}
	var setEntry command.Command
	if err := json.Unmarshal(out.Bytes(), &setEntry); err != nil {
		t.Fatalf("set 应返回 command_id 和状态 JSON: %v / %s", err, out.String())
	}
	if setEntry.CommandID == "" || setEntry.Status != command.StatusWaitingToSend || setEntry.Value == nil || *setEntry.Value != 120 {
		t.Fatalf("set CLI 返回值 = %#v", setEntry)
	}
	out.Reset()
	if _, err := commands.Execute(ctx, "command linux-01 "+setEntry.CommandID); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), setEntry.CommandID) || !strings.Contains(out.String(), command.StatusWaitingToSend) {
		t.Fatalf("command 应查询单条状态: %s", out.String())
	}
	out.Reset()
	if _, err := commands.Execute(ctx, "clear linux-01 segment-2"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), `"value"`) {
		t.Fatalf("clear 命令不应包含 value: %s", out.String())
	}
	out.Reset()
	if _, err := commands.Execute(ctx, "commands linux-01 10"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), setEntry.CommandID) || !strings.Contains(out.String(), command.StatusWaitingToSend) {
		t.Fatalf("commands 应返回有限历史: %s", out.String())
	}
	if _, err := commands.Execute(ctx, "commands linux-01 101"); !errors.Is(err, command.ErrCommandLimit) {
		t.Fatalf("CLI 必须限制命令历史数量: %v", err)
	}
	if _, err := commands.Execute(ctx, "set linux-01 segment-3 10"); !errors.Is(err, command.ErrMetricNotMutable) {
		t.Fatalf("不可修改指标应拒绝: %v", err)
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

func TestPrintEventShowsMQTTResultAndBrokerState(t *testing.T) {
	var out bytes.Buffer
	commands := cli.New(nil, nil, &out, nil)
	events := []receive.Event{
		{
			Type:      "accepted",
			DeviceID:  "device-001",
			MessageID: "message-001",
			Metrics: map[string]telemetry.MetricValue{
				"segment-1": {Value: 0, Unit: "V"},
			},
		},
		{Type: "duplicate", DeviceID: "device-001", MessageID: "message-001"},
		{Type: "reject", DeviceID: "device-001", MessageID: "message-002", Reason: "协议错误"},
		{Type: "broker", Status: "offline", Reason: "连接中断"},
	}
	for _, event := range events {
		if err := commands.PrintEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range []string{
		"接收成功 device_id=device-001 message_id=message-001",
		"segment-1=0V",
		"重复消息 device_id=device-001 message_id=message-001",
		"拒收 device_id=device-001 message_id=message-002 reason=协议错误",
		"[MQTT] 状态=offline 连接中断",
	} {
		if !strings.Contains(out.String(), expected) {
			t.Errorf("终端事件输出缺少 %q:\n%s", expected, out.String())
		}
	}
}
