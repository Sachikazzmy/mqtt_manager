package cli_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"Project/internal/cli"
	"Project/internal/device"
)

func TestCommandLifecycle(t *testing.T) {
	repo := device.NewMemoryRepository()
	service := device.NewService(repo)
	var out bytes.Buffer
	commands := cli.New(service, &out)
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

	if _, err := service.Get(ctx, "device-001"); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("删除后查询错误 = %v，期望包含 %v", err, device.ErrNotFound)
	}
}

func TestInvalidCommandDoesNotStopCLI(t *testing.T) {
	service := device.NewService(device.NewMemoryRepository())
	commands := cli.New(service, &bytes.Buffer{})

	quit, err := commands.Execute(context.Background(), "unknown")
	if err == nil {
		t.Fatal("未知命令应该返回错误")
	}
	if quit {
		t.Fatal("普通命令错误不应该退出 CLI")
	}
}

func TestQuit(t *testing.T) {
	service := device.NewService(device.NewMemoryRepository())
	commands := cli.New(service, &bytes.Buffer{})

	quit, err := commands.Execute(context.Background(), "quit")
	if err != nil {
		t.Fatal(err)
	}
	if !quit {
		t.Fatal("quit 应该要求调用方退出")
	}
}
