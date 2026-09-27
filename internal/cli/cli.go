// Package cli 将终端输入转换为设备业务操作，并负责展示操作结果。
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"Project/internal/device"
)

const Help = `命令：
  add <编号> <名称>       新增设备（名称可包含空格）
  list                    查询全部设备
  get <编号>              查询单个设备
  update <编号> <新名称>  修改设备名称
  delete <编号>           删除设备
  help                    显示帮助
  quit                    退出（内存数据会丢失）`

type CLI struct {
	service *device.Service
	out     io.Writer
}

func New(service *device.Service, out io.Writer) *CLI {
	return &CLI{
		service: service,
		out:     out,
	}
}

// word 只拆出第一个单词，使后面的设备名称可以包含空格。
func word(input string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(input), " ", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.TrimSpace(parts[1])
}

// Execute 执行一行命令。返回 true 表示调用方应该退出命令循环。
func (c *CLI) Execute(ctx context.Context, line string) (bool, error) {
	command, args := word(line)
	if command == "" {
		return false, nil
	}

	id, rest := word(args)

	switch command {
	case "quit":
		if args != "" {
			return false, fmt.Errorf("用法: quit")
		}
		return true, nil

	case "help":
		if args != "" {
			return false, fmt.Errorf("用法: help")
		}
		_, err := fmt.Fprintln(c.out, Help)
		return false, err

	case "add":
		if id == "" || rest == "" {
			return false, fmt.Errorf("用法: add <编号> <名称>")
		}
		if err := c.service.Create(ctx, id, rest); err != nil {
			return false, err
		}

	case "list":
		if args != "" {
			return false, fmt.Errorf("用法: list")
		}
		devices, err := c.service.List(ctx)
		if err != nil {
			return false, err
		}
		return false, c.printJSON(devices)

	case "get":
		if id == "" || rest != "" {
			return false, fmt.Errorf("用法: get <编号>")
		}
		d, err := c.service.Get(ctx, id)
		if err != nil {
			return false, err
		}
		return false, c.printJSON(d)

	case "update":
		if id == "" || rest == "" {
			return false, fmt.Errorf("用法: update <编号> <新名称>")
		}
		if err := c.service.Update(ctx, id, rest); err != nil {
			return false, err
		}

	case "delete":
		if id == "" || rest != "" {
			return false, fmt.Errorf("用法: delete <编号>")
		}
		if err := c.service.Delete(ctx, id); err != nil {
			return false, err
		}

	default:
		return false, fmt.Errorf("未知命令 %q，输入 help 查看帮助", command)
	}

	_, err := fmt.Fprintln(c.out, "OK")
	return false, err
}

func (c *CLI) printJSON(value any) error {
	encoder := json.NewEncoder(c.out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
