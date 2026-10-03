// Package cli 将终端输入转换为设备业务操作，并负责展示操作结果。
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	commandpkg "Project/internal/command"
	"Project/internal/device"
	"Project/internal/receive"
	"Project/internal/telemetry"
)

const Help = `命令：
  add <编号> <名称>       新增设备（名称可包含空格）
  list                    查询全部设备
  get <编号>              查询单个设备
  update <编号> <新名称>  修改设备名称
  enable <编号>           启用设备
  disable <编号>          禁用设备
  delete <编号>           删除设备
  reset-secret <编号>     重置设备密钥（旧密钥立即失效）
  receive <编号> <文件>   从本地 JSON 文件模拟上报
  history <编号> <数量> [起始时间] [结束时间]
  set <编号> <指标> <数值> 设置可修改指标的模拟值
  clear <编号> <指标>     清除模拟覆盖
  command <编号> <ID>      查询单条命令状态
  commands <编号> <数量> 查看有限数量的命令历史（1 到 100）
  help                    显示帮助
  quit                    退出服务（数据库数据保留）`

type CLI struct {
	devices      *device.Service
	telemetry    *telemetry.Service
	commands     *commandpkg.Service
	secretReader SecretReader
	out          io.Writer
}

func New(devices *device.Service, telemetryService *telemetry.Service, out io.Writer, secretReader SecretReader, commandServices ...*commandpkg.Service) *CLI {
	var commandService *commandpkg.Service
	if len(commandServices) > 0 {
		commandService = commandServices[0]
	}
	return &CLI{
		devices:      devices,
		telemetry:    telemetryService,
		commands:     commandService,
		secretReader: secretReader,
		out:          out,
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
		secret, err := c.devices.Create(ctx, id, rest)
		if err != nil {
			return false, err
		}
		_, err = fmt.Fprintf(c.out, "OK\n密钥（仅显示一次）: %s\n", secret)
		return false, err

	case "list":
		if args != "" {
			return false, fmt.Errorf("用法: list")
		}
		devices, err := c.devices.List(ctx)
		if err != nil {
			return false, err
		}
		return false, c.printJSON(devices)

	case "get":
		if id == "" || rest != "" {
			return false, fmt.Errorf("用法: get <编号>")
		}
		d, err := c.devices.Get(ctx, id)
		if err != nil {
			return false, err
		}
		return false, c.printJSON(d)

	case "update":
		if id == "" || rest == "" {
			return false, fmt.Errorf("用法: update <编号> <新名称>")
		}
		if err := c.devices.Update(ctx, id, rest); err != nil {
			return false, err
		}

	case "enable":
		if id == "" || rest != "" {
			return false, fmt.Errorf("用法: enable <编号>")
		}
		if err := c.devices.Enable(ctx, id); err != nil {
			return false, err
		}

	case "disable":
		if id == "" || rest != "" {
			return false, fmt.Errorf("用法: disable <编号>")
		}
		if err := c.devices.Disable(ctx, id); err != nil {
			return false, err
		}

	case "delete":
		if id == "" || rest != "" {
			return false, fmt.Errorf("用法: delete <编号>")
		}
		if err := c.devices.Delete(ctx, id); err != nil {
			return false, err
		}

	case "reset-secret":
		if id == "" || rest != "" {
			return false, fmt.Errorf("用法: reset-secret <编号>")
		}
		secret, err := c.devices.ResetSecret(ctx, id)
		if err != nil {
			return false, err
		}
		_, err = fmt.Fprintf(c.out, "OK\n密钥（仅显示一次）: %s\n", secret)
		return false, err

	case "receive":
		if id == "" || rest == "" {
			return false, fmt.Errorf("用法: receive <编号> <文件>")
		}
		payload, err := readPayload(rest)
		if err != nil {
			return false, err
		}
		if c.secretReader == nil {
			return false, fmt.Errorf("未配置密钥输入")
		}
		secret, err := c.secretReader.ReadSecret()
		if err != nil {
			return false, err
		}
		if err := c.telemetry.Receive(ctx, id, secret, payload); err != nil {
			return false, err
		}

	case "history":
		query, err := parseHistoryQuery(args)
		if err != nil {
			return false, err
		}
		samples, err := c.telemetry.History(ctx, query)
		if err != nil {
			return false, err
		}
		return false, c.printJSON(samples)

	case "set":
		fields := strings.Fields(args)
		if len(fields) != 3 {
			return false, fmt.Errorf("用法: set <编号> <指标> <数值>")
		}
		if c.commands == nil {
			return false, fmt.Errorf("命令服务未配置")
		}
		value, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return false, fmt.Errorf("数值必须是有限数字: %w", err)
		}
		entry, err := c.commands.SetMetric(ctx, fields[0], fields[1], value)
		if err != nil {
			return false, err
		}
		return false, c.printJSON(entry)

	case "clear":
		fields := strings.Fields(args)
		if len(fields) != 2 {
			return false, fmt.Errorf("用法: clear <编号> <指标>")
		}
		if c.commands == nil {
			return false, fmt.Errorf("命令服务未配置")
		}
		entry, err := c.commands.ClearOverride(ctx, fields[0], fields[1])
		if err != nil {
			return false, err
		}
		return false, c.printJSON(entry)

	case "command":
		fields := strings.Fields(args)
		if len(fields) != 2 {
			return false, fmt.Errorf("用法: command <编号> <command_id>")
		}
		if c.commands == nil {
			return false, fmt.Errorf("命令服务未配置")
		}
		entry, err := c.commands.Get(ctx, fields[0], fields[1])
		if err != nil {
			return false, err
		}
		return false, c.printJSON(entry)

	case "commands":
		fields := strings.Fields(args)
		if len(fields) != 2 {
			return false, fmt.Errorf("用法: commands <编号> <数量>")
		}
		if c.commands == nil {
			return false, fmt.Errorf("命令服务未配置")
		}
		limit, err := strconv.Atoi(fields[1])
		if err != nil {
			return false, fmt.Errorf("命令历史数量必须是 1 到 %d 的整数", commandpkg.MaxHistoryLimit)
		}
		entries, err := c.commands.History(ctx, fields[0], limit)
		if err != nil {
			return false, err
		}
		return false, c.printJSON(entries)

	default:
		return false, fmt.Errorf("未知命令 %q，输入 help 查看帮助", command)
	}

	_, err := fmt.Fprintln(c.out, "OK")
	return false, err
}

func readPayload(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开上报文件失败: %w", err)
	}
	defer file.Close()

	// 多读一个字节即可判断超限，避免把特殊文件或超大文件完整读入内存。
	payload, err := io.ReadAll(io.LimitReader(file, int64(telemetry.MaxPayloadBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("读取上报文件失败: %w", err)
	}
	if len(payload) > telemetry.MaxPayloadBytes {
		return nil, fmt.Errorf("上报文件超过大小限制：最大允许 %d 字节: %w", telemetry.MaxPayloadBytes, telemetry.ErrPayloadTooLarge)
	}
	return payload, nil
}

func parseHistoryQuery(args string) (telemetry.HistoryQuery, error) {
	fields := strings.Fields(args)
	if len(fields) < 2 || len(fields) > 4 {
		return telemetry.HistoryQuery{}, fmt.Errorf("用法: history <编号> <数量> [起始时间] [结束时间]")
	}
	limit, err := strconv.Atoi(fields[1])
	if err != nil || limit <= 0 {
		return telemetry.HistoryQuery{}, fmt.Errorf("历史数量必须是大于 0 的整数")
	}
	query := telemetry.HistoryQuery{DeviceID: fields[0], Limit: limit}
	if len(fields) >= 3 {
		from, err := time.Parse(time.RFC3339, fields[2])
		if err != nil {
			return telemetry.HistoryQuery{}, fmt.Errorf("起始时间必须是 RFC3339: %w", err)
		}
		query.From = &from
	}
	if len(fields) == 4 {
		to, err := time.Parse(time.RFC3339, fields[3])
		if err != nil {
			return telemetry.HistoryQuery{}, fmt.Errorf("结束时间必须是 RFC3339: %w", err)
		}
		query.To = &to
	}
	return query, nil
}

func (c *CLI) printJSON(value any) error {
	encoder := json.NewEncoder(c.out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func (c *CLI) PrintEvent(event receive.Event) error {
	switch event.Type {
	case "broker":
		if event.Reason == "" {
			_, err := fmt.Fprintf(c.out, "[MQTT] 状态=%s\n", event.Status)
			return err
		}
		_, err := fmt.Fprintf(c.out, "[MQTT] 状态=%s %s\n", event.Status, event.Reason)
		return err
	case "accepted":
		_, err := fmt.Fprintf(c.out, "[MQTT] 接收成功 device_id=%s message_id=%s metrics=%s\n", event.DeviceID, event.MessageID, formatMetrics(event.Metrics))
		return err
	case "duplicate":
		_, err := fmt.Fprintf(c.out, "[MQTT] 重复消息 device_id=%s message_id=%s\n", event.DeviceID, event.MessageID)
		return err
	case "command_result":
		_, err := fmt.Fprintf(c.out, "[MQTT] 设备命令结果 device_id=%s command_id=%s status=%s（遥测 Latest 需由后续采样确认）\n", event.DeviceID, event.CommandID, event.CommandStatus)
		return err
	case "reject":
		_, err := fmt.Fprintf(c.out, "[MQTT] 拒收 device_id=%s message_id=%s reason=%s\n", event.DeviceID, event.MessageID, event.Reason)
		return err
	case "retry":
		_, err := fmt.Fprintf(c.out, "[MQTT] 暂未提交 device_id=%s message_id=%s reason=%s\n", event.DeviceID, event.MessageID, event.Reason)
		return err
	case "queue_full", "notice":
		_, err := fmt.Fprintf(c.out, "[MQTT] %s device_id=%s message_id=%s\n", event.Reason, event.DeviceID, event.MessageID)
		return err
	default:
		return nil
	}
}

func formatMetrics(metrics map[string]telemetry.MetricValue) string {
	names := make([]string, 0, len(metrics))
	for name := range metrics {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, 0, len(names))
	for _, name := range names {
		metric := metrics[name]
		values = append(values, fmt.Sprintf("%s=%.6g%s", name, metric.Value, metric.Unit))
	}
	return strings.Join(values, ",")
}
