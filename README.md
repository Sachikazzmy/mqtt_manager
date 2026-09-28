# Project01 阶段 2

本阶段完成设备认证、遥测接收、内存历史保存和设备 Latest 状态闭环。当前只支持本机 CLI 模拟，不接入 HTTP、MQTT、PostgreSQL 或 Docker。

## 运行

```sh
go run ./cmd/server
```

CLI 启动后，设备配置、凭据和遥测数据共用一个内存存储实例。退出进程后数据全部丢失。

常用操作：

```text
add device-001 一号温度设备
list
get device-001
update device-001 新名称
disable device-001
enable device-001
reset-secret device-001
receive device-001 ./sample.json
history device-001 20
history device-001 20 2026-09-27T00:00:00Z 2026-09-28T00:00:00Z
delete device-001
quit
```

`add` 和 `reset-secret` 成功后只展示一次新密钥。`receive` 会在读取文件并确认不超过 64 KiB 后，从控制终端读取密钥并关闭回显；密钥不放在命令参数、遥测 JSON、Latest 或历史记录中。正常输入、EOF 和 Ctrl+C 都会恢复进入密钥输入前保存的完整终端状态。普通 `get` 和 `list` 不返回密钥或密钥摘要。

## 遥测消息

`receive` 使用下面的 JSON 格式。文件只保存消息，不保存密钥。

```json
{
  "version": "1",
  "device_id": "device-001",
  "message_id": "boot-20260927-0001",
  "sampled_at": "2026-09-27T12:00:00Z",
  "metrics": {
    "temperature": {"value": 0, "unit": "C"},
    "pressure": {"value": 101.3, "unit": "kPa"},
    "current": {"value": 2.5, "unit": "A"}
  }
}
```

支持的指标和单位是 `temperature/C`、`pressure/kPa`、`current/A`。首版默认没有写死硬件量程，只要求数值为有限数；`MetricRule` 可以为部署方配置单位、最小值和最大值。默认未来采样时间容差为 5 分钟，超出会拒收。采样时间和服务端接收时间均按 UTC 保存。

必填字段、消息大小和长度都会校验。`value: 0` 是合法值，缺少 `value` 会拒收。`message_id` 必须在同一设备重启后仍保持唯一，去重键为 `(device_id, message_id)`。

## 状态语义

设备创建后 `latest` 为 `null`。收到合法且非重复消息后，设备对象中的 `latest` 保存该条消息的完整指标快照，包含指标、采样时间、接收时间、最后有效接收时间和消息 ID；它不会把不同消息的指标合并。

历史会保存每条合法的非重复消息。乱序旧采样仍进入历史并可推进 `last_valid_received_at`，但不会覆盖较新的 Latest。Latest 的决胜顺序是：采样时间较新优先；采样时间相同则接收时间较新优先；两个时间都相同则消息 ID 字典序较大优先。重复或拒收消息不会推进最后有效接收时间。`enabled` 只表示是否允许接收，不代表设备在线，本阶段不做离线判定。

删除设备会删除配置、凭据和 Latest，但保留历史。一个进程内已删除的设备 ID 不允许重新创建，以避免历史混淆；由于本阶段是纯内存存储，进程退出后包括这个禁止复用记录在内的所有数据都会丢失。

历史查询必须指定正数数量限制，最多返回 1000 条；未指定时间时默认查询最近 24 小时，单次时间范围最多 7 天。可选设备 ID、起始时间和结束时间。返回超过限制时只保留符合条件的最新记录，再按采样时间升序返回；查询期间会响应 context 取消。

## 依赖与数据流

```text
cmd/server
  -> cli
  -> device.Service
  -> telemetry.Service
  -> storage.MemoryStore

本地 JSON 文件 + 独立密钥参数
  -> telemetry.Receive
  -> 校验、规范化
  -> storage.MemoryStore.Commit（同一把锁）
  -> 去重 + 历史追加 + Device.Latest 更新
  -> device get/list 或 history 查询
```

`device` 只负责设备模型、配置生命周期和随机密钥生成；`telemetry` 负责协议解析、字段校验、时间规则和接收流程；`storage` 负责设备配置、密钥摘要、去重键、历史及 Latest 的内存保存。仓储查询返回独立副本，外部修改不会改变内部 map 或 Latest 指针。

设备后端只保存 SHA-256 验证摘要，接收时用恒定时间比较；密钥由 `crypto/rand` 生成。删除、禁用、改名、重置密钥与接收提交共享同一存储锁，避免校验完成后状态已经改变。

## 可复现验证

```sh
gofmt -w cmd/server/main.go internal/cli/*.go internal/device/*.go internal/storage/*.go internal/telemetry/*.go
go test ./...
go vet ./...
go test -race ./...
```

仓库不提供真实密钥示例。可以按下面步骤手工验证闭环：

1. 启动 `go run ./cmd/server`。
2. 执行 `add device-001 一号温度设备`，暂时保存终端显示的一次性密钥。
3. 创建只包含上述 JSON 字段的本地消息文件。
4. 执行 `receive device-001 <消息文件>`，在不回显提示下输入密钥。
5. 执行 `get device-001` 或 `list` 查看 `latest`，执行 `history device-001 20` 查看历史。
6. 执行 `reset-secret device-001`，确认旧密钥拒收、新密钥接收；执行 `delete device-001`，确认历史仍可查询且同一进程内不能重新创建该 ID。
