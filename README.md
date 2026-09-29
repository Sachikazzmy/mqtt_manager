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

## MQTT 上报标准（v1）

完成 MQTT 接入后，设备只能通过下面的 Topic 和 JSON 载荷进入业务接收流程。JSON 的字段顺序和空白不重要，但字段名、字段类型、允许的指标和业务规则必须符合本节；MQTT 回调不得绕过 `telemetry.Receive` 直接写入存储。

### Topic

```text
factory/{device_id}/telemetry
```

后端订阅 `factory/+/telemetry`。设备发布时的 `{device_id}` 必须与 JSON 中的 `device_id` 一致。其他 Topic、通配符 Topic 或无法解析出唯一设备编号的消息都拒绝，不进入遥测处理。

### 载荷

载荷编码为 UTF-8 JSON 对象，最大 64 KiB。标准示例：

```json
{
  "version": "1",
  "device_id": "device-001",
  "message_id": "01J8V3Y7Q5M4K2N6P8R0S1T2U3",
  "sampled_at": "2026-09-29T04:15:30.123Z",
  "metrics": {
    "temperature": {"value": 23.6, "unit": "C"},
    "pressure": {"value": 101.3, "unit": "kPa"},
    "current": {"value": 2.5, "unit": "A"}
  }
}
```

字段规则：

| 字段 | 类型和要求 |
| --- | --- |
| `version` | 必填字符串，只允许 `"1"`。 |
| `device_id` | 必填非空字符串，最多 64 字节，必须与 Topic 中的设备编号一致。 |
| `message_id` | 必填非空字符串，最多 128 字节；同一设备跨重启不能重复，推荐使用设备持久化的 UUID 或 ULID。 |
| `sampled_at` | 必填 RFC3339 时间，必须带时区；设备端推荐统一发送 UTC 的 `Z`，后端按 UTC 保存。明显晚于服务端时间的消息拒绝，默认容差为 5 分钟。 |
| `metrics` | 必填非空对象，最多 64 项；每项只能是下表中的指标对象。可以只上报设备实际具备的一个或多个指标。 |

指标名称和单位是固定配对：

| 指标名称 | `unit` | `value` |
| --- | --- | --- |
| `temperature` | `C` | 有限 JSON 数字，`0` 合法 |
| `pressure` | `kPa` | 有限 JSON 数字，`0` 合法 |
| `current` | `A` | 有限 JSON 数字，`0` 合法 |

`value` 缺失、为 `null`、使用字符串表示、为 `NaN`/无穷大或 `unit` 不匹配时拒绝。当前默认不假设硬件量程；部署配置量程后，超出配置范围的值也拒绝。

消息中不得出现未定义字段，不得把密钥、摘要、接收时间或设备配置放入 JSON。密钥由 MQTT 认证或其他受控的设备认证流程单独提供给后端，不能通过 Topic、JSON 或日志传递。

### MQTT 回调的唯一入口

MQTT 适配层应只做以下工作：解析并校验 Topic、取得已认证设备对应的密钥、建立带超时的 `context`，然后调用：

```go
err := telemetryService.Receive(ctx, topicDeviceID, deviceSecret, payload)
```

`Receive` 成功后才视为业务接收成功；它会继续检查设备存在、密钥、启用状态、Topic 与载荷身份一致性、载荷格式、时间和去重。失败消息不得写入历史或 Latest。重复的 `(device_id, message_id)` 不产生第二条历史记录，也不更新状态。MQTT 层不得另写一套 JSON 解析或直接调用 `storage.Commit`。

因此，下面这些情况都不会进入存储：Topic 不符合格式、Topic 与 `device_id` 不一致、缺字段或多字段、未知指标、单位错误、数值非法、消息过大、时间明显超前、未知或禁用设备、密钥错误以及重复消息。

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
