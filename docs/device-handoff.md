# 下位机接入交接（协议 v1）

本文说明上位机当前接受的遥测和模拟指标命令协议。所有 Topic 中的设备编号均来自已注册的 `device_id`；`linux-01` 和 `device-001` 只是示例，不得在固件中跨设备硬编码。

本阶段的命令只用于设备侧模拟指标覆盖，不用于真实执行器、文件、脚本或 Shell。设备必须在执行时自行验证当前指标、可修改能力和值范围；`modifiable` 是设备声明，不是访问控制或安全联锁。

## 连接、认证与权限

| 参数 | 局域网 MQTT/TLS | 公网 MQTT over WSS |
| --- | --- | --- |
| 地址 | `10.0.0.113:8883` | `mqtt.web4sachika.asia:443` |
| URL | `mqtts://10.0.0.113:8883` | `wss://mqtt.web4sachika.asia/mqtt` |
| WebSocket path / 子协议 | 不适用 | `/mqtt` / `mqtt` |
| TLS 校验名称 | `10.0.0.113`（IP SAN） | `mqtt.web4sachika.asia`（域名 SNI） |
| 信任根 | 项目 `ca.crt` | 公网证书链的公共根 CA |

局域网 TLS 已部署；公网 Tunnel 路由须由管理员配置并端到端验证。设备需校准时钟并严格校验证书链、有效期和名称，不得跳过 TLS 校验。局域网只分发 CA 公共证书，不分发服务器私钥、CA 私钥或 Broker 管理凭据。

管理员在后端 CLI 执行 `add <device_id> <名称>`，并通过受控渠道把一次性密钥交给对应设备：

| 项目 | 要求 |
| --- | --- |
| MQTT 用户名 | 注册的 `device_id` |
| MQTT 密码 | `add` 或 `reset-secret` 当次显示的密钥 |
| ClientID | 每台设备唯一且稳定，不含密钥 |
| 遥测发布 | `factory/{device_id}/telemetry`，QoS 1，`retain=false` |
| 结果发布 | `factory/{device_id}/command_result`，QoS 1，`retain=false` |
| 命令订阅 | `factory/{device_id}/command`，QoS 1 |

设备角色只允许发布自己的 telemetry 和 command_result，并订阅、接收自己的 command。后端业务账户只订阅并接收 `factory/+/telemetry`、`factory/+/command_result`，并发布 `factory/+/command`；业务命令不使用 Dynamic Security 管理账户发送。ACL 按实际 `device_id` 隔离。

已有设备的 ACL 由后端幂等补齐；升级只修改角色权限，不重新创建设备账户、不轮换密钥、不改变账户启用状态。部分升级失败会保留已成功规则，后端之后继续补齐；意外角色、组成员或越权 ACL 会被拒绝并记录错误，先修复冲突再重试。

## 遥测

设备向 `factory/{device_id}/telemetry` 发送 UTF-8 JSON，最大 64 KiB。协议版本继续是字符串 `"1"`。旧设备可以省略 `modifiable`；省略表示不可修改。新设备按每个指标声明布尔值：

```json
{
  "version": "1",
  "device_id": "linux-01",
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "sampled_at": "2026-09-30T04:00:00Z",
  "metrics": {
    "segment-1": {"value": 23.6, "unit": "V", "modifiable": true},
    "segment-3": {"value": 55.0, "unit": "C", "modifiable": false}
  }
}
```

兼容的旧格式示例（缺省 `modifiable` 等价于 false）：

```json
{
  "version": "1",
  "device_id": "linux-01",
  "message_id": "550e8400-e29b-41d4-a716-446655440001",
  "sampled_at": "2026-09-30T04:00:01Z",
  "metrics": {"segment-1": {"value": 23.7, "unit": "V"}}
}
```

字段要求：

- `device_id` 必须与用户名和 Topic 中编号一致；编号限 64 字节 ASCII 字母、数字、下划线、连字符，以字母或数字开头，`admin` 保留。
- `message_id` 最多 128 字节、非空且无首尾空白/NUL；同一设备跨重启唯一。建议使用 UUID/ULID。重发同一采样必须沿用原 ID。
- `sampled_at` 使用带时区 RFC3339，推荐 UTC `Z`、毫秒精度。服务端统一转 UTC 并保存到微秒；最多允许设备时间比服务端快 5 分钟。
- 新设备使用 `segment-1` 至 `segment-100`，不要求连续编号。首次合法上报自动绑定该设备的 key 和 unit；单位以后必须保持一致。`MAX_METRICS_PER_DEVICE` 默认 10、允许配置 1 到 100。
- `metrics` 是非空对象，每个指标必须有有限 JSON 数字 `value`（0 合法）和字符串 `unit`；`modifiable` 可省略或为 JSON boolean。不得传 `null`、字符串或数字代替 boolean。
- 支持部分上报。缺少某指标表示本次无新样本，不是 0、不清除 Latest。历史保留原始子集，也保留 `modifiable` 原本是否出现；旧历史不会伪造该字段。
- 拒绝无效 UTF-8、重复 JSON 字段、未知字段、无效 message ID、身份不一致、超大消息、超指标数量和越界/非有限数值。

`modifiable` 随每项按现有 Latest 决胜顺序更新：`sampled_at`、`received_at`、`message_id`。旧协议省略字段时写入 Latest 的能力为 false；乱序消息不能覆盖较新的值或能力；未上报的其他指标保持不变。单位只在首次上报时绑定，`modifiable` 不是单位定义。

Latest 每项分别包含 `value`、`unit`、`modifiable`、`sampled_at`、`received_at` 和 `message_id`。`sampled_at` 是该项设备采样时间；设备的 `last_valid_received_at` 是服务端最后接收有效消息的时间。只在值变化时上报不能用于可靠判断在线状态；如需在线判断，必须有独立、周期性心跳和明确阈值。

上位机仅在遥测事务提交后 ACK。发布端收到 PUBACK 只说明 Broker 收到消息；它不表示服务端已存储。设备应在确认/重发策略中持久保存尚未完成消息和 message ID，不能把 QoS 1 当作业务端到端不丢失保证。

## 命令与结果

设备保持持久 MQTT 会话，订阅自己的 command Topic。命令和结果都是 QoS 1、`retain=false`。上位机先将命令持久化，再异步发布；CLI 会很快返回 command_id 和当前状态。

设置模拟值：

```json
{
  "version": "1",
  "command_id": "c7c8923d-0674-4b7e-9966-aa4b80c37a2d",
  "action": "set_metric",
  "metric_key": "segment-1",
  "value": 120.0
}
```

恢复模拟：

```json
{
  "version": "1",
  "command_id": "fd5a0ce0-3021-4db5-91de-80bc1004a9c9",
  "action": "clear_override",
  "metric_key": "segment-1"
}
```

成功设置的结果：

```json
{
  "version": "1",
  "command_id": "c7c8923d-0674-4b7e-9966-aa4b80c37a2d",
  "status": "applied",
  "metric_key": "segment-1",
  "value": 120.0
}
```

拒绝设置的结果：

```json
{
  "version": "1",
  "command_id": "c7c8923d-0674-4b7e-9966-aa4b80c37a2d",
  "status": "rejected",
  "metric_key": "segment-1",
  "value": 120.0
}
```

`clear_override` 成功时建议这样回复；不带 `value`，绝不能把缺少字段解释为数值 0：

```json
{
  "version": "1",
  "command_id": "fd5a0ce0-3021-4db5-91de-80bc1004a9c9",
  "status": "applied",
  "metric_key": "segment-1"
}
```

结果身份由 Topic 确定，并按 Topic 设备和 `command_id` 与已存命令关联。结果不要求新增 `device_id`、`action` 或时间字段。`set_metric` 的 applied 和 rejected 结果都必须回显请求 value；rejected 的 value 只是请求回显，不是实际采样。结果只能更新命令状态，不能更新遥测 Latest 或制造历史采样。设备要通过后续 telemetry 上报实际值。

上位机状态含义：

| 状态 | 含义 |
| --- | --- |
| `waiting_to_send` | 命令已持久化，等待发送或受限重试 |
| `broker_acked` | Broker 已确认发布；设备是否执行仍未知 |
| `applied` | 收到并验证设备执行结果；不代表已观察到目标遥测值 |
| `rejected` | 设备报告拒绝执行 |
| `result_unknown` | 命令期限结束后执行情况未知；允许迟到的有效结果完成确认 |
| `cancelled` | 设备删除、发送前能力检查失效，或期限内未曾发送 |

当前默认最多 5 次发布尝试，间隔从 5 秒指数增加、上限 60 秒，命令期限 10 分钟。达到次数上限后停止重发，但仍等待期限，以便接收最后一次尝试的迟到结果。期限结束时，曾尝试发布的命令为 `result_unknown`；从未发送的命令为 `cancelled`。相同 ID 和内容的重试沿用原 command_id；同设备同指标存在 waiting、broker_acked 或 result_unknown 命令时，不接受新的该指标命令。PUBACK 不会标记 applied。

设备禁用或处于生命周期过渡时，上位机暂停该设备命令发送和重试，但命令期限按创建时间继续计算。服务端没有执行序号或设备可验证的绝对过期时间。QoS 1、Broker 持久会话或设备离线可能导致已排队命令迟到；服务端停止重发不能撤回 Broker 或设备已收到的消息。设备须在执行时再次确认指标存在、仍可修改和值是否合法，并用持久去重记录避免重复执行。

CLI 联调：

```text
set linux-01 segment-1 120
command linux-01 <返回的 command_id>
commands linux-01 20
clear linux-01 segment-1
command linux-01 <clear 返回的 command_id>
get linux-01
history linux-01 20
```

先查看命令终态，再通过 `get` 或 `history` 检查实际 telemetry；数值相等也不能单独证明严格的命令/采样因果关系。

## 下位机去重与重启恢复要求

服务器端 command_id 去重只能避免服务器为同一调用创建重复 outbox 记录，不能阻止 Broker 在 QoS 1 下重复投递。设备必须：

1. 跨重启持久记录 `command_id`、命令内容和处理结果。
2. 相同 ID、相同内容：返回已存结果，不再次执行；相同 ID、不同内容：拒绝并记录异常。
3. 让去重记录保留期覆盖服务端命令重试期限和消息可能重投期限；不能只放 RAM。
4. 将模拟 override 状态和执行记录以一致方式持久化。避免“已执行但回复前崩溃”后重启再次执行；可用同一事务提交 override 与执行结果，回复失败后由重复命令返回已存结果。
5. 重启时恢复仍有效的 override；`clear_override` 成功则持久清除覆盖状态，并返回不含 value 的 applied 结果。具体覆盖存储介质和掉电原子性需下位机实现确认。

仓库当前 `sub_device/APP` 只是单次遥测发布示例：它硬编码 `device-001` 和 `temperature/pressure/current` 样例，不适用于新注册设备的 segment key；它没有 command 订阅、结果发布、执行日志、去重或 override 持久化。因此不能据此宣称命令具备重启后去重或覆盖恢复保证；在接入真实下位机前必须完成并联调上述持久化处理。其旧 telemetry 省略 `modifiable` 时上位机会安全地按 false 处理；该示例的 legacy 指标也不代表新设备默认指标。

## 联调顺序

1. 使用正确的 TLS/WSS 入口和设备凭据连接，检查 CONNACK。
2. 订阅自己的 command Topic，并发布一条部分 segment telemetry（先 false 或省略、再 true）。用 `get` 检查逐指标 Latest 与 `modifiable`。
3. 对 true 指标执行 `set`，收到 applied 后单独上报实际值；验证结果本身不改 Latest/history。再发送 clear_override 并返回无 value 的 applied。
4. 对 false 指标尝试命令，确认上位机拒绝提交；也测试下位机在能力变化后自行拒绝旧命令。
5. 重发同一命令验证设备返回既有结果但只执行一次；重启后再测同 ID 同内容、同 ID 不同内容及覆盖状态恢复。
6. 测试 rejected、结果丢失后的迟到结果、重复结果、断线重连、旧 telemetry 省略字段、重复 message_id 和跨设备 Topic 隔离。

`sampled_at` 表示每项采样时间；`received_at` 与 `last_valid_received_at` 是服务端接收时间。只在值变化时上报不能用于可靠判断在线状态。模拟下位机应单独周期发送心跳样本，即使测量值没有变化。
