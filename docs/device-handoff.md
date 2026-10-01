# 下位机接入交接（协议 v1）

## 选择入口

| 参数 | 局域网 MQTT/TLS | 公网 MQTT over WSS |
| --- | --- | --- |
| 地址 | 10.0.0.113 | mqtt.web4sachika.asia |
| 端口 | 8883 | 443 |
| 传输 | 原生 MQTT over TLS | MQTT over WebSocket over TLS |
| URL | mqtts://10.0.0.113:8883 | wss://mqtt.web4sachika.asia/mqtt |
| WebSocket path | 不适用 | /mqtt |
| WebSocket 子协议 | 不适用 | mqtt |
| 信任证书 | 本项目 ca.crt | Cloudflare 公网证书链的公共根 CA |
| TLS 校验名称 | 10.0.0.113（IP SAN） | mqtt.web4sachika.asia，同时发送域名 SNI |
| 前提 | 能路由到该局域网地址 | 设备支持 WSS，管理员已配置 Tunnel 路由 |

不知道固件是否支持 WSS 时，先完成局域网 TLS 联调。不可把 mqtts://域名:8883 当作 Tunnel 的替代入口，也不可把原生 MQTT 包直接发送给 443。局域网 TLS 已部署；公网 Tunnel 路由仍需管理员配置和端到端验证。

局域网下发文件是 `ca.crt`，不下发 server.key、ca.key、Broker 管理密码或后端订阅密码。CA SHA-256 指纹：

```text
BD:34:2A:05:CF:34:A4:51:85:43:CE:0B:D4:64:C5:67:A0:FF:6F:D4:32:27:58:EA:51:A1:31:9B:74:22:D2:F0
```

设备应校准时钟并严格验证证书链、有效期和名称，不允许跳过 TLS 校验。WSS 的公网证书由 Cloudflare 管理，会续期；不要固定服务器叶证书。局域网当前服务器证书到期时间为 2027-09-29 08:05:32 UTC。

## 认证与发布

管理员在运行中的后端 CLI 执行 `add device-001 设备名称`，将产生的密钥通过受控渠道交给这一台设备：

| 字段 | 配置 |
| --- | --- |
| MQTT 版本 | 推荐 5.0 |
| Username | 注册的 device_id，例如 device-001 |
| Password | add/reset-secret 返回的设备密钥，原样使用 |
| ClientID | 唯一、稳定且不含密钥，例如 sensor-device-001 |
| 发布 Topic | factory/device-001/telemetry |
| QoS | 1 |
| Retain | false |
| 订阅 | 无；设备账户仅允许向自身 Topic 发布 |

两个入口共享相同认证和 ACL。密钥不放在 Topic、JSON、ClientID、URL、日志或命令参数。不同设备不能共用设备身份。reset-secret 后旧密钥失效；禁用/删除会撤销接入。正常重启或普通容器重建后，原设备密钥继续有效，不必重新注册。

## 指标编号与单位约定

新设备不需要管理员提前登记指标。每台设备可使用 `segment-1` 至 `segment-100`，首次合法上报某个编号后，服务会记住该设备的编号和首次上报单位；默认每设备最多绑定 10 个编号，可通过 `MAX_METRICS_PER_DEVICE` 调整为 1 到 100。超出容量的新编号会被拒绝。单位必须在后续消息中保持一致，后端不提供单位或物理含义修改命令。

下位机项目应在固件配置或独立的接线/集成记录中固定编号语义。已经上报过的编号不能改作其他物理量；单位或含义变化时，使用尚未用过的新编号。`list/get` 不展示 `metric_definitions`，当前 CLI 也没有指标定义管理命令。旧设备历史引用的 legacy 指标记录会为保留历史而继续存储；新设备消息使用 segment 编号。

建议在设备集成记录中维护每台设备自己的映射和陈旧阈值：

| 固件 key | 固定物理含义 | `unit` | 采样周期 / 陈旧阈值 |
| --- | --- | --- | --- |
| `segment-1` | 按设备接线表填写，例如直流母线电压 | 按传感器量纲填写，例如 `V` | 按该通道采样策略设置 |
| `segment-2` | 按设备接线表填写，例如出口压力 | 按传感器量纲填写，例如 `kPa` | 按该通道采样策略设置 |

首次上报时选定单位要特别谨慎：后续单位不同会被拒绝。新自动绑定的 segment 没有服务器端可配置的数值范围或显示名称；值必须是有限 JSON 数字，`0` 合法。历史保留的旧指标仍按原有绑定规则校验。缺少的 key 代表本次没有新采样，不是 0，也不会清除该项 Latest。

## 消息

UTF-8 JSON，最大 65536 字节。以下时间和 message_id 仅作格式示例，每次采样必须生成真实时间和新 ID：

```json
{
  "version": "1",
  "device_id": "device-001",
  "message_id": "01J8V3Y7Q5M4K2N6P8R0S1T2U3",
  "sampled_at": "2026-09-29T04:15:30.123Z",
  "metrics": {
    "segment-1": {"value": 23.6, "unit": "V"}
  }
}
```

这是一条只上报 `segment-1` 的合法部分消息，无需预登记。首次成功后服务会自动记录 `segment-1` 和单位 `V`。只上报两个或更多刚产生新样本的指标时，在 `metrics` 中加入对应成员即可：

```json
"metrics": {
  "segment-1": {"value": 23.6, "unit": "V"},
  "segment-2": {"value": 101.3, "unit": "kPa"}
}
```

- 所有顶层字段必填，只允许 version 为字符串 "1"，device_id 必须与注册身份及 Topic 一致。
- device_id 最多 64 字节，仅 ASCII 字母、数字、下划线和连字符，以字母或数字开头；admin 保留。
- message_id 为非空字符串、最多 128 字节、不得包含 NUL 或首尾空白，同一设备跨重启唯一。生成新 UUID/ULID 并将待发送消息持久化；重发同一采样沿用原 ID，不能每次重试生成新 ID。
- sampled_at 为带时区 RFC3339，推荐 UTC Z，毫秒精度即可；服务端统一转 UTC 并截到微秒，超出微秒的尾数会丢弃；最多允许比服务器时间超前 5 分钟。
- metric key 只能含 ASCII 字母、数字、下划线和连字符，以字母或数字开头，最多 64 字节。`metrics` 至少一项；数量不能超过每设备身份总数上限（默认 10），每一项必须是规范 `segment-N` 编号，已绑定指标单位必须一致，未绑定编号可在剩余容量内首次自动创建；payload 最大 65536 字节。
- 新设备只接受规范 `segment-1` 到 `segment-100`；首次合法上报会自动绑定单位，不要求编号连续；历史保留的旧定义继续按已存单位校验。可选范围包含边界。value 必须是有限 JSON 数字，0 合法；每项必须含 `value` 和 `unit`。
- 不允许缺失字段、额外字段、重复顶层字段、重复 metric key、重复指标对象字段、非法 UTF-8、非规范 `segment-N` key、超出身份总数上限、错误单位、字符串数值、null、超大消息及明显超前时间。

缺少的指标表示“本次没有新采样”，不是零或清空。Latest 按 key 独立更新，每项含 `value`、`unit`、`sampled_at`、`received_at`、`message_id`；设备对象的 `last_valid_received_at` 则是最后一次有效消息的服务端接收时间。例如 10:00 只报 `segment-1`，11:00 报 `segment-2` 和 `segment-3` 后，Latest 同时有三项，`segment-1.sampled_at` 保持 10:00；历史仍是两条原始消息，第二条只含两项。乱序旧消息照样进入历史，但只会更新其内各个满足 `(sampled_at, received_at, message_id)` 决胜规则的指标。业务去重键为 `(device_id, message_id)`，重复消息不新增历史。

历史按稳定 key 展示；当前版本没有服务器端显示名称管理，历史记录也不保存名称快照。

陈旧判断按每项 `sampled_at` 与为该指标配置的采样周期/陈旧阈值比较；`received_at` 可辅助识别延迟到达。只在值变化时上报会让 `last_valid_received_at` 长时间不变，不能据此断定设备离线。后续可在固件映射中预留一个尚未用于其他物理量的槽位（例如 `segment-10`）专作心跳，单位 `1`，每个配置周期即使值未变化也发送一个最小 v1 消息（例如值 `1`）。界面按设备配置的心跳周期计算：最后有效接收不超过 3 个周期为在线，超过 3 个周期为离线；未配置周期或从未收到消息时为未知。当前尚未实现在线判断，也不自动登记专用 heartbeat key；不得根据“数据未变化”判断离线。

## 联调验收

1. 校准设备时间，导入对应入口的信任根，注册设备并配置 Username/Password。
2. 建立 TLS（WSS 还需 WebSocket Upgrade），确认 MQTT CONNACK 成功。
3. 发送一条 QoS 1、非 retained 的合法消息；管理员确认 CLI 接收成功，并用 get/history 检查。
4. 原消息重复发送，历史不增加；再发新 ID，历史增加，Latest 按时间更新。
5. 验证错误密码、其他设备 Topic、错误单位被拒绝。认证失败不要无限快速重试。
6. 断网恢复后按指数退避重连（例如 1、2、4 秒，上限 30 秒并加随机抖动），重发未确认的原消息。

PUBACK 只确认 Broker 接收，不是业务入库回执。本阶段没有设备侧业务 ACK Topic；业务结果在后端观察。后端提交成功后持久化历史和去重键；设备端缓存与 MQTT QoS 1 仍不能单独保证业务永不丢失。
