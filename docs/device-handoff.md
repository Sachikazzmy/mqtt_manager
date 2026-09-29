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

两个入口共享相同认证和 ACL。密钥不放在 Topic、JSON、ClientID、URL、日志或命令参数。不同设备不能共用设备身份。reset-secret 后旧密钥失效；禁用/删除会撤销接入。后端当前使用内存，重启后需重新注册并下发新密钥。

## 消息

UTF-8 JSON，最大 65536 字节。以下时间和 message_id 仅作格式示例，每次采样必须生成真实时间和新 ID：

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

- 所有顶层字段必填，只允许 version 为字符串 "1"，device_id 必须与注册身份及 Topic 一致。
- device_id 最多 64 字节，仅 ASCII 字母、数字、下划线和连字符，以字母或数字开头；admin 保留。
- message_id 为非空字符串、最多 128 字节，同一设备跨重启唯一。生成新 UUID/ULID 并将待发送消息持久化；重发同一采样沿用原 ID，不能每次重试生成新 ID。
- sampled_at 为带时区 RFC3339，推荐 UTC Z，毫秒精度即可；最多允许比服务器时间超前 5 分钟。
- metrics 至少一个指标，只允许 temperature/C、pressure/kPa、current/A。value 为有限 JSON 数字，0 合法；每项必须包含 value 和 unit。
- 不允许缺失字段、额外字段、未知指标、错误单位、字符串数值、null、超大消息及明显超前时间。

每条消息是完整指标快照：Latest 不会把本条缺少的指标与上一条合并。乱序旧采样仍保存历史，但不覆盖较新 Latest。业务去重键为 (device_id, message_id)。

## 联调验收

1. 校准设备时间，导入对应入口的信任根，注册设备并配置 Username/Password。
2. 建立 TLS（WSS 还需 WebSocket Upgrade），确认 MQTT CONNACK 成功。
3. 发送一条 QoS 1、非 retained 的合法消息；管理员确认 CLI 接收成功，并用 get/history 检查。
4. 原消息重复发送，历史不增加；再发新 ID，历史增加，Latest 按时间更新。
5. 验证错误密码、其他设备 Topic、错误单位被拒绝。认证失败不要无限快速重试。
6. 断网恢复后按指数退避重连（例如 1、2、4 秒，上限 30 秒并加随机抖动），重发未确认的原消息。

PUBACK 只确认 Broker 接收，不是业务入库回执。本阶段没有设备侧业务 ACK Topic；业务结果在后端观察。Go 内存数据无法跨重启保留，设备端缓存和 MQTT QoS 1 都不能单独保证业务永不丢失。
