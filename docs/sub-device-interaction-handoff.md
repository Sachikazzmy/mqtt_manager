# 下位机可修改指标交接

## 功能说明

下位机上报的每个指标增加 `modifiable` 布尔字段，表示服务器是否可以修改该指标的模拟值。

- `segment-1`：板子 SoC 真实温度，`modifiable: false`。
- `segment-2`：模拟压力，`modifiable: true`。
- `segment-3`：模拟电流，`modifiable: true`。

示例上报：

```json
{
  "version": "1",
  "device_id": "linux-01",
  "message_id": "linux-01-123-1",
  "sampled_at": "2026-09-30T13:30:00.000Z",
  "metrics": {
    "segment-1": {"value": 55.2, "unit": "V", "modifiable": false},
    "segment-2": {"value": 101.3, "unit": "kPa", "modifiable": true},
    "segment-3": {"value": 2.5, "unit": "A", "modifiable": true}
  }
}
```

`modifiable` 是协议扩展字段。服务端校验器必须允许该字段；如果继续使用只允许 `value` 和 `unit` 的旧严格 schema，这些消息会被拒绝。

## MQTT 命令

服务器发布到：

```text
factory/linux-01/command
```

设备返回到：

```text
factory/linux-01/command_result
```

修改 segment-2：

```json
{
  "version": "1",
  "command_id": "cmd-001",
  "action": "set_metric",
  "metric_key": "segment-2",
  "value": 120.0
}
```

修改 segment-3：

```json
{
  "version": "1",
  "command_id": "cmd-002",
  "action": "set_metric",
  "metric_key": "segment-3",
  "value": 12.0
}
```

成功结果：

```json
{
  "version": "1",
  "command_id": "cmd-001",
  "status": "applied",
  "metric_key": "segment-2",
  "value": 120.0
}
```

服务器尝试修改 `segment-1`、未知指标、缺少字段或非有限数字时，设备返回 `status: "rejected"`，不会修改真实温度。


恢复正常模拟：

```json
{
  "version": "1",
  "command_id": "cmd-003",
  "action": "clear_override",
  "metric_key": "segment-2"
}
```

## ACL 和启动

服务器需要允许设备：

```text
订阅：factory/linux-01/command
发布：factory/linux-01/telemetry
发布：factory/linux-01/command_result
```

启动命令：

```bash
export MQTT_USER=linux-01
export MQTT_KEY='设备密钥'
export MQTT_SEND=1
export MQTT_RECEIVE=1
./sub_device/build-linux/sub_device --mode normal --interval 10
```

设备命令连接使用稳定 ClientID：`sensor-linux-01-telemetry`（遥测）和 `sensor-linux-01-command`（命令）。命令使用 QoS 1、Retain false；服务端应使用 `command_id` 做去重。

## 联调步骤

1. 启动设备并确认日志出现 `MQTT command subscription active`。
2. 服务器下发 `set_metric segment-2`，值设为 `120.0`。
3. 观察下一次 telemetry，确认压力为 `120.00` 且 `modifiable` 为 `true`。
4. 尝试修改 `segment-1`，确认返回 `rejected`，CPU 温度仍由板端传感器读取。
5. 服务器修改 `current` 为 `12.0`，确认触发范围告警并上报异常值。
