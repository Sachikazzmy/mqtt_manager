# Project01 阶段 3

本阶段提供 Mosquitto Dynamic Security、容器内 Go CLI、MQTT 设备认证、持续遥测接收和内存历史查询闭环。当前不包含 HTTP API、PostgreSQL 或前端；设备配置及遥测在 Go 进程退出后丢失。

部署步骤见 [部署说明](docs/deployment.md)，交给下位机开发者的连接参数见 [设备接入交接](docs/device-handoff.md)。

## 启动

```sh
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/create-local-secrets.sh
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/renew-server-cert.sh
./scripts/init-broker.sh
docker compose attach backend
```

Compose 管理 `broker`、一次性 `broker-init` 和 `backend`。已有证书续签保留原 CA，备份旧服务器证书；日常启动只需运行初始化脚本，不必重复续签。Go 在容器内运行，CLI 与 MQTT 接收共用一个 `MemoryStore`。用 Ctrl-P、Ctrl-Q 依次按下离开附着终端，保持接收运行；`quit`/Ctrl-C 会结束进程。不要再启动第二个 server。Broker 断线时本地查询仍可用，依赖 Broker 的设备操作会返回错误。

后端关闭 Docker 日志持久化，避免 CLI 展示的一次性密钥落盘；实时消息通过附着终端查看，历史通过 CLI 查询。不要对该终端启用录屏或输出重定向。

可用 `docker compose stop broker` 停止 Broker；普通 `docker compose down` 不删除 `mosquitto-data` 数据卷。不要使用 `docker compose down -v`，除非明确要删除 Broker 状态。

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

设备通过 `device_id` 作为 MQTT 用户名、一次性密钥作为密码连接。Topic 为 `factory/{device_id}/telemetry`。独立模拟器生成当前 UTC 采样时间和新的 UUIDv4 `message_id`，发布 QoS 1、`retain=false`：

```sh
docker compose exec backend publish -address broker:8883 -server-name broker -ca /run/secrets/mqtt_ca_cert -device device-001
docker compose exec backend publish -address broker:8883 -server-name broker -ca /run/secrets/mqtt_ca_cert -device device-001 -metrics temperature=23.6,pressure=101.3,current=2.5
```

模拟器会在控制终端无回显地读取设备密钥。输出的 `PUBACK` 只表示 Broker 确认了 MQTT 发布，不代表 Go 已验证或保存业务数据；业务接收结果由运行 `cmd/server` 的终端显示。

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

设备只能通过下面的 Topic 和 JSON 载荷进入业务接收流程。JSON 的字段顺序和空白不重要，但字段名、字段类型、允许的指标和业务规则必须符合本节；MQTT 回调不得绕过 `telemetry.Service` 直接写入存储。

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

### Broker 接收入口

普通 MQTT 订阅消息不含发布者的连接密码。Mosquitto 先验证发布者用户名和密钥，再用 Dynamic Security ACL 将每个设备限制为仅可发布其自身的 `factory/{device_id}/telemetry`。Go 使用独立的订阅账户，仅允许订阅和接收 `factory/+/telemetry`。接收适配调用 `telemetry.Service.ReceiveFromBroker`；该入口校验 Topic、载荷身份和完整协议，再由 `CommitFromBroker` 在内存存储锁内完成设备存在/启用检查、去重、历史追加和 Latest 更新。此入口只供已受 TLS、Broker 认证及 ACL 保护的接收适配使用，不会关闭本机 `receive` 的密钥验证。

消息通过 `ReceiveFromBroker` 校验及存储提交后，Go 才将 QoS 1 ACK 加入 Paho 的确认队列。Paho 按顺序发送 ACK；进程若在存储后、Broker 收到 ACK 前退出，重复投递会由 `(device_id, message_id)` 去重。QoS 1 不保证内存业务数据跨进程重启保留，也不等于业务入库确认。永久拒收和重复消息会确认并丢弃；暂时存储错误保持未确认并触发重连。

回调只将消息放进有界队列，默认 128 条、上限 4096；队列满时消息不确认并触发断开/重连。一个有界 worker 负责校验和内存存储。退出时未处理的队列消息不确认；Broker 持久会话最多保留 24 小时，超出后可能过期。终端事件缓冲也有上限，溢出的显示事件会汇总提示，可用 `get`、`list`、`history` 查询当前状态。

Broker 禁止新 retained 发布。订阅使用 MQTT 5 的 retain-as-published 和发送已有 retained 消息选项；因此升级前遗留的 retained 遥测会被 Go 明确拒收并确认，不会写入历史或 Latest。Broker 同时将遥测 payload 限制为 64 KiB；完整 MQTT 包另限为 66000 字节，Go 也独立限制 payload 为 64 KiB。

## 状态语义

设备创建后 `latest` 为 `null`。收到合法且非重复消息后，设备对象中的 `latest` 保存该条消息的完整指标快照，包含指标、采样时间、接收时间、最后有效接收时间和消息 ID；它不会把不同消息的指标合并。

历史会保存每条合法的非重复消息。乱序旧采样仍进入历史并可推进 `last_valid_received_at`，但不会覆盖较新的 Latest。Latest 的决胜顺序是：采样时间较新优先；采样时间相同则接收时间较新优先；两个时间都相同则消息 ID 字典序较大优先。重复或拒收消息不会推进最后有效接收时间。`enabled` 只表示是否允许接收，不代表设备在线，本阶段不做离线判定。

删除设备会删除配置、凭据和 Latest，但保留历史。一个进程内已删除的设备 ID 不允许重新创建，以避免历史混淆；由于本阶段是纯内存存储，进程退出后包括这个禁止复用记录在内的所有数据都会丢失。

历史查询必须指定正数数量限制，最多返回 1000 条；未指定时间时默认查询最近 24 小时，单次时间范围最多 7 天。可选设备 ID、起始时间和结束时间。返回超过限制时只保留符合条件的最新记录，再按采样时间升序返回；查询期间会响应 context 取消。

## 依赖与数据流

```text
cmd/server
  -> cli
     ├-> device.Service -> Broker Dynamic Security
     ├-> receive.Receiver <- Mosquitto (TLS, QoS 1)
     │  -> 有界队列 -> telemetry.ReceiveFromBroker
     └-> telemetry.Service
        -> storage.MemoryStore（去重 + history + Latest 原子更新）

本地 JSON 文件 + 控制终端密钥
  -> telemetry.Receive（保留密钥验证）
  -> 同一套协议校验
  -> storage.MemoryStore.Commit（同一把锁）
  -> 去重 + 历史追加 + Device.Latest 更新
  -> device get/list 或 history 查询
```

`device` 负责设备模型、配置生命周期和随机密钥生成；`receive` 负责 TLS MQTT 连接、Dynamic Security 管理、持续订阅、重连、有界队列及确认时机；`telemetry` 负责本机文件入口与 Broker 入口共用的协议解析、字段校验、时间规则和接收流程；`storage` 负责设备配置、密钥摘要、去重键、历史及 Latest 的内存保存。仓储查询返回独立副本，外部修改不会改变内部 map 或 Latest 指针。

设备后端只保存 SHA-256 验证摘要，文件入口校验时用恒定时间比较；密钥由 `crypto/rand` 生成。设备 ID 仅允许 ASCII 字母、数字、`_`、`-`，必须以字母或数字开头，最多 64 字节，并保留 `admin` 给 Broker 管理账户。密钥不进入遥测 JSON、Topic、日志、命令参数或明文持久化业务文件；设备密钥仅在 TLS 保护的 Dynamic Security 管理 JSON 中短暂传递，Mosquitto 持久化的是认证摘要。

设备的 Broker 管理账户由管理员账户创建；账户只能绑定本应用为该设备创建的专属角色，不能加入 Dynamic Security 组；启停、重置及删除前会重新检查角色 ACL 仍精确限定为该设备的发布 Topic。后端订阅账户和管理账户是不同身份，匿名连接关闭。新建设备时先配置 Broker，再写入本地内存；本地创建失败会尝试删除 Broker 账户。启用时先确保本地接收关闭，Broker 确认启用后才开放本地接收；请求失败时设备保持本地禁用，若本地提交失败则尝试禁用 Broker 账户。禁用和删除时先关闭本地接收，再操作 Broker；Broker 失败时本地仍禁用，修复权限或连接后可重试。重置时先在本地禁用，再让 Broker 禁用旧连接、设置新密钥并按原状态启用，最后更新本地摘要。删除先撤销/删除 Broker 账户，再删除本地设备。两边没有共同事务：如果管理响应超时或状态不确定，不展示密钥；设备保持本地禁用，用户可重试。若添加操作在 Broker 已创建账户后中断，再次 `add` 会校验专属角色并轮换该账户密钥。失败补偿无法确认时，按文末恢复说明人工检查，不自动清理其他账户。

## TLS 与真实设备

当前保留两条独立入口：局域网 `10.0.0.113:8883` 原生 MQTT/TLS，以及公网 `wss://mqtt.web4sachika.asia/mqtt`（需配置 Cloudflare Tunnel）。8883 默认监听所有宿主机网卡；9001 仅绑定宿主机回环，供宿主机 cloudflared 转发。公网 WSS 使用 Cloudflare 边缘证书，局域网 TLS 使用本项目私有 CA；两者不可混用信任配置。

更换地址或续签时：

```sh
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/renew-server-cert.sh
docker compose up -d --force-recreate broker
```

局域网真实设备必须校验地址/证书名称，并信任 `.secrets/mosquitto/ca.crt`。只复制 CA 公共证书，不复制服务器或 CA 私钥。仅在停止容器 backend 后才可单独调试宿主机 Go：

```sh
MQTT_ADDRESS=10.0.0.113:8883 MQTT_SERVER_NAME=10.0.0.113 MQTT_CA_FILE="$PWD/.secrets/mosquitto/ca.crt" go run ./cmd/server
```

真实设备配置 MQTT over TLS、用户名 `device_id`、密码为 `add` 或 `reset-secret` 当次显示的密钥，发布 QoS 1、`retain=false` 到该设备精确 Topic。真实设备应持久保存其 UUID/ULID `message_id`，同一消息重发必须沿用原 ID；新消息使用新 ID。

本机 `.secrets/` 包含 Broker 管理密码、订阅账户密码、服务器私钥和 CA 私钥，必须限制访问并纳入受控备份，不能提交到版本库。`scripts/create-local-secrets.sh` 不会覆盖已有凭据或证书；如果既有证书不完整会停止，以免悄悄替换信任根。容器动态安全账户、ACL 和 MQTT 状态保存在 `mosquitto-data` 卷中。

## 重启与恢复

Go 进程重启会清空所有本地设备和遥测；Broker 卷中的设备账户与角色仍然存在，但它们不能单独让接收器接纳数据。重新运行 Go 后，对原设备 ID 执行 `add`：服务会确认 Broker 上存在本应用专属、Topic 精确的角色，再轮换该账户密钥并建立新的内存设备记录。新的一次性密钥必须重新安全配置到设备。不会扫描或自动删除其他 Broker 账户。设备删除后本应用账户会被禁用并删除；该设备专属角色会留在 Broker 中，以免误删被人工复用的角色。

如果 `add`、`reset-secret`、启停或删除遇到超时/连接中断，终端会返回失败且不显示密钥；本地设备在无法确定 Broker 状态时保持禁用。重新执行相同操作会先检查账户和角色状态。若管理员或订阅账户、证书文件丢失，初始化命令不能从 Broker 的摘要恢复明文密码，应先从受控备份恢复 `.secrets/`；不要直接删除数据卷。当前没有在线状态、离线阈值或跨进程数据恢复能力。

## 可复现验证

```sh
gofmt -w cmd/server/*.go cmd/broker-setup/*.go cmd/publish/*.go internal/cli/*.go internal/device/*.go internal/receive/*.go internal/storage/*.go internal/telemetry/*.go
go test ./...
go vet ./...
go test -race ./...
./scripts/create-local-secrets.sh
docker compose config --quiet
PROJECT01_MQTT_E2E=1 \
  MQTT_CA_FILE="$PWD/.secrets/mosquitto/ca.crt" \
  MQTT_ADMIN_PASSWORD_FILE="$PWD/.secrets/dynsec_admin_password" \
  MQTT_RECEIVER_PASSWORD_FILE="$PWD/.secrets/receiver_password" \
  go test ./internal/receive -run '^TestBrokerEndToEndDeviceToCLIQueries$' -count=1
```

仓库不提供真实密钥示例。容器可运行的环境中，可按下面步骤验证完整闭环：

1. 执行 `./scripts/init-broker.sh`，再附着 `docker compose attach backend`。
2. 在 CLI 执行 `add device-001 一号温度设备`，将一次性密钥配置到模拟器或设备。
3. 另一个终端运行前述 `docker compose exec backend publish ...` 模拟器，输入密钥；服务终端应显示接收成功、设备 ID、消息 ID 和指标。
4. 在服务 CLI 执行 `get device-001`、`list`、`history device-001 20`，检查 Latest、接收时间和历史记录。
5. 再执行 `reset-secret device-001`，旧密钥应不能重新连接，新密钥可发布；执行 `disable` 后发布应被拒收；重新启用后再检查 `delete` 撤权。
6. 使用两个测试设备分别尝试彼此 Topic、错误密码、QoS 0、retained、错误单位和超大 payload；这些消息不能写入历史或 Latest。

开启 `PROJECT01_MQTT_E2E=1` 前必须停止 `backend`（会丢失内存数据），避免测试接收器与运行中的后端抢占相同 ClientID。集成测试会重启当前 Compose Broker，验证自动重连、认证/ACL、生命周期撤权和共享内存查询；测试结束删除临时设备账户，保留角色和 Broker 数据卷，再启动 backend。真实异机 TLS/WSS 联调及业务数据跨进程恢复仍需在目标部署环境验证。
