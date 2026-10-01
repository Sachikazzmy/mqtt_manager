# Project01 阶段 A：部分遥测指标与逐指标 Latest

本阶段基于阶段 4 的 Mosquitto Dynamic Security、容器内 Go CLI、MQTT 设备认证、持续遥测接收和 PostgreSQL 持久化，支持每设备按变化只上报部分 `segment-N` 指标，并独立维护每项 Latest。新设备无需在 CLI 预登记指标；首次合法上报会将该 key 和单位绑定到该设备，默认最多绑定 10 个，硬上限为 100。当前不包含指标定义管理命令、设备下发命令、HTTP API 或 Web UI。设备配置、密钥验证摘要、指标内部身份、Latest、历史与去重键保存在数据库。

部署步骤见 [部署说明](docs/deployment.md)，交给下位机开发者的连接参数见 [设备接入交接](docs/device-handoff.md)。

## 启动

```sh
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/create-local-secrets.sh
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/renew-server-cert.sh
./scripts/init-broker.sh
docker compose attach backend
```

Compose 管理 `broker`、`db`、一次性 `broker-init` 和 `backend`。已有证书续签保留原 CA，备份旧服务器证书；日常启动只需运行初始化脚本，不必重复续签。Go 在容器内运行，CLI 与 MQTT 接收共用 PostgreSQL 仓储。用 Ctrl-P、Ctrl-Q 依次按下离开附着终端，保持接收运行；`quit`/Ctrl-C 会结束进程。不要再启动第二个 server。Broker 断线时本地查询仍可用，依赖 Broker 的设备操作会返回错误。

后端关闭 Docker 日志持久化，避免 CLI 展示的一次性密钥落盘；实时消息通过附着终端查看，历史通过 CLI 查询。不要对该终端启用录屏或输出重定向。

可用 `docker compose stop broker` 停止 Broker；普通 `docker compose down` 不删除 `mosquitto-data` 和 `postgres-data` 数据卷。不要使用 `docker compose down -v`，除非明确要删除全部业务数据。

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

启动迁移独立使用 `MIGRATION_TIMEOUT`，默认 `15m`，接受 Go duration（例如 `30m`）；它不受普通数据库查询 5 秒期限影响。Compose 后端健康检查默认给启动 16 分钟；若将迁移期限配置得更长，也应相应调大 `docker-compose.yml` 中 backend 健康检查的 `start_period`。

设备通过 `device_id` 作为 MQTT 用户名、一次性密钥作为密码连接。Topic 为 `factory/{device_id}/telemetry`。独立模拟器生成当前 UTC 采样时间和新的 UUIDv4 `message_id`，发布 QoS 1、`retain=false`：

```sh
docker compose exec backend publish -address broker:8883 -server-name broker -ca /run/secrets/mqtt_ca_cert -device device-001
docker compose exec backend publish -address broker:8883 -server-name broker -ca /run/secrets/mqtt_ca_cert -device device-001 -metrics segment-1=23.6:V,segment-3=0:A
```

模拟器可以只选一个或多个指标；不同次运行可用不同的 `-metrics` 子集。`segment-1` 至 `segment-100` 无需先登记，首次合法上报会在每设备身份数量未达配置上限时自动绑定该项单位。新设备只使用 segment 指标；已有数据库中被历史采样引用的旧指标身份为兼容历史而保留，未被历史使用的旧默认项由迁移移除。模拟器会在控制终端无回显地读取设备密钥。输出的 `PUBACK` 只表示 Broker 确认了 MQTT 发布，不代表 Go 已验证或保存业务数据；业务接收结果由运行 `cmd/server` 的终端显示。

## 遥测消息

`receive` 使用下面的 JSON 格式。文件只保存消息，不保存密钥。

```json
{
  "version": "1",
  "device_id": "device-001",
  "message_id": "boot-20260927-0001",
  "sampled_at": "2026-09-27T12:00:00Z",
  "metrics": {
    "segment-1": {"value": 0, "unit": "V"}
  }
}
```

新设备首次合法上报某个规范 `segment-N` 后，服务自动记住该 key 和首次上报的单位。`MAX_METRICS_PER_DEVICE` 默认 10，范围为 1 到 100，限制每台设备已绑定的指标身份总数；旧库中仍被历史引用的兼容指标也占用容量。没有上报过的 segment 不会出现在 Latest。任何值都必须是有限 JSON 数字，`0` 合法。默认未来采样时间容差为 5 分钟，超出会拒收。采样时间和服务端接收时间均按 UTC 保存。

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
    "segment-1": {"value": 23.6, "unit": "V"}
  }
}
```

字段规则：

| 字段 | 类型和要求 |
| --- | --- |
| `version` | 必填字符串，只允许 `"1"`。 |
| `device_id` | 必填非空字符串，最多 64 字节，必须与 Topic 中的设备编号一致。 |
| `message_id` | 必填非空字符串，最多 128 字节，不得含 NUL 或首尾空白；同一设备跨重启不能重复，推荐使用设备持久化的 UUID 或 ULID。 |
| `sampled_at` | 必填 RFC3339 时间，必须带时区；设备端推荐统一发送 UTC 的 `Z`，后端按 UTC 保存。明显晚于服务端时间的消息拒绝，默认容差为 5 分钟。 |
| `metrics` | 必填非空对象；可以只包含本次有新样本的一个或多个指标。规范的 `segment-1` 至 `segment-100` 可在首次合法上报时自动绑定单位，但不得超过每设备身份总数上限（默认 10；旧历史定义也计数）；payload 最大 64 KiB。 |

指标 key 只能由 ASCII 字母、数字、下划线和连字符组成，且以字母或数字开头，最多 64 字节。新设备仅接受规范 `segment-1` 至 `segment-100`；首次合法上报会自动绑定本次 `unit`，后续消息必须完全一致。此版本没有定义管理命令；没有上报过的指标不会显示在设备 Latest 中。`MAX_METRICS_PER_DEVICE` 限制每台设备已绑定身份总数，范围为 1 到 100。不能把已用编号改作其他物理含义或单位；需要变化时，应在设备集成记录中选择新编号。已有历史引用的 legacy key 及其单位不会被升级迁移删除，只有未被历史使用的旧默认定义会清理。

一次消息中重复出现同一个顶层字段、metric key、指标对象内的 `value` 或 `unit`，上报非规范或未登记 key、超过身份容量、已停用指标、单位不一致、非法 UTF-8 或 payload 超限都会被拒绝。v1 顶层结构及每项 `{"value": ..., "unit": ...}` 格式保持不变。

消息中不得出现未定义字段，不得把密钥、摘要、接收时间或设备配置放入 JSON。密钥由 MQTT 认证或其他受控的设备认证流程单独提供给后端，不能通过 Topic、JSON 或日志传递。

### Broker 接收入口

普通 MQTT 订阅消息不含发布者的连接密码。Mosquitto 先验证发布者用户名和密钥，再用 Dynamic Security ACL 将每个设备限制为仅可发布其自身的 `factory/{device_id}/telemetry`。Go 使用独立的订阅账户，仅允许订阅和接收 `factory/+/telemetry`。接收适配调用 `telemetry.Service.ReceiveFromBroker`；该入口校验 Topic、载荷身份和完整协议，再由 `CommitFromBroker` 在数据库事务内完成设备存在/启用检查、去重、历史追加和 Latest 更新。此入口只供已受 TLS、Broker 认证及 ACL 保护的接收适配使用，不会关闭本机 `receive` 的密钥验证。

消息通过 `ReceiveFromBroker` 校验及存储提交后，Go 才将 QoS 1 ACK 加入 Paho 的确认队列。Paho 按顺序发送 ACK；进程若在存储后、Broker 收到 ACK 前退出，重复投递会由 `(device_id, message_id)` 去重。设备收到的 PUBACK 仅表示 Broker 收到发布，并不等于 Go 已提交；Go 的订阅 ACK 仅在数据库事务成功提交后排队。永久拒收和重复消息会确认并丢弃；暂时存储错误保持未确认并触发重连。

回调只将消息放进有界队列，默认 128 条、上限 4096；队列满时消息不确认并触发断开/重连。一个有界 worker 负责校验和数据库提交。退出时未处理的队列消息不确认；Broker 持久会话最多保留 24 小时，超出后可能过期。终端事件缓冲也有上限，溢出的显示事件会汇总提示，可用 `get`、`list`、`history` 查询当前状态。

Broker 禁止新 retained 发布。订阅使用 MQTT 5 的 retain-as-published 和发送已有 retained 消息选项；因此升级前遗留的 retained 遥测会被 Go 明确拒收并确认，不会写入历史或 Latest。Broker 同时将遥测 payload 限制为 64 KiB；完整 MQTT 包另限为 66000 字节，Go 也独立限制 payload 为 64 KiB。

## 状态语义

设备创建后 `latest` 为 `null`。收到合法且非重复消息后，`latest.metrics` 按 `metric_key` 合并各指标独立的最新状态。每项分别包含 `value`、`unit`、`sampled_at`、`received_at` 和 `message_id`。本次未上报的指标保留旧值和旧时间，不会被清空或写成零。设备对象顶层的 `last_valid_received_at` 只表示最后一条有效非重复消息的服务端接收时间。

例如 10:00 上报 `segment-1`，11:00 上报 `segment-2` 和 `segment-3`：Latest 同时包含三项，其中 `segment-1.sampled_at` 仍为 10:00；历史有两条原始消息，第二条只含两项。乱序旧样本也作为原样消息进入历史，但只会在对应指标的 `(sampled_at, received_at, message_id)` 决胜顺序更新时覆盖该项 Latest。重复或拒收消息不写历史，也不推进 `last_valid_received_at`。

`get` 返回的时间字段会分开显示：

```json
{
  "last_valid_received_at": "2026-09-27T11:00:05Z",
  "latest": {
    "metrics": {
      "segment-1": {"value": 1, "unit": "V", "sampled_at": "2026-09-27T10:00:00Z", "received_at": "2026-09-27T10:00:05Z", "message_id": "m-10"},
      "segment-2": {"value": 2, "unit": "kPa", "sampled_at": "2026-09-27T11:00:00Z", "received_at": "2026-09-27T11:00:05Z", "message_id": "m-11"},
      "segment-3": {"value": 3, "unit": "A", "sampled_at": "2026-09-27T11:00:00Z", "received_at": "2026-09-27T11:00:05Z", "message_id": "m-11"}
    }
  }
}
```

按指标判断数据陈旧时，使用该项 `sampled_at` 与该指标的采样周期/陈旧阈值比较；`received_at` 用于识别传输延迟。此阶段不预设各指标周期，消费端应按设备的实际采样要求配置阈值。仅在数值变化时上报时，`last_valid_received_at` 不能表示在线状态。后续可在固件映射中预留一个未用于其他物理量的槽位（例如 `segment-10`）专作心跳，单位 `1`，每个配置周期即使测量值未变化也发送一个最小 v1 消息（例如值 `1`）。界面可在最后有效接收时间不超过 3 个配置心跳周期时显示在线，超过 3 个周期显示离线；未配置周期或从未收到消息时显示未知。当前没有在线判断实现，也不自动登记专用 heartbeat key，不能根据“数据未变化”判断离线。

若未来界面为历史记录显示名称，将按当前指标定义的 `display_name` 展示；历史本身保存 key、value、unit 和时间，不保存显示名快照。显示名修改会同步影响旧历史的标签，但不会改变 key、单位或物理含义。

`enabled` 只表示设备是否允许接收，不代表在线；当前阶段没有 Web UI 或离线判断。

删除设备会删除配置、凭据和 Latest，但保留历史。数据库保留已删除 ID 墓碑，跨重启不可复用，以避免历史混淆。

历史查询必须指定正数数量限制，最多返回 1000 条；未指定时间时默认查询最近 24 小时，单次时间范围最多 7 天。可选设备 ID、起始时间和结束时间。返回超过限制时只保留符合条件的最新记录，再按采样时间升序返回；查询期间会响应 context 取消。

## 依赖与数据流

```text
cmd/server
  -> cli
     ├-> device.Service -> Broker Dynamic Security
     ├-> receive.Receiver <- Mosquitto (TLS, QoS 1)
     │  -> 有界队列 -> telemetry.ReceiveFromBroker
     └-> telemetry.Service
        -> storage.PostgresStore（事务去重 + history + Latest）

本地 JSON 文件 + 控制终端密钥
  -> telemetry.Receive（保留密钥验证）
  -> 同一套协议校验
  -> storage.PostgresStore.Commit（同一事务）
  -> 去重 + 历史追加 + Device.Latest 更新
  -> device get/list 或 history 查询
```

`device` 负责设备模型、配置生命周期和随机密钥生成；`receive` 负责 TLS MQTT 连接、Dynamic Security 管理、持续订阅、重连、有界队列及确认时机；`telemetry` 负责本机文件入口与 Broker 入口共用的协议解析、字段校验、时间规则和接收流程；`storage` 负责设备配置、密钥摘要、去重键、历史及 Latest 的 PostgreSQL 持久化。`MemoryStore` 仅用于测试。

设备后端只保存 SHA-256 验证摘要，文件入口校验时用恒定时间比较；密钥由 `crypto/rand` 生成。设备 ID 仅允许 ASCII 字母、数字、`_`、`-`，必须以字母或数字开头，最多 64 字节，并保留 `admin` 给 Broker 管理账户。密钥不进入遥测 JSON、Topic、日志、命令参数或明文持久化业务文件；设备密钥仅在 TLS 保护的 Dynamic Security 管理 JSON 中短暂传递，Mosquitto 持久化的是认证摘要。

设备的 Broker 管理账户由管理员账户创建，角色和 ACL 必须精确限制为该设备 Topic。设备生命周期的操作意图先持久化，并立即关闭数据库接收；Broker 完成后再标记完成。中断后的 pending 操作会在启动及运行期间重试恢复：创建中断撤销账户并保留 ID 墓碑，删除中断继续删除，启停或重置中断使 Broker 与数据库都保持禁用。重置中断会持久标记“必须重新重置密钥”，直接 `enable` 将被拒绝；只有重新执行 `reset-secret` 完成两端同步后才能启用。一个设备恢复失败不会阻塞其他设备。正常启动不重建或轮换已有设备账户。详见 [部署与恢复](docs/deployment.md)。

## TLS 与真实设备

当前保留两条独立入口：局域网 `10.0.0.113:8883` 原生 MQTT/TLS，以及公网 `wss://mqtt.web4sachika.asia/mqtt`（需配置 Cloudflare Tunnel）。8883 默认监听所有宿主机网卡；9001 仅绑定宿主机回环，供宿主机 cloudflared 转发。公网 WSS 使用 Cloudflare 边缘证书，局域网 TLS 使用本项目私有 CA；两者不可混用信任配置。

更换地址或续签时：

```sh
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/renew-server-cert.sh
docker compose up -d --force-recreate broker
```

局域网真实设备必须校验地址/证书名称，并信任 `.secrets/mosquitto/ca.crt`。只复制 CA 公共证书，不复制服务器或 CA 私钥。宿主机 Go 调试须先停止容器 backend，并在本机覆盖配置中临时将数据库 5432 只映射到 `127.0.0.1`；默认 Compose 不开放该端口：

```sh
MQTT_ADDRESS=10.0.0.113:8883 MQTT_SERVER_NAME=10.0.0.113 MQTT_CA_FILE="$PWD/.secrets/mosquitto/ca.crt" DB_HOST=127.0.0.1 DB_PASSWORD_FILE="$PWD/.secrets/db_password" go run ./cmd/server
```

真实设备配置 MQTT over TLS、用户名 `device_id`、密码为 `add` 或 `reset-secret` 当次显示的密钥，发布 QoS 1、`retain=false` 到该设备精确 Topic。真实设备应持久保存其 UUID/ULID `message_id`，同一消息重发必须沿用原 ID；新消息使用新 ID。

本机 `.secrets/` 包含 Broker 管理密码、订阅账户密码、服务器私钥和 CA 私钥，必须限制访问并纳入受控备份，不能提交到版本库。`scripts/create-local-secrets.sh` 不会覆盖已有凭据或证书；如果既有证书不完整会停止，以免悄悄替换信任根。容器动态安全账户、ACL 和 MQTT 状态保存在 `mosquitto-data` 卷中。

## 重启与恢复

正常重启及普通容器重建后，原设备密钥继续可用，不必重新 `add`。数据库和 Broker 必须一同备份；只恢复其中一侧可能造成账户与摘要不一致。旧内存版数据没有自动迁移：若旧进程已经退出，其设备配置与历史无法恢复；现有 Broker 账户仍在时，对旧设备执行一次新的 `add` 会轮换密钥，必须重新安全下发。若旧进程尚运行，应在停机前人工导出所需设备清单和历史；旧版没有受支持的自动导出工具。

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

开启 `PROJECT01_MQTT_E2E=1` 前必须停止 `backend`，避免测试接收器与运行中的后端抢占相同 ClientID。集成测试会重启当前 Compose Broker，验证自动重连、认证/ACL、生命周期撤权和测试用共享内存查询；测试结束删除临时设备账户，保留角色和 Broker 数据卷，再启动 backend。真实异机 TLS/WSS 联调仍需在目标部署环境验证。

PostgreSQL 结构、迁移、故障恢复与备份步骤见 [部署文档](docs/deployment.md)。`pgx/v5` 只承担 PostgreSQL 连接、参数化查询和连接池；未引入 ORM、缓存或额外消息中间件。数据库密码保存在 `.secrets/db_password`，由初始化脚本生成；旧内存数据没有自动迁移。
