# Project01 部署与恢复

Compose 包括 broker、db、broker-init（初始化后退出）和 backend；不包含 HTTP API 或网页。PostgreSQL 保存设备、密钥摘要、逐指标 Latest、遥测历史、命令 outbox/结果异常、去重键及已删除 ID；普通重启或容器重建继续使用原设备密钥。CLI 仅增加面向模拟指标的 `set_metric` / `clear_override`，不控制真实执行器。

## 本机部署

要求 Docker Compose v2、OpenSSL，以及可用的 8883/9001 端口。Go 通过多阶段镜像构建，宿主机无需安装 Go。

```sh
cp .env.example .env
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/create-local-secrets.sh
# 保留现有 CA，为当前域名、局域网 IP 和容器服务名签发证书。
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/renew-server-cert.sh
./scripts/init-broker.sh
docker compose ps -a
docker compose attach backend
```

已有 `.env` 时按需编辑，不要覆盖；已有证书仅在地址变化或续期时运行 renew。`.env` 由 Compose 读取，证书脚本使用上面的显式环境变量，不读取 `.env`。默认 8883 绑定全部网卡，9001 仅绑定 `127.0.0.1`。如只允许局域网地址监听，将 `.env` 中 `MQTT_BIND_ADDRESS` 改为 `10.0.0.113` 并重建 broker；主机防火墙只向需要的局域网开放 8883，不做公网端口映射。

预期 db、broker、backend healthy，broker-init Exited (0)。backend 健康标记需要 telemetry 和 command_result 两个 MQTT 订阅都成功及数据库 Ping 成功；依赖断连时清除。默认 `start_period` 为 16 分钟，以覆盖 15 分钟的迁移期限。Docker 健康状态不能代替端到端业务验证。

CLI 中执行：

```text
add device-001 温度设备
get device-001
list
history device-001 20
set device-001 segment-1 120
command device-001 <command_id>
commands device-001 20
clear device-001 segment-1
```

将 add 显示的密钥单独交给对应设备。依次按 Ctrl-P、Ctrl-Q 离开附着终端，接收器继续运行。不要用 `docker compose exec backend server` 或另启宿主机 server；它们会与原接收器争抢 ClientID。需要停止时使用 `docker compose stop backend`；直接 quit/EOF/Ctrl-C 会结束 Go，重启策略可能将其重新启动，数据库数据保留。

后端以 UID/GID 10001 运行，根文件系统只读，只有临时目录可写。其日志驱动为 none，避免一次性设备密钥被 Docker 捕获；`docker compose logs backend` 不提供实时记录，请 attach 查看。Broker 不记录消息正文，仍应限制 Docker 和日志访问权。

另一终端可以模拟设备：

```sh
docker compose exec backend publish -address broker:8883 -server-name broker -ca /run/secrets/mqtt_ca_cert -device device-001
```

按提示输入设备密钥。PUBACK 仅表示 Broker 确认；在附着终端和 get/history 中确认业务接收。

要测试命令前，可再上报一条带可修改能力的最新值：

```sh
docker compose exec backend publish -address broker:8883 -server-name broker -ca /run/secrets/mqtt_ca_cert -device device-001 -metrics segment-1=23.6:V -modifiable segment-1
```

然后在 backend CLI 执行 `set device-001 segment-1 120`。CLI 返回命令 ID 后，设备客户端需要订阅自身 command Topic、执行并发布 command_result；若只使用仓库当前 `sub_device/APP` 示例，不会有命令回复。

## Cloudflare Tunnel / WSS

公网目标为 `wss://mqtt.web4sachika.asia/mqtt`。需要设备 MQTT 库支持 WebSocket（TLS + HTTP Upgrade + MQTT 二进制帧），仅支持原生 MQTT/TLS 的设备不能直连此入口。

在运行于本机的 cloudflared Tunnel 中添加 Published application route：

| 配置 | 值 |
| --- | --- |
| 公网主机名 | mqtt.web4sachika.asia |
| 源站类型 | HTTP |
| 源站地址 | localhost:9001 |
| 路径过滤 | 留空，保留请求路径 /mqtt |

Cloudflare 负责公网 TLS，Tunnel 通过本机 HTTP WebSocket 入口交给 Mosquitto。源站地址不要填 HTTPS 或 8883，也不需要关闭源站证书验证。域名应指向对应 Tunnel，由 Cloudflare 管理域名路由；不能仅将公网 DNS A 记录指向私网 10.0.0.113。

若 cloudflared 也运行于容器，localhost 指的是 cloudflared 自己。将它接入本 Compose 默认网络，并使用 `http://broker:9001`；不要为了容器互通把 9001 映射到公网。此项目未创建或修改你的 Tunnel，也不包含 Tunnel token；需在现有 Tunnel 中完成上述路由。

WSS 设备信任 Cloudflare 公网证书链对应的公共根 CA，并发送域名 SNI；不要使用本项目的私有 CA 验证公网入口。不要开启需要浏览器登录的 Cloudflare Access 策略，除非下位机已实现对应机器认证。MQTT 用户名/密码及 Topic ACL 仍由 Mosquitto 校验。

普通 Tunnel 的 TCP 路由需要客户端 cloudflared 或其他受管客户端，不能让普通 MCU 直接连接域名 8883。参考 [Tunnel 协议说明](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/protocols/)。WebSocket 会断线，设备必须实现退避重连和重传，不能假设连接永久有效。

## 证书与升级

服务器证书 `.secrets/mosquitto/server.crt`，私钥 `server.key`；信任根 `ca.crt`，签发私钥 `ca.key`。只将 CA 公共证书交给下位机。当前服务器证书有效期为 2026-09-29 08:05:32 UTC 至 2027-09-29 08:05:32 UTC，SAN 包括域名、10.0.0.113、localhost、127.0.0.1、broker。

```sh
MQTT_CERT_DNS_NAME=mqtt.web4sachika.asia MQTT_CERT_IP=10.0.0.113 ./scripts/renew-server-cert.sh
docker compose up -d --force-recreate broker
```

续签脚本保留 CA，将旧服务器文件备份到 `.secrets/mosquitto/backup-*`；重建 broker 才能替换容器中的文件挂载并加载新证书，期间会断开连接，后端自动重连。CA 不变通常无需更新设备信任根。请自行安排到期前续签，当前没有自动续期。

`.secrets` 及证书目录权限为 700；为让非 root 容器读取 Compose 文件型 secret，被挂载的文件为 644，CA 私钥仍为 600。目录必须保持私有，不能移动到可公开遍历的位置。整个 `.secrets` 应做受控加密备份；Docker 管理权限本身可以读取容器 secret。

Broker 的账户、ACL 和 MQTT 会话使用原 `mosquitto-data` 卷，数据库使用 `postgres-data` 卷。升级部署时先停止整个组合，再构建并启动新版本；普通 `down` 不删除命名卷，不要使用 `down -v`：

```sh
docker compose down
docker compose up -d --build
```

该流程会短暂中断 MQTT 接入，不会轮换设备密钥。数据库仅在 Compose 内网监听 5432，没有宿主机端口映射。

## 验证

`go test -race ./...`、`go vet ./...`、`docker compose config --quiet` 用于本地检查。项目原有的 Broker 集成测试会重启 Broker，应在维护窗口停止 backend 后执行，避免抢占接收器身份。

PostgreSQL 集成测试仅在显式设置 `PROJECT01_TEST_DATABASE_URL` 时运行，必须指向**独立测试数据库**；测试会执行迁移并新增测试设备与历史，不要指向生产库。`TestPostgresContainerRecreation` 使用 `PROJECT01_TEST_RESTART_PHASE=seed` 与 `check` 两阶段，并在两阶段之间普通重建隔离数据库容器、保留同一测试卷。测试库验证不需要删除现有卷。

公网路由配置完成后，用支持 MQTT/WSS 的设备或客户端，携带已注册设备密钥连接域名 443，并检查后端接收事件及 history。仅看到 TLS 握手或 WebSocket 101 不等于 MQTT 认证和业务存储成功。

## PostgreSQL 结构与迁移

后端使用 `pgx/v5`（PostgreSQL 驱动和最多 10 个连接的连接池），没有 ORM。`db_password` 由 `scripts/create-local-secrets.sh` 生成并作为文件型 Secret 注入；不把密码放进 `.env`。首次启动及后续升级自动按文件名顺序执行 `migrations/*.sql`，用 `schema_migrations` 记录已应用版本，同一事务和数据库 advisory lock 避免并发迁移。迁移错误会阻止后端启动，不会重建表。数据库启动等待最长 60 秒，运行时查询和遥测事务最长 5 秒；断连时查询报错、遥测不 ACK 并触发重连，恢复后连接池重新建立连接。设备生命周期的每一步最长 8 秒。

`0001` 建立设备和历史表；`0002` 给已部署的 stage4 数据库增加密钥重置门禁；`0003` 新增逐设备指标身份及分项 Latest，并按每个指标的采样时间、接收时间、消息 ID 从原始历史重建最新状态，同时排除已删除设备；`0004` 清除旧版 `0003` 曾为墓碑设备重建的 Latest；`0005` 删除没有 Latest 或历史引用的旧默认指标定义；`0006` 给逐指标 Latest 增加默认 false 的 `modifiable`，并新增命令 outbox 与结果异常表。旧迁移不修改；升级保留设备、密钥摘要、历史、去重键、Latest 和数据卷，墓碑设备不会重获 Latest。命令内容由数据库触发器保护为不可变，结果、发送尝试和状态单独更新。旧快照列保留用于迁移/回退参考，但新读写只使用 `device_metric_latest`。迁移期限由独立的 `MIGRATION_TIMEOUT` 配置，默认 15 分钟，使用 Go duration 格式（例如 `30m`）；普通查询仍为 5 秒。Compose 默认 backend 健康检查 `start_period` 为 16 分钟，若增加迁移期限也应同步调整此值。协议校验拒绝 PostgreSQL 无法存储的 NUL `message_id` 和含首尾空白的 ID；仓储仅把已知的文本编码错误 `SQLSTATE 22021` 归为永久输入错误。连接失败、超时、生命周期操作待完成及提交结果不确定都保留 MQTT 消息等待重投；启用操作期间 Broker 先放行的消息不会因数据库仍处于 pending 状态而被永久 ACK 丢弃。

Compose 配置 `DB_HOST=db`、`DB_PASSWORD_FILE=/run/secrets/db_password`、`MIGRATIONS_DIR=/app/migrations`；`MAX_METRICS_PER_DEVICE` 默认为 10，可配置为 1 到 100，限制每台设备已绑定的指标身份总数。segment key 编号范围固定为 `segment-1` 至 `segment-100`，每设备上限另行限制身份数量。新设备首次合法上报某个 segment 时会在提交事务中创建 key/unit 绑定，无需 CLI 预登记；升级后仍被旧历史引用的指标身份保留并计入容量。新设备创建时不预置 temperature、pressure、current，设备 `list/get` 也不返回 `metric_definitions`。数据库默认端口为 5432，可通过 `DB_PORT` 调整。数据库名和用户均为 `project01`。此连接只在 Compose 内网使用，当前未配置数据库 TLS；宿主机调试须临时将数据库端口仅绑定到 `127.0.0.1`，不要直接开放到局域网或公网。

`devices` 保存配置、SHA-256 密钥摘要、删除墓碑、待恢复操作、重置门禁和设备级 `last_valid_received_at`。内部 `device_metrics` 表记录已绑定的稳定 key 和单位；新设备仅在首次成功上报 `segment-N` 时建立记录，不提供 CLI 管理或设备列表输出。未上报过的 segment 没有绑定行。首次合法上报时，仓储在锁住的设备事务中检查身份总数上限，再同时写入新绑定、历史消息和每项 Latest。`device_metric_latest` 为每个已采样指标单独保存 value、unit、modifiable、采样/接收时间和 message ID；新增列默认 false，旧 Latest 安全地不可修改。新旧 telemetry 混合时，modifiable 与其他 Latest 字段按同一决胜顺序原子更新。`telemetry_samples` 保存每条消息的原始指标子集、`sampled_at`、`received_at`，主键 `(device_id, message_id)` 负责跨重启幂等；JSON 不补写旧消息缺少的 modifiable。分项 Latest 更新、首次绑定、去重和历史插入在同一事务内，设备行锁保证并发首次上报一致。按设备及采样时间、全局采样时间分别建历史索引；所有历史查询在数据库侧限定闭区间及最多 1000 条、最长 7 天，先选时间范围内最新 N 条，再按采样时间、接收时间、消息 ID、设备 ID 升序返回。删除设备保留 `devices` 墓碑、内部指标身份与关联历史，但清除密钥摘要和 Latest；ID 永不可复用。数据保留期尚未确定，目前不自动清理历史。

`device_commands` 以 `(device_id, command_id)` 唯一保存不可变命令内容、创建时间、尝试次数/时间、Broker 确认时间、结果接收时间和状态。部分索引保证同一设备同一指标最多一个 waiting、broker_acked 或 result_unknown 命令。后端先提交 outbox，再用业务账户 QoS 1 发布；PUBACK 只置 `broker_acked`。默认最多 5 次发送尝试、5 秒起指数退避（上限 60 秒），命令期限 10 分钟；达到次数上限后停止重发，仍留出结果期限。期限结束时，至少尝试过一次发布的命令置 `result_unknown`，允许有效迟到结果完成确认；从未尝试发送的命令置 `cancelled`。设备停用/生命周期过渡时停止发送和重试，期限仍按创建时间计算。删除设备会取消未解决命令。单实例后端使用 PostgreSQL 设备级 advisory lock 与 lifecycle 操作串行，发送前再次检查启用状态、过渡状态、Latest 能力及已配置范围。

结果只更新命令记录，经过事务提交后才 ACK；它不会写 Latest 或遥测历史。`applied` 表示设备报告已执行，实际目标数值仍须由后续 telemetry 观察，数值相等也没有严格因果关联。数据库故障时结果不 ACK 并触发重连；未知 command_id、错误 Topic 设备身份、错误 key/value 和冲突终态结果写入 `command_result_anomalies`。原始异常结果最多保留 64 KiB。

时间在校验后统一转 UTC、截到微秒（PostgreSQL `timestamptz` 的精度）；查询边界使用用户给定的时间。`message_id` 和 `device_id` 使用 PostgreSQL `C` 排序规则，其 UTF-8 字节顺序与 Go 字符串字典序一致。每项 Latest 独立按采样时间、接收时间、消息 ID 排序；每次合法非重复写入都更新设备 `last_valid_received_at` 的最大值，即使它是乱序旧采样。历史不会补齐本条缺少的指标。只在值变化时发布的设备不能依靠最后接收时间判断在线；后续可将一个保留的 segment 槽位固定用作心跳（单位 `1`），按设备配置周期发送最小消息；最后有效接收时间超过 3 个配置周期才判离线。未配置周期或从未收到消息时状态未知。当前阶段不实现该在线判断，也不能自动登记 `heartbeat` 这样的非 segment key。

## 跨数据库与 Broker 的操作恢复

设备 Broker 角色只授予自身 telemetry/command_result 发布与自身 command 订阅/接收。业务接收角色只授予 telemetry/result 通配订阅和接收，以及 `factory/+/command` 发布；管理账户不承载业务命令。后端启动和运行时会幂等升级旧设备及旧业务角色 ACL，遇到部分成功会重试；升级保留账户、密钥及启用状态，并拒绝意外角色、组和额外权限。`broker-init` 运行 `BootstrapReceiver`，修复中断的后端角色升级可重新运行 `./scripts/init-broker.sh`。

数据库与 Dynamic Security 没有共同事务。后端先写 `pending_operation` 并关闭该设备在数据库侧的接收，再向 Broker 发命令，成功后清除 pending。启动和运行中每 10 秒扫描 pending：中断的新增撤销 Broker 账户并留下 ID 墓碑；删除继续删除；启用、禁用或重置中断则将 Broker 和数据库都置为禁用。重置中断还会持久设置 `requires_secret_reset`；数据库约束和服务层都禁止直接启用，直到新密钥在 Broker 与数据库两端完成同步。无法联系 Broker 时 pending 保留并下次重试；单个账户角色或权限冲突会单独报告，不阻塞其他设备恢复，也不会清理其他账户。修复冲突后重新执行 `reset-secret`，只交付本次显示的密钥，再显式 `enable`。管理响应或数据库提交状态不确定时不交付新密钥；若最终提交实际上已成功而密钥未显示，操作员可再执行 `reset-secret`。正常启动不重建或轮换已有设备账户。部署只运行一个后端实例和 CLI；多实例生命周期协调尚未支持。

旧内存版没有自动数据迁移。旧进程退出后丢失的配置、摘要、历史和去重键无法从 Broker 摘要还原。升级前保存需要的设备清单；升级后重新 `add` 旧 ID 会验证既有专属角色并轮换 Broker 密钥，必须把新密钥安全交给设备。旧历史无法自动补录。

## 备份与恢复

需要一致的数据库、Broker 卷及 `.secrets/` 快照。维护窗口先停止后端与 Broker，避免两边继续写入；数据库保持运行：

```sh
mkdir -p backups
chmod 700 backups
docker compose stop backend broker
docker compose exec -T db pg_dump -U project01 -d project01 -Fc > backups/project01.dump
docker run --rm --volumes-from "$(docker compose ps -a -q broker)" -v "$PWD/backups:/backup" alpine:3.23.3 tar -C /mosquitto/data -czf /backup/mosquitto-data.tar.gz .
tar -czf backups/secrets.tar.gz .secrets
chmod 600 backups/*
docker compose up -d broker backend
```

将备份加密并异地保管，定期在隔离环境演练。以下恢复命令只用于使用相同 Compose 项目名的**新部署空卷**；先恢复 `.secrets/`，再创建尚未启动的 Broker 容器并导入卷快照，启动 db 并导入转储，最后启动其余服务：

```sh
tar -xzf backups/secrets.tar.gz
docker compose create broker
docker run --rm --volumes-from "$(docker compose ps -a -q broker)" -v "$PWD/backups:/backup:ro" alpine:3.23.3 tar -C /mosquitto/data -xzf /backup/mosquitto-data.tar.gz
docker compose up -d db
cat backups/project01.dump | docker compose exec -T db pg_restore -U project01 -d project01 --clean --if-exists --no-owner
docker compose up -d broker broker-init backend
```

恢复目标须使用与快照对应的 Broker 卷、数据库转储及 Secrets；若只恢复一侧，设备可能无法认证或被错误拒收。不要执行 `docker compose down -v` 或删除现有卷来验证备份。恢复后用原设备密钥连接并检查 `get/history`，再发新 ID 与重复 ID 验证接收及去重。
