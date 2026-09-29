# 阶段 3 部署

当前是单进程内存版。Compose 包括 broker、broker-init（初始化后退出）、backend；不包含数据库、HTTP API 或网页。后端重启、升级或容器重建都会丢失设备配置、去重状态、Latest 和历史，不能作为持久化生产存储。

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

预期 broker、backend healthy，broker-init Exited (0)。backend 的健康标记仅在 MQTT 订阅成功后建立，断连及退出时清除。Docker 健康状态不等于业务数据持久化，也不会自动重启一个仍运行但 unhealthy 的进程。

CLI 中执行：

```text
add device-001 温度设备
get device-001
list
history device-001 20
```

将 add 显示的密钥单独交给对应设备。依次按 Ctrl-P、Ctrl-Q 离开附着终端，接收器继续运行。不要用 `docker compose exec backend server` 或另启宿主机 server；它们会创建另一套内存数据，并与原接收器争抢 ClientID。需要停止时使用 `docker compose stop backend`；直接 quit/EOF/Ctrl-C 会结束 Go，重启策略可能将其重新启动，内存仍会丢失。

后端以 UID/GID 10001 运行，根文件系统只读，只有临时目录可写。其日志驱动为 none，避免一次性设备密钥被 Docker 捕获；`docker compose logs backend` 不提供实时记录，请 attach 查看。Broker 不记录消息正文，仍应限制 Docker 和日志访问权。

另一终端可以模拟设备：

```sh
docker compose exec backend publish -address broker:8883 -server-name broker -ca /run/secrets/mqtt_ca_cert -device device-001
```

按提示输入设备密钥。PUBACK 仅表示 Broker 确认；在附着终端和 get/history 中确认业务接收。

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

Broker 的账户、ACL 和 MQTT 会话使用原 `mosquitto-data` 卷。普通 down/up 不删除卷，禁止用 down -v 清理部署。后端升级运行 `docker compose up -d --build backend`；内存丢失后重新 add 原设备 ID 会轮换密钥，需要重新配置设备。保留 Broker 数据卷不能恢复 Go 内存数据。

## 验证

`go test -race ./...`、`go vet ./...`、`docker compose config --quiet` 用于本地检查。项目原有的 Broker 集成测试会重启 Broker，应在维护窗口停止 backend 后执行，避免抢占接收器身份。

公网路由配置完成后，用支持 MQTT/WSS 的设备或客户端，携带已注册设备密钥连接域名 443，并检查后端接收事件及 history。仅看到 TLS 握手或 WebSocket 101 不等于 MQTT 认证和业务存储成功。
