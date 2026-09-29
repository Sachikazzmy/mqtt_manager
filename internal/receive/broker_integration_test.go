package receive_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"Project/internal/cli"
	"Project/internal/device"
	"Project/internal/receive"
	"Project/internal/storage"
	"Project/internal/telemetry"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

func TestBrokerEndToEndDeviceToCLIQueries(t *testing.T) {
	if os.Getenv("PROJECT01_MQTT_E2E") != "1" {
		t.Skip("set PROJECT01_MQTT_E2E=1 to use the local Mosquitto broker")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	config, err := receive.LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	manager := receive.NewManager(config)
	if err := manager.BootstrapReceiver(ctx); err != nil {
		t.Fatal(err)
	}

	store := storage.NewMemoryStore()
	devices := device.NewManagedService(store, manager)
	telemetryService := telemetry.NewService(store)
	id, err := integrationID("e2e-")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := devices.Create(ctx, id, "临时集成设备")
	if err != nil {
		t.Fatal(err)
	}
	var terminal bytes.Buffer
	commands := cli.New(devices, telemetryService, &terminal, nil)
	deleted := false
	defer func() {
		if !deleted {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_ = manager.DeleteDevice(cleanupCtx, id)
		}
	}()

	receiver, err := receive.NewReceiver(config, telemetryService)
	if err != nil {
		t.Fatal(err)
	}
	receiver.Start(ctx)
	defer receiver.Close()
	if err := commands.PrintEvent(waitForBrokerEvent(t, ctx, receiver.Events(), "online", "")); err != nil {
		t.Fatal(err)
	}

	if _, err := connectIntegrationPublisher(ctx, config, id, "incorrect-secret"); err == nil {
		t.Fatal("错误设备密钥不应通过 Broker 认证")
	}
	publisher, err := connectIntegrationPublisher(ctx, config, id, secret)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Disconnect(&paho.Disconnect{})

	firstPayload := integrationPayload(id, "message-001", time.Now().UTC(), 21.5)
	response, err := integrationPublish(ctx, publisher, "factory/other-device/telemetry", firstPayload)
	if err == nil && (response == nil || response.ReasonCode < 0x80) {
		t.Fatal("设备账户不应获准向其他设备 Topic 发布")
	}
	response, err = integrationPublish(ctx, publisher, "factory/"+id+"/telemetry", firstPayload)
	if err != nil || response == nil || response.ReasonCode >= 0x80 {
		t.Fatalf("合法 QoS 1 发布失败: response=%#v err=%v", response, err)
	}
	if err := commands.PrintEvent(waitForBrokerEvent(t, ctx, receiver.Events(), "accepted", "message-001")); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"get " + id, "list", "history " + id + " 10"} {
		quit, err := commands.Execute(ctx, command)
		if err != nil || quit {
			t.Fatalf("CLI 查询 %q 失败: quit=%v err=%v", command, quit, err)
		}
	}
	if !bytes.Contains(terminal.Bytes(), []byte(`"message_id": "message-001"`)) || !bytes.Contains(terminal.Bytes(), []byte(id)) {
		t.Fatalf("CLI get/list/history 未展示上报数据: %s", terminal.String())
	}

	response, err = integrationPublish(ctx, publisher, "factory/"+id+"/telemetry", firstPayload)
	if err != nil || response == nil || response.ReasonCode >= 0x80 {
		t.Fatalf("重复消息发布失败: response=%#v err=%v", response, err)
	}
	if err := commands.PrintEvent(waitForBrokerEvent(t, ctx, receiver.Events(), "duplicate", "message-001")); err != nil {
		t.Fatal(err)
	}
	history, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: id, Limit: 10})
	if err != nil || len(history) != 1 {
		t.Fatalf("重复消息不应增加历史: 数量=%d err=%v", len(history), err)
	}

	invalidPayload := []byte(`{"version":"1","device_id":"` + id + `","message_id":"message-invalid","sampled_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","metrics":{"temperature":{"value":20,"unit":"kPa"}}}`)
	response, err = integrationPublish(ctx, publisher, "factory/"+id+"/telemetry", invalidPayload)
	if err != nil || response == nil || response.ReasonCode >= 0x80 {
		t.Fatalf("格式错误消息未到达业务校验入口: response=%#v err=%v", response, err)
	}
	if err := commands.PrintEvent(waitForBrokerEvent(t, ctx, receiver.Events(), "reject", "message-invalid")); err != nil {
		t.Fatal(err)
	}
	_ = publisher.Disconnect(&paho.Disconnect{})
	drainBrokerStatusEvents(receiver.Events())
	if err := restartComposeBroker(ctx); err != nil {
		t.Fatal(err)
	}
	if err := commands.PrintEvent(waitForBrokerEvent(t, ctx, receiver.Events(), "online", "")); err != nil {
		t.Fatal(err)
	}

	if err := devices.Disable(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := connectIntegrationPublisher(ctx, config, id, secret); err == nil {
		t.Fatal("禁用设备不应再次连接 Broker")
	}
	if err := devices.Enable(ctx, id); err != nil {
		t.Fatal(err)
	}

	oldPublisher, err := connectIntegrationPublisher(ctx, config, id, secret)
	if err != nil {
		t.Fatal(err)
	}
	newSecret, err := devices.ResetSecret(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldPublisher.Done():
	case <-time.After(time.Second):
		t.Fatal("密钥重置没有断开仍在线的旧设备连接")
	}
	if _, err := connectIntegrationPublisher(ctx, config, id, secret); err == nil {
		t.Fatal("密钥重置后旧密钥仍可连接")
	}
	newPublisher, err := connectIntegrationPublisher(ctx, config, id, newSecret)
	if err != nil {
		t.Fatal(err)
	}
	secondPayload := integrationPayload(id, "message-002", time.Now().UTC(), 22.5)
	response, err = integrationPublish(ctx, newPublisher, "factory/"+id+"/telemetry", secondPayload)
	if err != nil || response == nil || response.ReasonCode >= 0x80 {
		t.Fatalf("新密钥发布失败: response=%#v err=%v", response, err)
	}
	if err := commands.PrintEvent(waitForBrokerEvent(t, ctx, receiver.Events(), "accepted", "message-002")); err != nil {
		t.Fatal(err)
	}
	_ = newPublisher.Disconnect(&paho.Disconnect{})

	if err := devices.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	deleted = true
	if _, err := devices.Get(ctx, id); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("删除后的 get 查询应返回 not found: %v", err)
	}
	history, err = telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: id, Limit: 10})
	if err != nil || len(history) != 2 {
		t.Fatalf("删除设备应保留本进程历史: 数量=%d err=%v", len(history), err)
	}
	if _, err := connectIntegrationPublisher(ctx, config, id, newSecret); err == nil {
		t.Fatal("删除设备后 Broker 应拒绝连接")
	}
	for _, expected := range []string{"接收成功 device_id=" + id + " message_id=message-001", "重复消息 device_id=" + id, "拒收 device_id=" + id} {
		if !bytes.Contains(terminal.Bytes(), []byte(expected)) {
			t.Errorf("终端没有展示预期接收结果 %q:\n%s", expected, terminal.String())
		}
	}
}

func connectIntegrationPublisher(ctx context.Context, config receive.Config, id, secret string) (*paho.Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, config.DialTimeout)
	defer cancel()
	connection, err := (&tls.Dialer{
		NetDialer: &net.Dialer{Timeout: config.DialTimeout},
		Config:    config.TLSConfig.Clone(),
	}).DialContext(dialCtx, "tcp", config.Address)
	if err != nil {
		return nil, err
	}
	client := paho.NewClient(paho.ClientConfig{
		Conn:          packets.NewThreadSafeConn(connection),
		PacketTimeout: config.PacketTimeout,
	})
	clientID, err := integrationID("project01-e2e-")
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	connectCtx, connectCancel := context.WithTimeout(ctx, config.PacketTimeout)
	defer connectCancel()
	if _, err := client.Connect(connectCtx, &paho.Connect{
		ClientID:     clientID,
		Username:     id,
		Password:     []byte(secret),
		UsernameFlag: true,
		PasswordFlag: true,
		CleanStart:   true,
		KeepAlive:    30,
	}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return client, nil
}

func integrationPublish(ctx context.Context, client *paho.Client, topic string, payload []byte) (*paho.PublishResponse, error) {
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return client.Publish(publishCtx, &paho.Publish{
		QoS:     1,
		Topic:   topic,
		Payload: payload,
		Retain:  false,
	})
}

func integrationPayload(id, messageID string, sampledAt time.Time, value float64) []byte {
	payload, _ := json.Marshal(telemetry.Message{
		Version:   "1",
		DeviceID:  id,
		MessageID: messageID,
		SampledAt: sampledAt,
		Metrics: map[string]telemetry.MetricValue{
			"temperature": {Value: value, Unit: "C"},
		},
	})
	return payload
}

func waitForBrokerEvent(t *testing.T, ctx context.Context, events <-chan receive.Event, eventType, messageID string) receive.Event {
	t.Helper()
	var observed []string
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("等待 Broker 事件 %s/%s 超时: %v；已观察: %v", eventType, messageID, ctx.Err(), observed)
		case event, ok := <-events:
			if !ok {
				t.Fatalf("接收器在等待事件 %s/%s 时关闭；已观察: %v", eventType, messageID, observed)
			}
			matched := event.Type == eventType || event.Type == "broker" && event.Status == eventType
			if matched && (messageID == "" || event.MessageID == messageID) {
				return event
			}
			if event.Type == "broker" {
				observed = append(observed, event.Status+" "+event.Reason)
			}
		}
	}
}

func containsDevice(devices []device.Device, id string) bool {
	for _, value := range devices {
		if value.ID == id {
			return true
		}
	}
	return false
}

func TestIntegrationPublishHelpersHaveStableProtocol(t *testing.T) {
	value := integrationPayload("device-001", "id-1", time.Date(2026, 9, 29, 4, 15, 30, 0, time.UTC), 0)
	var message telemetry.Message
	if err := json.Unmarshal(value, &message); err != nil {
		t.Fatal(err)
	}
	if message.Version != "1" || message.DeviceID != "device-001" || message.MessageID != "id-1" || message.Metrics["temperature"].Value != 0 {
		t.Fatalf("集成消息帮助函数协议错误: %s", value)
	}
}

func restartComposeBroker(ctx context.Context) error {
	restartCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	command := exec.CommandContext(restartCtx, "docker", "compose", "restart", "broker")
	command.Dir = "../.."
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("重启 Compose Broker 失败: %w (%s)", err, output)
	}
	return nil
}

func drainBrokerStatusEvents(events <-chan receive.Event) {
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Type != "broker" {
				continue
			}
		default:
			return
		}
	}
}

func integrationID(prefix string) (string, error) {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value), nil
}
