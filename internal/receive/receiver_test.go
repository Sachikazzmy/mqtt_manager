package receive

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
	"github.com/eclipse/paho.golang/paho"
)

func TestReadinessTracksSubscriptionEvenWhenEventsAreFull(t *testing.T) {
	receiver, _, _ := newTestReceiver(t, 1)
	receiver.config.ReadinessFile = filepath.Join(t.TempDir(), "ready")
	for len(receiver.events) < cap(receiver.events) {
		receiver.events <- Event{Type: "notice"}
	}
	for _, status := range []string{"connecting", "online", "offline", "online"} {
		receiver.emit(Event{Type: "broker", Status: status})
		_, err := os.Stat(receiver.config.ReadinessFile)
		if status == "online" && err != nil {
			t.Fatalf("online marker: %v", err)
		}
		if status != "online" && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s must remove marker: %v", status, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	receiver.Start(ctx)
	receiver.Close()
	if _, err := os.Stat(receiver.config.ReadinessFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shutdown must remove marker: %v", err)
	}
}

type transientRepository struct{}

func (transientRepository) Commit(context.Context, string, string, telemetry.Sample) error {
	return errors.New("temporary storage failure")
}

func (transientRepository) CommitFromBroker(context.Context, string, telemetry.Sample) error {
	return errors.New("temporary storage failure")
}

func (transientRepository) History(context.Context, telemetry.HistoryQuery) ([]telemetry.Sample, error) {
	return nil, errors.New("temporary storage failure")
}

type transitionRepository struct{}

func (transitionRepository) Commit(context.Context, string, string, telemetry.Sample) error {
	return telemetry.ErrDeviceTransition
}

func (transitionRepository) CommitFromBroker(context.Context, string, telemetry.Sample) error {
	return telemetry.ErrDeviceTransition
}

func (transitionRepository) History(context.Context, telemetry.HistoryQuery) ([]telemetry.Sample, error) {
	return nil, telemetry.ErrDeviceTransition
}

func receiverTestServices(t *testing.T) (*device.Service, *telemetry.Service, string) {
	t.Helper()
	store := storage.NewMemoryStore()
	devices := device.NewService(store)
	secret, err := devices.Create(context.Background(), "device-001", "test")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	service := telemetry.NewServiceWithClock(store, func() time.Time { return now }, telemetry.Config{
		MaxFutureSkew: 5 * time.Minute,
	})
	return devices, service, secret
}

func receiverTestPayload(messageID string) []byte {
	value, _ := json.Marshal(telemetry.Message{
		Version:   "1",
		DeviceID:  "device-001",
		MessageID: messageID,
		SampledAt: time.Date(2026, 9, 27, 11, 59, 0, 0, time.UTC),
		Metrics: map[string]telemetry.MetricValue{
			"segment-1": {Value: 0, Unit: "V"},
		},
	})
	return value
}

func newTestReceiver(t *testing.T, queueSize int) (*Receiver, *device.Service, *telemetry.Service) {
	t.Helper()
	devices, telemetryService, _ := receiverTestServices(t)
	receiver, err := NewReceiver(Config{QueueSize: queueSize}, telemetryService)
	if err != nil {
		t.Fatal(err)
	}
	return receiver, devices, telemetryService
}

func TestProcessDeliveryAcknowledgesOnlyAfterStorageCommit(t *testing.T) {
	receiver, _, telemetryService := newTestReceiver(t, 4)
	acked := false
	receiver.processDelivery(context.Background(), delivery{
		packet: &paho.Publish{Topic: "factory/device-001/telemetry", Payload: receiverTestPayload("received"), QoS: 1},
		ack: func() error {
			acked = true
			history, err := telemetryService.History(context.Background(), telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
			if err != nil || len(history) != 1 {
				t.Fatalf("ACK 前应已提交历史: count=%d err=%v", len(history), err)
			}
			return nil
		},
	})
	if !acked {
		t.Fatal("成功存储后没有 MQTT ACK")
	}
	event := <-receiver.Events()
	if event.Type != "accepted" || event.DeviceID != "device-001" || event.MessageID != "received" || event.Metrics["segment-1"].Value != 0 {
		t.Fatalf("接收事件 = %#v", event)
	}
}

func TestProcessDeliveryClassifiesDuplicateRetainedAndRejected(t *testing.T) {
	receiver, _, telemetryService := newTestReceiver(t, 8)
	ctx := context.Background()
	first := receiverTestPayload("same")
	if _, err := telemetryService.ReceiveFromBroker(ctx, "factory/device-001/telemetry", first); err != nil {
		t.Fatal(err)
	}
	var acknowledgements int
	ack := func() error { acknowledgements++; return nil }
	receiver.processDelivery(ctx, delivery{packet: &paho.Publish{Topic: "factory/device-001/telemetry", Payload: first, QoS: 1}, ack: ack})
	receiver.processDelivery(ctx, delivery{packet: &paho.Publish{Topic: "factory/device-001/telemetry", Payload: receiverTestPayload("retained"), QoS: 1, Retain: true}, ack: ack})
	receiver.processDelivery(ctx, delivery{packet: &paho.Publish{Topic: "factory/device-001/telemetry", Payload: []byte("{}"), QoS: 1}, ack: ack})
	receiver.processDelivery(ctx, delivery{packet: &paho.Publish{Topic: "factory/device-001/telemetry", Payload: receiverTestPayload("bad\x00id"), QoS: 1}, ack: ack})
	if acknowledgements != 4 {
		t.Fatalf("重复、retained 和永久拒收消息都应确认，ACK 数=%d", acknowledgements)
	}
	for _, expected := range []string{"duplicate", "reject", "reject", "reject"} {
		if event := <-receiver.Events(); event.Type != expected {
			t.Fatalf("事件类型=%q，期望=%q: %#v", event.Type, expected, event)
		}
	}
	history, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil || len(history) != 1 {
		t.Fatalf("重复、retained 或拒收消息不应写历史: count=%d err=%v", len(history), err)
	}
}

func TestProcessDeliveryLeavesTransientAndCanceledMessagesUnacknowledged(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	service := telemetry.NewServiceWithClock(transientRepository{}, func() time.Time { return now }, telemetry.Config{
		MaxFutureSkew: 5 * time.Minute,
	})
	receiver, err := NewReceiver(Config{QueueSize: 4}, service)
	if err != nil {
		t.Fatal(err)
	}
	var acknowledgements int
	disconnected := false
	item := delivery{
		packet:     &paho.Publish{Topic: "factory/device-001/telemetry", Payload: receiverTestPayload("transient"), QoS: 1},
		ack:        func() error { acknowledgements++; return nil },
		disconnect: func() { disconnected = true },
	}
	receiver.processDelivery(context.Background(), item)
	if acknowledgements != 0 || !disconnected {
		t.Fatalf("暂时存储错误应不 ACK 并断开重连: ack=%d disconnected=%v", acknowledgements, disconnected)
	}
	if event := <-receiver.Events(); event.Type != "retry" {
		t.Fatalf("暂时存储事件 = %#v", event)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	receiver.processDelivery(canceled, item)
	if acknowledgements != 0 {
		t.Fatalf("退出期间不得确认未处理消息，实际=%d", acknowledgements)
	}
}

func TestProcessDeliveryRetriesLifecycleTransitionWithoutAcknowledging(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	service := telemetry.NewServiceWithClock(transitionRepository{}, func() time.Time { return now }, telemetry.Config{
		MaxFutureSkew: 5 * time.Minute,
	})
	receiver, err := NewReceiver(Config{QueueSize: 4}, service)
	if err != nil {
		t.Fatal(err)
	}
	acknowledgements := 0
	disconnected := false
	receiver.processDelivery(context.Background(), delivery{
		packet:     &paho.Publish{Topic: "factory/device-001/telemetry", Payload: receiverTestPayload("transition"), QoS: 1},
		ack:        func() error { acknowledgements++; return nil },
		disconnect: func() { disconnected = true },
	})
	if acknowledgements != 0 || !disconnected {
		t.Fatalf("生命周期过渡期间必须保持未 ACK 并触发重投: ack=%d disconnected=%v", acknowledgements, disconnected)
	}
	if event := <-receiver.Events(); event.Type != "retry" {
		t.Fatalf("生命周期过渡事件应可重试: %#v", event)
	}
}

func TestQueueFullKeepsDeliveryUnacknowledgedAndSchedulesReconnect(t *testing.T) {
	receiver, _, _ := newTestReceiver(t, 1)
	firstAcked := false
	first := delivery{
		packet: &paho.Publish{Topic: "factory/device-001/telemetry", QoS: 1},
		ack:    func() error { firstAcked = true; return nil },
	}
	secondAcked := false
	second := delivery{
		packet: &paho.Publish{Topic: "factory/device-001/telemetry", QoS: 1},
		ack:    func() error { secondAcked = true; return nil },
	}
	if !receiver.enqueue(first) || receiver.enqueue(second) {
		t.Fatal("滿載隊列應只接納一條消息")
	}
	if firstAcked || secondAcked {
		t.Fatal("入隊階段不應确认消息")
	}
	select {
	case <-receiver.reconnect:
	default:
		t.Fatal("滿載時應排入重連請求")
	}
	event := <-receiver.Events()
	if event.Type != "queue_full" {
		t.Fatalf("滿載事件 = %#v", event)
	}
}

func TestReceiverReportsQueueAndConfigBoundaries(t *testing.T) {
	_, telemetryService, _ := receiverTestServices(t)
	if _, err := NewReceiver(Config{QueueSize: 0}, telemetryService); err == nil {
		t.Fatal("零大小隊列應拒絕")
	}
	if _, err := NewReceiver(Config{QueueSize: 4097}, telemetryService); err == nil {
		t.Fatal("超大隊列應拒絕")
	}
	if _, err := NewReceiver(Config{QueueSize: 1}, nil); err == nil {
		t.Fatal("nil telemetry service 應拒絕")
	}
}

func TestPermanentRejectionSet(t *testing.T) {
	if !isPermanentRejection(telemetry.ErrInvalidTopic) || !isPermanentRejection(device.ErrNotFound) {
		t.Fatal("协议与未知设备错误应被视为永久拒收")
	}
	if isPermanentRejection(errors.New("temporary storage error")) {
		t.Fatal("未知存储错误不应被视为永久拒收")
	}
}
