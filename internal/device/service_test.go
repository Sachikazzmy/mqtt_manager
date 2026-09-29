package device_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
)

type brokerStub struct {
	createErr    error
	stateErr     error
	resetErr     error
	deleteErr    error
	createdID    string
	createdKey   string
	resetEnabled bool
	resetCalls   int
	deleteCalls  int
	store        *storage.MemoryStore
	onState      func(context.Context, string, bool)
}

func (b *brokerStub) CreateDevice(ctx context.Context, id, secret string) error {
	b.createdID, b.createdKey = id, secret
	if b.store != nil {
		if _, err := b.store.Get(ctx, id); !errors.Is(err, device.ErrNotFound) {
			return errors.New("Broker 账户应在本地设备创建之前配置")
		}
	}
	return b.createErr
}

func (b *brokerStub) SetDeviceEnabled(ctx context.Context, id string, enabled bool) error {
	if b.onState != nil {
		b.onState(ctx, id, enabled)
	}
	return b.stateErr
}

func (b *brokerStub) ResetDeviceSecret(_ context.Context, _ string, _ string, enabled bool) error {
	b.resetCalls++
	b.resetEnabled = enabled
	return b.resetErr
}

func (b *brokerStub) DeleteDevice(context.Context, string) error {
	b.deleteCalls++
	return b.deleteErr
}

func TestManagedCreateSyncsBrokerBeforeLocalAndDoesNotReturnFailedSecret(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	broker := &brokerStub{store: store}
	service := device.NewManagedService(store, broker)
	secret, err := service.Create(ctx, "device-001", "test")
	if err != nil {
		t.Fatal(err)
	}
	if secret == "" || broker.createdKey != secret || broker.createdID != "device-001" {
		t.Fatal("成功注册应同步同一密钥并返回一次性密钥")
	}

	failedStore := storage.NewMemoryStore()
	failedBroker := &brokerStub{createErr: errors.New("broker offline")}
	failedService := device.NewManagedService(failedStore, failedBroker)
	failedSecret, err := failedService.Create(ctx, "device-002", "test")
	if err == nil || failedSecret != "" {
		t.Fatalf("Broker 同步失败不应返回密钥: secret=%q err=%v", failedSecret, err)
	}
	if _, err := failedService.Get(ctx, "device-002"); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("Broker 注册失败后不应开放本地设备: %v", err)
	}
}

func TestManagedEnableKeepsIngressClosedUntilBrokerConfirms(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	broker := &brokerStub{store: store}
	service := device.NewManagedService(store, broker)
	if _, err := service.Create(ctx, "device-001", "test"); err != nil {
		t.Fatal(err)
	}
	if err := service.Disable(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}

	receiveTime := time.Date(2026, 9, 29, 4, 15, 30, 0, time.UTC)
	receiver := telemetry.NewServiceWithClock(store, func() time.Time { return receiveTime }, telemetry.Config{
		MaxFutureSkew: telemetry.DefaultMaxFutureSkew,
		MetricRules:   telemetry.DefaultMetricRules(),
	})
	var receiveErr error
	broker.onState = func(ctx context.Context, id string, enabled bool) {
		if !enabled {
			return
		}
		payload := []byte(fmt.Sprintf(`{"version":"1","device_id":%q,"message_id":"during-enable","sampled_at":%q,"metrics":{"temperature":{"value":23.6,"unit":"C"}}}`, id, receiveTime.Add(-time.Second).Format(time.RFC3339Nano)))
		_, receiveErr = receiver.ReceiveFromBroker(ctx, "factory/"+id+"/telemetry", payload)
	}
	broker.stateErr = errors.New("broker enable rejected")

	if err := service.Enable(ctx, "device-001"); !errors.Is(err, broker.stateErr) {
		t.Fatalf("Broker 启用失败未向调用方传播: %v", err)
	}
	if !errors.Is(receiveErr, telemetry.ErrDeviceDisabled) {
		t.Fatalf("Broker 确认前本地应拒收遥测: %v", receiveErr)
	}
	history, err := receiver.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("启用失败期间不应保存历史: %#v", history)
	}
	current, err := service.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled {
		t.Fatal("Broker 启用失败后设备必须保持禁用")
	}

	broker.stateErr = nil
	if err := service.Enable(ctx, "device-001"); err != nil {
		t.Fatalf("Broker 确认启用后本地提交失败: %v", err)
	}
	confirmedPayload := []byte(fmt.Sprintf(`{"version":"1","device_id":"device-001","message_id":"after-enable","sampled_at":%q,"metrics":{"temperature":{"value":23.6,"unit":"C"}}}`, receiveTime.Add(-time.Second).Format(time.RFC3339Nano)))
	if _, err := receiver.ReceiveFromBroker(ctx, "factory/device-001/telemetry", confirmedPayload); err != nil {
		t.Fatalf("Broker 确认后本地接收门应开放: %v", err)
	}
}

func TestManagedDisableClosesIngressBeforeBrokerRevocation(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	broker := &brokerStub{store: store}
	service := device.NewManagedService(store, broker)
	if _, err := service.Create(ctx, "device-001", "test"); err != nil {
		t.Fatal(err)
	}
	receiveTime := time.Date(2026, 9, 29, 4, 15, 30, 0, time.UTC)
	receiver := telemetry.NewServiceWithClock(store, func() time.Time { return receiveTime }, telemetry.Config{
		MaxFutureSkew: telemetry.DefaultMaxFutureSkew,
		MetricRules:   telemetry.DefaultMetricRules(),
	})
	var receiveErr error
	broker.onState = func(ctx context.Context, id string, enabled bool) {
		if enabled {
			return
		}
		payload := []byte(fmt.Sprintf(`{"version":"1","device_id":%q,"message_id":"during-disable","sampled_at":%q,"metrics":{"temperature":{"value":23.6,"unit":"C"}}}`, id, receiveTime.Add(-time.Second).Format(time.RFC3339Nano)))
		_, receiveErr = receiver.ReceiveFromBroker(ctx, "factory/"+id+"/telemetry", payload)
	}
	broker.stateErr = errors.New("unexpected group permission")

	if err := service.Disable(ctx, "device-001"); !errors.Is(err, broker.stateErr) {
		t.Fatalf("Broker 撤权错误未传播: %v", err)
	}
	if !errors.Is(receiveErr, telemetry.ErrDeviceDisabled) {
		t.Fatalf("Broker 撤权期间本地应先拒收遥测: %v", receiveErr)
	}
	current, err := service.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled {
		t.Fatal("Broker 撤权失败后本地接收仍必须关闭")
	}
}

func TestManagedResetKeepsDeviceDisabledWhenBrokerStateIsUncertain(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	broker := &brokerStub{store: store}
	service := device.NewManagedService(store, broker)
	oldSecret, err := service.Create(ctx, "device-001", "test")
	if err != nil {
		t.Fatal(err)
	}
	broker.resetErr = device.ErrBrokerStateUncertain
	newSecret, err := service.ResetSecret(ctx, "device-001")
	if err == nil || newSecret != "" || !errors.Is(err, device.ErrBrokerStateUncertain) {
		t.Fatalf("不确定重置不应返回密钥: secret=%q err=%v", newSecret, err)
	}
	current, err := service.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled {
		t.Fatal("Broker 状态不确定时本地设备必须保持禁用")
	}
	if broker.resetCalls != 1 || !broker.resetEnabled {
		t.Fatalf("Broker 重置参数: calls=%d enabled=%v", broker.resetCalls, broker.resetEnabled)
	}
	if oldSecret == "" {
		t.Fatal("测试预期已创建旧密钥")
	}
}

func TestManagedResetRestoresEnableStateOnDefiniteFailure(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	broker := &brokerStub{store: store}
	service := device.NewManagedService(store, broker)
	if _, err := service.Create(ctx, "device-001", "test"); err != nil {
		t.Fatal(err)
	}
	broker.resetErr = errors.New("rejected before change")
	if secret, err := service.ResetSecret(ctx, "device-001"); err == nil || secret != "" {
		t.Fatalf("失敗重置结果: secret=%q err=%v", secret, err)
	}
	current, err := service.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if !current.Enabled {
		t.Fatal("明确失败时应恢复设备原启用状态")
	}
}

func TestManagedResetPreservesDisabledStateAndDeleteFailureBlocksIngress(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	broker := &brokerStub{store: store}
	service := device.NewManagedService(store, broker)
	if _, err := service.Create(ctx, "device-001", "test"); err != nil {
		t.Fatal(err)
	}
	if err := service.Disable(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ResetSecret(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	if broker.resetEnabled {
		t.Fatal("禁用设备重置密钥后不应在 Broker 重新启用")
	}
	broker.deleteErr = device.ErrBrokerStateUncertain
	if err := service.Delete(ctx, "device-001"); !errors.Is(err, device.ErrBrokerStateUncertain) {
		t.Fatalf("删除失败错误 = %v", err)
	}
	current, err := service.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled || broker.deleteCalls != 1 {
		t.Fatalf("不确定删除后状态 enabled=%v deleteCalls=%d", current.Enabled, broker.deleteCalls)
	}
}

func TestDeviceIDRejectsBrokerReservedAndUnsafeCharacters(t *testing.T) {
	service := device.NewService(storage.NewMemoryStore())
	for _, id := range []string{"admin", "contains/slash", "wildcard+", "con\ntrol", "_leading"} {
		if secret, err := service.Create(context.Background(), id, "test"); !errors.Is(err, device.ErrInvalidID) || secret != "" {
			t.Errorf("编号 %q 应拒绝: secret=%q err=%v", id, secret, err)
		}
	}
}
