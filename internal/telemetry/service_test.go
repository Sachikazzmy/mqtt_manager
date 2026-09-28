package telemetry_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
)

func testSetup(t *testing.T) (*storage.MemoryStore, *device.Service, *telemetry.Service, string, *time.Time) {
	t.Helper()
	store := storage.NewMemoryStore()
	devices := device.NewService(store)
	secret, err := devices.Create(context.Background(), "device-001", "测试设备")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	clock := &now
	service := telemetry.NewServiceWithClock(store, func() time.Time { return *clock }, telemetry.Config{
		MaxFutureSkew: 5 * time.Minute,
		MetricRules:   telemetry.DefaultMetricRules(),
	})
	return store, devices, service, secret, clock
}

func messagePayload(deviceID, messageID, sampledAt, metrics string) []byte {
	return []byte(fmt.Sprintf(`{"version":"1","device_id":%q,"message_id":%q,"sampled_at":%q,"metrics":%s}`, deviceID, messageID, sampledAt, metrics))
}

func temperaturePayload(messageID, sampledAt string, value string) []byte {
	return messagePayload("device-001", messageID, sampledAt, fmt.Sprintf(`{"temperature":{"value":%s,"unit":"C"}}`, value))
}

func TestReceiveValidatesPayloadAndPreservesZero(t *testing.T) {
	_, devices, service, secret, clock := testSetup(t)
	ctx := context.Background()

	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("zero", "2026-09-27T11:59:00Z", "0")); err != nil {
		t.Fatal(err)
	}
	deviceValue, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if deviceValue.Latest == nil || deviceValue.Latest.Metrics["temperature"].Value != 0 {
		t.Fatalf("Latest 没有保留真实零值: %#v", deviceValue.Latest)
	}

	cases := []struct {
		name    string
		payload []byte
		want    error
	}{
		{
			name:    "missing value",
			payload: messagePayload("device-001", "missing", "2026-09-27T11:59:01Z", `{"temperature":{"unit":"C"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "wrong unit",
			payload: messagePayload("device-001", "unit", "2026-09-27T11:59:02Z", `{"temperature":{"value":1,"unit":"F"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "unknown metric",
			payload: messagePayload("device-001", "metric", "2026-09-27T11:59:03Z", `{"voltage":{"value":1,"unit":"V"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "non finite value",
			payload: messagePayload("device-001", "overflow", "2026-09-27T11:59:04Z", `{"temperature":{"value":1e999,"unit":"C"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := service.Receive(ctx, "device-001", secret, tc.payload); !errors.Is(err, tc.want) {
				t.Fatalf("错误 = %v，期望包含 %v", err, tc.want)
			}
		})
	}

	*clock = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("future", "2026-09-27T12:06:00Z", "1")); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("超前时间错误 = %v", err)
	}
	if err := service.Receive(ctx, "device-001", "wrong-secret", temperaturePayload("secret", "2026-09-27T11:59:04Z", "1")); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("错误密钥错误 = %v", err)
	}
	if err := service.Receive(ctx, "other-device", secret, temperaturePayload("identity", "2026-09-27T11:59:05Z", "1")); !errors.Is(err, telemetry.ErrDeviceMismatch) {
		t.Fatalf("身份不匹配错误 = %v", err)
	}
	if err := service.Receive(ctx, "unknown", secret, messagePayload("unknown", "unknown", "2026-09-27T11:59:05Z", `{"temperature":{"value":1,"unit":"C"}}`)); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("未知设备错误 = %v", err)
	}

	if err := devices.Disable(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("disabled", "2026-09-27T11:59:06Z", "1")); !errors.Is(err, telemetry.ErrDeviceDisabled) {
		t.Fatalf("禁用设备错误 = %v", err)
	}
}

func TestSecretResetInvalidatesOldSecret(t *testing.T) {
	_, devices, service, oldSecret, clock := testSetup(t)
	ctx := context.Background()

	newSecret, err := devices.ResetSecret(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if newSecret == oldSecret {
		t.Fatal("重置后的密钥不应与旧密钥相同")
	}
	*clock = (*clock).Add(time.Second)
	if err := service.Receive(ctx, "device-001", oldSecret, temperaturePayload("old", "2026-09-27T11:59:00Z", "1")); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("旧密钥错误 = %v", err)
	}
	if err := service.Receive(ctx, "device-001", newSecret, temperaturePayload("new", "2026-09-27T11:59:01Z", "2")); err != nil {
		t.Fatal(err)
	}

	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if stringValue := fmt.Sprint(d); stringValue == "" || containsSecret(stringValue, newSecret) || containsSecret(stringValue, oldSecret) {
		t.Fatalf("设备查询不应暴露密钥: %s", stringValue)
	}
}

func TestDuplicateOutOfOrderAndTieBreaking(t *testing.T) {
	_, devices, service, secret, clock := testSetup(t)
	ctx := context.Background()

	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("new", "2026-09-27T11:59:00Z", "10")); err != nil {
		t.Fatal(err)
	}
	*clock = (*clock).Add(time.Second)
	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("old", "2026-09-27T11:00:00Z", "1")); err != nil {
		t.Fatal(err)
	}
	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("old", "2026-09-27T11:00:00Z", "1")); !errors.Is(err, telemetry.ErrDuplicateMessage) {
		t.Fatalf("重复消息错误 = %v", err)
	}

	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest.MessageID != "new" || d.Latest.Metrics["temperature"].Value != 10 {
		t.Fatalf("旧采样覆盖了 Latest: %#v", d.Latest)
	}
	if !d.Latest.LastValidReceivedAt.Equal(*clock) {
		t.Fatalf("旧采样没有推进最后有效接收时间: %v", d.Latest.LastValidReceivedAt)
	}

	*clock = (*clock).Add(time.Second)
	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("same-later", "2026-09-27T11:59:00Z", "20")); err != nil {
		t.Fatal(err)
	}
	d, err = devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest.MessageID != "same-later" {
		t.Fatalf("相同采样时间应由后接收消息胜出: %#v", d.Latest)
	}

	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("same-earlier-id", "2026-09-27T11:59:00Z", "30")); err != nil {
		t.Fatal(err)
	}
	d, err = devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest.MessageID != "same-later" {
		t.Fatalf("相同采样和接收时间应由消息 ID 决胜: %#v", d.Latest)
	}
}

func TestConcurrentDuplicateOnlyCommitsOnce(t *testing.T) {
	_, _, service, secret, _ := testSetup(t)
	ctx := context.Background()
	payload := temperaturePayload("same", "2026-09-27T11:59:00Z", "1")

	const attempts = 32
	errs := make(chan error, attempts)
	var group sync.WaitGroup
	for i := 0; i < attempts; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- service.Receive(ctx, "device-001", secret, payload)
		}()
	}
	group.Wait()
	close(errs)

	var success, duplicate int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, telemetry.ErrDuplicateMessage):
			duplicate++
		default:
			t.Fatalf("并发重复出现意外错误: %v", err)
		}
	}
	if success != 1 || duplicate != attempts-1 {
		t.Fatalf("成功数=%d，重复数=%d", success, duplicate)
	}
}

func TestContextCancellationAndConcurrentLifecycle(t *testing.T) {
	_, devices, service, secret, _ := testSetup(t)
	ctx := context.Background()

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := service.Receive(canceled, "device-001", secret, temperaturePayload("canceled", "2026-09-27T11:59:00Z", "1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消 context 错误 = %v", err)
	}

	// 结果只允许是“接收先提交”或“配置先提交”，不能出现半条记录。
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	var receiveErr, disableErr error
	go func() {
		defer group.Done()
		<-start
		receiveErr = service.Receive(ctx, "device-001", secret, temperaturePayload("disable-race", "2026-09-27T11:59:00Z", "1"))
	}()
	go func() {
		defer group.Done()
		<-start
		disableErr = devices.Disable(ctx, "device-001")
	}()
	close(start)
	group.Wait()
	if disableErr != nil {
		t.Fatal(disableErr)
	}
	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if receiveErr == nil && len(history) != 1 {
		t.Fatalf("接收成功但历史未保存: %#v", history)
	}
	if errors.Is(receiveErr, telemetry.ErrDeviceDisabled) && len(history) != 0 {
		t.Fatalf("禁用先提交后仍写入历史: %#v", history)
	}
	if receiveErr != nil && !errors.Is(receiveErr, telemetry.ErrDeviceDisabled) {
		t.Fatalf("并发禁用出现意外错误: %v", receiveErr)
	}

}

func TestHistoryResourceLimitsAndCancellation(t *testing.T) {
	_, _, service, secret, clock := testSetup(t)
	ctx := context.Background()
	if err := service.Receive(ctx, "device-001", secret, temperaturePayload("history", "2026-09-27T11:59:00Z", "1")); err != nil {
		t.Fatal(err)
	}

	if history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10}); err != nil || len(history) != 1 {
		t.Fatalf("默认历史窗口查询 = %#v, %v", history, err)
	}
	if _, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: telemetry.MaxHistoryLimit + 1}); !errors.Is(err, telemetry.ErrHistoryLimit) {
		t.Fatalf("过大数量限制错误 = %v", err)
	}
	from := (*clock).Add(-telemetry.MaxHistoryWindow - time.Second)
	to := *clock
	if _, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", From: &from, To: &to, Limit: 10}); !errors.Is(err, telemetry.ErrHistoryWindow) {
		t.Fatalf("过大时间范围错误 = %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := service.History(canceled, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消历史查询错误 = %v", err)
	}
}

func TestConcurrentDeleteAndReceiveKeepsOnlyCommittedHistory(t *testing.T) {
	ctx := context.Background()
	_, devices, service, secret, _ := testSetup(t)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	var receiveErr, deleteErr error
	go func() {
		defer group.Done()
		<-start
		receiveErr = service.Receive(ctx, "device-001", secret, temperaturePayload("delete-race", "2026-09-27T11:59:00Z", "1"))
	}()
	go func() {
		defer group.Done()
		<-start
		deleteErr = devices.Delete(ctx, "device-001")
	}()
	close(start)
	group.Wait()

	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if receiveErr != nil && !errors.Is(receiveErr, device.ErrNotFound) {
		t.Fatalf("并发删除与接收出现意外错误: %v", receiveErr)
	}
	if _, err := devices.Get(ctx, "device-001"); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("删除后设备查询错误 = %v", err)
	}
	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if receiveErr == nil && len(history) != 1 {
		t.Fatalf("接收成功但删除后历史缺失: %#v", history)
	}
	if errors.Is(receiveErr, device.ErrNotFound) && len(history) != 0 {
		t.Fatalf("删除先提交后仍写入历史: %#v", history)
	}
}

func TestConcurrentResetAndReceiveInvalidatesOldSecret(t *testing.T) {
	ctx := context.Background()
	_, devices, service, oldSecret, _ := testSetup(t)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	var receiveErr error
	var newSecret string
	var resetErr error
	go func() {
		defer group.Done()
		<-start
		receiveErr = service.Receive(ctx, "device-001", oldSecret, temperaturePayload("reset-race", "2026-09-27T11:59:00Z", "1"))
	}()
	go func() {
		defer group.Done()
		<-start
		newSecret, resetErr = devices.ResetSecret(ctx, "device-001")
	}()
	close(start)
	group.Wait()

	if resetErr != nil {
		t.Fatal(resetErr)
	}
	if receiveErr != nil && !errors.Is(receiveErr, telemetry.ErrInvalidSecret) {
		t.Fatalf("并发重置与接收出现意外错误: %v", receiveErr)
	}
	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if receiveErr == nil && len(history) != 1 {
		t.Fatalf("接收成功但历史缺失: %#v", history)
	}
	if errors.Is(receiveErr, telemetry.ErrInvalidSecret) && len(history) != 0 {
		t.Fatalf("重置先提交后仍接受旧密钥: %#v", history)
	}
	if err := service.Receive(ctx, "device-001", oldSecret, temperaturePayload("old-after-reset", "2026-09-27T11:59:01Z", "2")); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("重置后旧密钥错误 = %v", err)
	}
	if err := service.Receive(ctx, "device-001", newSecret, temperaturePayload("new-after-reset", "2026-09-27T11:59:02Z", "3")); err != nil {
		t.Fatalf("重置后新密钥接收失败: %v", err)
	}
}

func containsSecret(value, secret string) bool {
	return secret != "" && len(value) >= len(secret) && stringContains(value, secret)
}

func stringContains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
