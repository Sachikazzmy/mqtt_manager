package telemetry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
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
	})
	return store, devices, service, secret, clock
}

func messagePayload(deviceID, messageID, sampledAt, metrics string) []byte {
	return []byte(fmt.Sprintf(`{"version":"1","device_id":%q,"message_id":%q,"sampled_at":%q,"metrics":%s}`, deviceID, messageID, sampledAt, metrics))
}

func segmentPayload(messageID, sampledAt string, value string) []byte {
	return messagePayload("device-001", messageID, sampledAt, fmt.Sprintf(`{"segment-1":{"value":%s,"unit":"V"}}`, value))
}

func TestReceiveValidatesPayloadAndPreservesZero(t *testing.T) {
	_, devices, service, secret, clock := testSetup(t)
	ctx := context.Background()

	if err := service.Receive(ctx, "device-001", secret, segmentPayload("zero", "2026-09-27T11:59:00Z", "0")); err != nil {
		t.Fatal(err)
	}
	deviceValue, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if deviceValue.Latest == nil || deviceValue.Latest.Metrics["segment-1"].Value != 0 {
		t.Fatalf("Latest 没有保留真实零值: %#v", deviceValue.Latest)
	}

	cases := []struct {
		name    string
		payload []byte
		want    error
	}{
		{
			name:    "missing value",
			payload: messagePayload("device-001", "missing", "2026-09-27T11:59:01Z", `{"segment-1":{"unit":"V"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "wrong unit",
			payload: messagePayload("device-001", "unit", "2026-09-27T11:59:02Z", `{"segment-1":{"value":1,"unit":"mV"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "unknown metric",
			payload: messagePayload("device-001", "metric", "2026-09-27T11:59:03Z", `{"voltage":{"value":1,"unit":"V"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "NUL message ID",
			payload: segmentPayload("bad\x00id", "2026-09-27T11:59:03Z", "1"),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "non finite value",
			payload: messagePayload("device-001", "overflow", "2026-09-27T11:59:04Z", `{"segment-1":{"value":1e999,"unit":"V"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "duplicate top-level field",
			payload: []byte(`{"version":"1","device_id":"device-001","message_id":"first","message_id":"second","sampled_at":"2026-09-27T11:59:04Z","metrics":{"segment-1":{"value":1,"unit":"V"}}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "duplicate metrics field",
			payload: []byte(`{"version":"1","device_id":"device-001","message_id":"duplicate-metrics","sampled_at":"2026-09-27T11:59:04Z","metrics":{"segment-1":{"value":1,"unit":"V"}},"metrics":{"segment-1":{"value":2,"unit":"V"}}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "duplicate value field",
			payload: messagePayload("device-001", "duplicate-value", "2026-09-27T11:59:04Z", `{"segment-1":{"value":1,"value":2,"unit":"V"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "duplicate unit field",
			payload: messagePayload("device-001", "duplicate-unit", "2026-09-27T11:59:04Z", `{"segment-1":{"value":1,"unit":"V","unit":"F"}}`),
			want:    telemetry.ErrInvalidMessage,
		},
		{
			name:    "invalid unit whitespace",
			payload: messagePayload("device-001", "bad-unit", "2026-09-27T11:59:04Z", `{"segment-1":{"value":1,"unit":" V "}}`),
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
	invalidUTF8 := []byte(`{"version":"1","device_id":"device-001","message_id":"`)
	invalidUTF8 = append(invalidUTF8, 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`","sampled_at":"2026-09-27T11:59:04Z","metrics":{"segment-1":{"value":1,"unit":"V"}}}`)...)
	if err := service.Receive(ctx, "device-001", secret, invalidUTF8); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("无效 UTF-8 必须在 JSON 解析前拒绝: %v", err)
	}

	*clock = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	if err := service.Receive(ctx, "device-001", secret, segmentPayload("future", "2026-09-27T12:06:00Z", "1")); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("超前时间错误 = %v", err)
	}
	if err := service.Receive(ctx, "device-001", "wrong-secret", segmentPayload("secret", "2026-09-27T11:59:04Z", "1")); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("错误密钥错误 = %v", err)
	}
	if err := service.Receive(ctx, "other-device", secret, segmentPayload("identity", "2026-09-27T11:59:05Z", "1")); !errors.Is(err, telemetry.ErrDeviceMismatch) {
		t.Fatalf("身份不匹配错误 = %v", err)
	}
	if err := service.Receive(ctx, "unknown", secret, messagePayload("unknown", "unknown", "2026-09-27T11:59:05Z", `{"segment-1":{"value":1,"unit":"V"}}`)); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("未知设备错误 = %v", err)
	}

	if err := devices.Disable(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	if err := service.Receive(ctx, "device-001", secret, segmentPayload("disabled", "2026-09-27T11:59:06Z", "1")); !errors.Is(err, telemetry.ErrDeviceDisabled) {
		t.Fatalf("禁用设备错误 = %v", err)
	}
}

func TestMessageIDRejectsWhitespaceAndOriginalOverLimitValue(t *testing.T) {
	_, _, service, secret, _ := testSetup(t)
	ctx := context.Background()
	if err := service.Receive(ctx, "device-001", secret, segmentPayload("a", "2026-09-27T11:59:00Z", "1")); err != nil {
		t.Fatal(err)
	}
	if err := service.Receive(ctx, "device-001", secret, segmentPayload(" a ", "2026-09-27T11:59:01Z", "2")); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("带首尾空白的 ID 不得归一成已用 ID，错误 = %v", err)
	}
	longPaddedID := " " + strings.Repeat("x", 128) + " "
	if err := service.Receive(ctx, "device-001", secret, segmentPayload(longPaddedID, "2026-09-27T11:59:02Z", "3")); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("原始超长 ID 不得通过 trim 绕过限制，错误 = %v", err)
	}
}

func TestNewDevicesAutomaticallyBindSegmentMetrics(t *testing.T) {
	_, devices, service, secret, clock := testSetup(t)
	ctx := context.Background()
	full := messagePayload("device-001", "segments-full", "2026-09-27T11:59:00Z", `{"segment-1":{"value":23.6,"unit":"V"},"segment-2":{"value":101.3,"unit":"kPa"},"segment-3":{"value":2.5,"unit":"A"}}`)
	if err := service.Receive(ctx, "device-001", secret, full); err != nil {
		t.Fatalf("首次 segment 指标上报: %v", err)
	}
	*clock = clock.Add(time.Second)
	partial := messagePayload("device-001", "segment-partial", "2026-09-27T12:00:00Z", `{"segment-2":{"value":0,"unit":"kPa"}}`)
	if err := service.Receive(ctx, "device-001", secret, partial); err != nil {
		t.Fatalf("segment 指标子集上报: %v", err)
	}
	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest == nil || len(d.Latest.Metrics) != 3 || d.Latest.Metrics["segment-1"].Value != 23.6 || d.Latest.Metrics["segment-2"].Value != 0 || d.Latest.Metrics["segment-3"].Value != 2.5 {
		t.Fatalf("segment 完整/部分上报失败: %#v", d.Latest)
	}
	if d.Latest.Metrics["segment-1"].MessageID != "segments-full" || d.Latest.Metrics["segment-2"].MessageID != "segment-partial" {
		t.Fatalf("各项 Latest 应指向各自的消息: %#v", d.Latest.Metrics)
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
	if err := service.Receive(ctx, "device-001", oldSecret, segmentPayload("old", "2026-09-27T11:59:00Z", "1")); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("旧密钥错误 = %v", err)
	}
	if err := service.Receive(ctx, "device-001", newSecret, segmentPayload("new", "2026-09-27T11:59:01Z", "2")); err != nil {
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

	if err := service.Receive(ctx, "device-001", secret, segmentPayload("new", "2026-09-27T11:59:00Z", "10")); err != nil {
		t.Fatal(err)
	}
	*clock = (*clock).Add(time.Second)
	if err := service.Receive(ctx, "device-001", secret, segmentPayload("old", "2026-09-27T11:00:00Z", "1")); err != nil {
		t.Fatal(err)
	}
	if err := service.Receive(ctx, "device-001", secret, segmentPayload("old", "2026-09-27T11:00:00Z", "1")); !errors.Is(err, telemetry.ErrDuplicateMessage) {
		t.Fatalf("重复消息错误 = %v", err)
	}

	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest.Metrics["segment-1"].MessageID != "new" || d.Latest.Metrics["segment-1"].Value != 10 {
		t.Fatalf("旧采样覆盖了 Latest: %#v", d.Latest)
	}
	if d.LastValidReceivedAt == nil || !d.LastValidReceivedAt.Equal(*clock) {
		t.Fatalf("旧采样没有推进最后有效接收时间: %v", d.LastValidReceivedAt)
	}

	*clock = (*clock).Add(time.Second)
	if err := service.Receive(ctx, "device-001", secret, segmentPayload("same-later", "2026-09-27T11:59:00Z", "20")); err != nil {
		t.Fatal(err)
	}
	d, err = devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest.Metrics["segment-1"].MessageID != "same-later" {
		t.Fatalf("相同采样时间应由后接收消息胜出: %#v", d.Latest)
	}

	if err := service.Receive(ctx, "device-001", secret, segmentPayload("same-earlier-id", "2026-09-27T11:59:00Z", "30")); err != nil {
		t.Fatal(err)
	}
	d, err = devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest.Metrics["segment-1"].MessageID != "same-later" {
		t.Fatalf("相同采样和接收时间应由消息 ID 决胜: %#v", d.Latest)
	}
}

func TestPartialMetricsMaintainIndependentLatestAndRawHistory(t *testing.T) {
	_, devices, service, secret, clock := testSetup(t)
	ctx := context.Background()
	initial, err := devices.Get(ctx, "device-001")
	if err != nil || len(initial.MetricDefinitions) != 0 {
		t.Fatalf("新设备不应预置指标定义: %#v %v", initial.MetricDefinitions, err)
	}
	listed, err := devices.List(ctx)
	if err != nil || len(listed) != 1 || len(listed[0].MetricDefinitions) != 0 {
		t.Fatalf("list 中的新设备不应包含预置指标: %#v %v", listed, err)
	}

	firstReceived := time.Date(2026, 9, 27, 10, 0, 5, 0, time.UTC)
	*clock = firstReceived
	first := messagePayload("device-001", "m-10", "2026-09-27T10:00:00Z", `{"segment-1":{"value":1,"unit":"V"}}`)
	if err := service.Receive(ctx, "device-001", secret, first); err != nil {
		t.Fatal(err)
	}

	secondReceived := time.Date(2026, 9, 27, 11, 0, 5, 0, time.UTC)
	*clock = secondReceived
	second := messagePayload("device-001", "m-11", "2026-09-27T11:00:00Z", `{"segment-2":{"value":0,"unit":"kPa"},"segment-3":{"value":3,"unit":"A"}}`)
	if err := service.Receive(ctx, "device-001", secret, second); err != nil {
		t.Fatal(err)
	}

	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest == nil || len(d.Latest.Metrics) != 3 || len(d.MetricDefinitions) != 3 {
		t.Fatalf("分项 Latest 应包含三个指标: %#v", d.Latest)
	}
	segment1 := d.Latest.Metrics["segment-1"]
	segment2 := d.Latest.Metrics["segment-2"]
	if segment1.Value != 1 || segment1.SampledAt.Hour() != 10 || segment1.ReceivedAt != firstReceived || segment1.MessageID != "m-10" {
		t.Fatalf("未上报的 segment-1 必须保留原状态: %#v", segment1)
	}
	if segment2.Value != 0 || segment2.SampledAt.Hour() != 11 || segment2.MessageID != "m-11" {
		t.Fatalf("零值应作为本次真实样本保存: %#v", segment2)
	}
	if d.LastValidReceivedAt == nil || !d.LastValidReceivedAt.Equal(secondReceived) {
		t.Fatalf("设备接收时间应推进到第二条消息: %v", d.LastValidReceivedAt)
	}

	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil || len(history) != 2 {
		t.Fatalf("应保存两条原始消息: %#v %v", history, err)
	}
	if len(history[0].Metrics) != 1 || len(history[1].Metrics) != 2 || history[0].Metrics["segment-2"].Value != 0 {
		t.Fatalf("历史必须保留各条消息的实际子集: %#v", history)
	}

	*clock = secondReceived.Add(time.Minute)
	old := messagePayload("device-001", "m-old", "2026-09-27T09:00:00Z", `{"segment-1":{"value":9,"unit":"V"}}`)
	if err := service.Receive(ctx, "device-001", secret, old); err != nil {
		t.Fatal(err)
	}
	d, err = devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if got := d.Latest.Metrics["segment-1"]; got.Value != 1 || got.SampledAt.Hour() != 10 || got.MessageID != "m-10" {
		t.Fatalf("乱序样本不应覆盖 segment-1 Latest: %#v", got)
	}
	history, err = service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil || len(history) != 3 || history[0].MessageID != "m-old" || len(history[0].Metrics) != 1 || history[0].Metrics["segment-1"].Value != 9 {
		t.Fatalf("乱序子集应原样进入历史: %#v %v", history, err)
	}
	if err := service.Receive(ctx, "device-001", secret, old); !errors.Is(err, telemetry.ErrDuplicateMessage) {
		t.Fatalf("相同消息重投应去重: %v", err)
	}
}

func TestSegmentMetricsBindUnitAutomaticallyAndRejectInvalidKeys(t *testing.T) {
	_, _, service, secret, clock := testSetup(t)
	ctx := context.Background()
	cases := []struct {
		name    string
		metrics string
	}{
		{"unknown metric", `{"voltage":{"value":1,"unit":"V"}}`},
		{"segment outside supported namespace", `{"segment-101":{"value":1,"unit":"V"}}`},
		{"duplicate metric key", `{"segment-1":{"value":1,"unit":"V"},"segment-1":{"value":2,"unit":"V"}}`},
		{"legacy default key is not preconfigured", `{"temperature":{"value":1,"unit":"C"}}`},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := messagePayload("device-001", fmt.Sprintf("invalid-%d", index), "2026-09-27T11:59:00Z", tc.metrics)
			if err := service.Receive(ctx, "device-001", secret, payload); !errors.Is(err, telemetry.ErrInvalidMessage) {
				t.Fatalf("无效指标应被拒绝: %v", err)
			}
		})
	}
	if err := service.Receive(ctx, "device-001", secret, messagePayload("device-001", "first", "2026-09-27T11:59:00Z", `{"segment-1":{"value":1,"unit":"V"}}`)); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Second)
	if err := service.Receive(ctx, "device-001", secret, messagePayload("device-001", "wrong-unit", "2026-09-27T11:59:01Z", `{"segment-1":{"value":1,"unit":"mV"}}`)); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("自动绑定后单位变化必须被拒绝: %v", err)
	}
	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil || len(history) != 1 {
		t.Fatalf("只有合法首报应进入历史: %#v %v", history, err)
	}
}

func TestFirstSegmentReportCreatesOnlyItsBoundedDefinition(t *testing.T) {
	store := storage.NewMemoryStoreWithMetricLimit(4)
	devices := device.NewService(store)
	secret, err := devices.Create(context.Background(), "device-001", "test")
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	service := telemetry.NewServiceWithClock(store, func() time.Time { return clock }, telemetry.Config{MaxFutureSkew: 5 * time.Minute})
	ctx := context.Background()
	if err = service.Receive(ctx, "device-001", secret, messagePayload("device-001", "partial-rejected", "2026-09-27T11:58:59Z", `{"segment-1":{"value":1,"unit":"V"},"segment-2":{"value":2,"unit":"A"},"segment-3":{"value":3,"unit":"V"},"segment-4":{"value":4,"unit":"V"},"segment-5":{"value":5,"unit":"V"}}`)); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("包含超限 segment 的多项消息应整条拒收: %v", err)
	}
	before, err := devices.Get(ctx, "device-001")
	if err != nil || len(before.MetricDefinitions) != 0 || before.Latest != nil {
		t.Fatalf("无效消息不得部分初始化槽位或 Latest: %#v %v", before, err)
	}
	if err = service.Receive(ctx, "device-001", secret, messagePayload("device-001", "first-segment", "2026-09-27T11:59:00Z", `{"segment-1":{"value":0,"unit":"V"}}`)); err != nil {
		t.Fatal(err)
	}
	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if len(d.MetricDefinitions) != 1 || d.Latest == nil || len(d.Latest.Metrics) != 1 {
		t.Fatalf("首次上报只应创建其对应槽位: %#v", d)
	}
	var first *device.MetricDefinition
	for i := range d.MetricDefinitions {
		if d.MetricDefinitions[i].Key == "segment-1" {
			first = &d.MetricDefinitions[i]
		}
		if d.MetricDefinitions[i].Key == "segment-2" {
			t.Fatal("未上报的 segment-2 不应出现在定义列表")
		}
	}
	if first == nil || first.Unit != "V" || !first.Enabled || first.DisplayName != "segment-1" {
		t.Fatalf("首次上报应绑定单位并采用稳定默认名: %#v", first)
	}
	if err = service.Receive(ctx, "device-001", secret, messagePayload("device-001", "wrong-unit", "2026-09-27T11:59:01Z", `{"segment-1":{"value":1,"unit":"mV"}}`)); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("绑定后单位变化必须拒收: %v", err)
	}
	if err = service.Receive(ctx, "device-001", secret, messagePayload("device-001", "over-capacity", "2026-09-27T11:59:02Z", `{"segment-2":{"value":2,"unit":"V"},"segment-3":{"value":3,"unit":"V"},"segment-4":{"value":4,"unit":"V"},"segment-5":{"value":5,"unit":"V"}}`)); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("超过每设备定义上限必须拒收: %v", err)
	}
	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil || len(history) != 1 {
		t.Fatalf("拒收消息不得留下历史、定义或部分提交: %#v %v", history, err)
	}
}

func TestConfiguredMetricLimitCountsRegisteredSegments(t *testing.T) {
	const limit = 10
	store := storage.NewMemoryStoreWithMetricLimit(limit)
	devices := device.NewService(store)
	ctx := context.Background()
	goodSecret, err := devices.Create(ctx, "device-limit-good", "within limit")
	if err != nil {
		t.Fatal(err)
	}
	badSecret, err := devices.Create(ctx, "device-limit-bad", "over limit")
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	service := telemetry.NewServiceWithClock(store, func() time.Time { return clock }, telemetry.Config{MaxFutureSkew: 5 * time.Minute})
	metricObject := func(segmentCount int) string {
		entries := []string{}
		for index := 1; index <= segmentCount; index++ {
			entries = append(entries, fmt.Sprintf(`"segment-%d":{"value":%d,"unit":"V"}`, index, index))
		}
		return "{" + strings.Join(entries, ",") + "}"
	}
	if err = service.Receive(ctx, "device-limit-good", goodSecret, messagePayload("device-limit-good", "ten-metrics", "2026-09-27T11:59:00Z", metricObject(10))); err != nil {
		t.Fatalf("10 个 segment 应刚好达到每设备上限: %v", err)
	}
	good, err := devices.Get(ctx, "device-limit-good")
	if err != nil || len(good.MetricDefinitions) != limit || good.Latest == nil || len(good.Latest.Metrics) != limit {
		t.Fatalf("达到总上限时定义与 Latest 应各有 10 项: %#v %v", good, err)
	}
	if err = service.Receive(ctx, "device-limit-bad", badSecret, messagePayload("device-limit-bad", "eleven-metrics", "2026-09-27T11:59:00Z", metricObject(11))); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("11 个 segment 必须超过每设备 10 项上限: %v", err)
	}
	bad, err := devices.Get(ctx, "device-limit-bad")
	if err != nil || len(bad.MetricDefinitions) != 0 || bad.Latest != nil {
		t.Fatalf("超限消息不得部分创建定义或 Latest: %#v %v", bad, err)
	}
	from := clock.Add(-2 * time.Minute)
	to := clock
	history, err := store.History(ctx, telemetry.HistoryQuery{DeviceID: "device-limit-bad", From: &from, To: &to, Limit: 10})
	if err != nil || len(history) != 0 {
		t.Fatalf("超限消息不得写入历史: %#v %v", history, err)
	}
}

func TestConcurrentFirstReportsBindOnlyOneUnit(t *testing.T) {
	_, devices, service, secret, _ := testSetup(t)
	ctx := context.Background()
	payloadV := messagePayload("device-001", "racing-v", "2026-09-27T11:59:00Z", `{"segment-1":{"value":1,"unit":"V"}}`)
	payloadMV := messagePayload("device-001", "racing-mv", "2026-09-27T11:59:00Z", `{"segment-1":{"value":1000,"unit":"mV"}}`)
	start := make(chan struct{})
	var group sync.WaitGroup
	var receiveErrs [2]error
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		receiveErrs[0] = service.Receive(ctx, "device-001", secret, payloadV)
	}()
	go func() {
		defer group.Done()
		<-start
		receiveErrs[1] = service.Receive(ctx, "device-001", secret, payloadMV)
	}()
	close(start)
	group.Wait()
	accepted := 0
	for _, receiveErr := range receiveErrs {
		if receiveErr == nil {
			accepted++
		} else if !errors.Is(receiveErr, telemetry.ErrInvalidMessage) {
			t.Fatalf("并发首次绑定只允许冲突单位整条拒收: %v", receiveErr)
		}
	}
	if accepted != 1 {
		t.Fatalf("并发首次绑定应只有一条单位定义获胜: %v", receiveErrs)
	}
	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || d.Latest == nil || d.Latest.Metrics["segment-1"].MessageID != history[0].MessageID {
		t.Fatalf("并发首次绑定的历史与 Latest 应原子一致: history=%#v device=%#v", history, d)
	}
	definitionUnit := ""
	for _, definition := range d.MetricDefinitions {
		if definition.Key == "segment-1" {
			definitionUnit = definition.Unit
		}
	}
	if definitionUnit != d.Latest.Metrics["segment-1"].Unit {
		t.Fatalf("自动绑定单位应与获胜消息的 Latest 一致: definition=%q latest=%q", definitionUnit, d.Latest.Metrics["segment-1"].Unit)
	}
}

func TestConcurrentDuplicateOnlyCommitsOnce(t *testing.T) {
	_, _, service, secret, _ := testSetup(t)
	ctx := context.Background()
	payload := segmentPayload("same", "2026-09-27T11:59:00Z", "1")

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
	if err := service.Receive(canceled, "device-001", secret, segmentPayload("canceled", "2026-09-27T11:59:00Z", "1")); !errors.Is(err, context.Canceled) {
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
		receiveErr = service.Receive(ctx, "device-001", secret, segmentPayload("disable-race", "2026-09-27T11:59:00Z", "1"))
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
	if err := service.Receive(ctx, "device-001", secret, segmentPayload("history", "2026-09-27T11:59:00Z", "1")); err != nil {
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

func TestReceiveFromBrokerCommitsLatestAndHistoryAtomically(t *testing.T) {
	ctx := context.Background()
	_, devices, service, _, clock := testSetup(t)
	payload := segmentPayload("broker-1", "2026-09-27T11:59:00Z", "0")

	result, err := service.ReceiveFromBroker(ctx, "factory/device-001/telemetry", payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeviceID != "device-001" || result.MessageID != "broker-1" || result.Metrics["segment-1"].Value != 0 {
		t.Fatalf("Broker 接收结果 = %#v", result)
	}
	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest == nil || d.Latest.Metrics["segment-1"].MessageID != "broker-1" || d.Latest.Metrics["segment-1"].Value != 0 || d.LastValidReceivedAt == nil || !d.LastValidReceivedAt.Equal(*clock) {
		t.Fatalf("Broker 接收没有原子更新 Latest: %#v", d.Latest)
	}
	history, err := service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].MessageID != "broker-1" {
		t.Fatalf("Broker 接收历史 = %#v", history)
	}
	if _, err := service.ReceiveFromBroker(ctx, "factory/device-001/telemetry", payload); !errors.Is(err, telemetry.ErrDuplicateMessage) {
		t.Fatalf("重复 Broker 消息错误 = %v", err)
	}
	history, err = service.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil || len(history) != 1 {
		t.Fatalf("重复消息不应新增历史: len=%d err=%v", len(history), err)
	}
}

func TestReceiveFromBrokerRejectsWrongTopicIdentityAndDisabledDevice(t *testing.T) {
	ctx := context.Background()
	_, devices, service, _, _ := testSetup(t)
	valid := segmentPayload("valid", "2026-09-27T11:59:00Z", "1")

	for _, topic := range []string{
		"factory/device-001/telemetry/extra",
		"factory/+/telemetry",
		"factory/device/001/telemetry",
		"factory/device-001/other",
		"$CONTROL/dynamic-security/v1",
	} {
		if _, err := service.ReceiveFromBroker(ctx, topic, valid); !errors.Is(err, telemetry.ErrInvalidTopic) {
			t.Errorf("Topic %q 错误 = %v", topic, err)
		}
	}
	if _, err := service.ReceiveFromBroker(ctx, "factory/device-002/telemetry", valid); !errors.Is(err, telemetry.ErrDeviceMismatch) {
		t.Fatalf("跨设备 Topic 错误 = %v", err)
	}
	unknownPayload := messagePayload("device-002", "unknown", "2026-09-27T11:59:00Z", `{"segment-1":{"value":1,"unit":"V"}}`)
	if _, err := service.ReceiveFromBroker(ctx, "factory/device-002/telemetry", unknownPayload); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("未知 Broker 设备错误 = %v", err)
	}
	if err := devices.Disable(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReceiveFromBroker(ctx, "factory/device-001/telemetry", valid); !errors.Is(err, telemetry.ErrDeviceDisabled) {
		t.Fatalf("禁用 Broker 设备错误 = %v", err)
	}
}

func TestReceiveFromBrokerPayloadBoundary(t *testing.T) {
	ctx := context.Background()
	_, _, service, _, _ := testSetup(t)
	base := segmentPayload("boundary", "2026-09-27T11:59:00Z", "1")
	if len(base) >= telemetry.MaxPayloadBytes {
		t.Fatalf("测试基础消息意外过大: %d", len(base))
	}
	maxPayload := append(append([]byte(nil), base...), bytes.Repeat([]byte{' '}, telemetry.MaxPayloadBytes-len(base))...)
	if _, err := service.ReceiveFromBroker(ctx, "factory/device-001/telemetry", maxPayload); err != nil {
		t.Fatalf("恰好 %d 字节应接受: %v", telemetry.MaxPayloadBytes, err)
	}
	tooLarge := append(maxPayload, ' ')
	if _, err := service.ReceiveFromBroker(ctx, "factory/device-001/telemetry", tooLarge); !errors.Is(err, telemetry.ErrPayloadTooLarge) {
		t.Fatalf("超过边界的载荷错误 = %v", err)
	}
}

func TestReceiveFromBrokerRejectsStrictProtocolViolations(t *testing.T) {
	ctx := context.Background()
	_, _, service, _, _ := testSetup(t)
	if _, err := service.ReceiveFromBroker(ctx, "factory/device-001/telemetry", segmentPayload("unit-binding", "2026-09-27T11:59:00Z", "1")); err != nil {
		t.Fatalf("测试前应绑定 segment-1 的基准单位: %v", err)
	}
	cases := []struct {
		name    string
		payload []byte
	}{
		{"extra field", []byte(`{"version":"1","device_id":"device-001","message_id":"extra","sampled_at":"2026-09-27T11:59:00Z","metrics":{"segment-1":{"value":1,"unit":"V"}},"secret":"x"}`)},
		{"second JSON", append(segmentPayload("second", "2026-09-27T11:59:00Z", "1"), []byte(` {}`)...)},
		{"string number", messagePayload("device-001", "string", "2026-09-27T11:59:00Z", `{"segment-1":{"value":"1","unit":"V"}}`)},
		{"null number", messagePayload("device-001", "null", "2026-09-27T11:59:00Z", `{"segment-1":{"value":null,"unit":"V"}}`)},
		{"unknown metric", messagePayload("device-001", "metric", "2026-09-27T11:59:00Z", `{"voltage":{"value":1,"unit":"V"}}`)},
		{"unit mismatch", messagePayload("device-001", "unit", "2026-09-27T11:59:00Z", `{"segment-1":{"value":1,"unit":"F"}}`)},
		{"time without zone", messagePayload("device-001", "time", "2026-09-27T11:59:00", `{"segment-1":{"value":1,"unit":"V"}}`)},
		{"future timestamp", segmentPayload("future", "2026-09-27T12:06:00Z", "1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := service.ReceiveFromBroker(ctx, "factory/device-001/telemetry", tc.payload); !errors.Is(err, telemetry.ErrInvalidMessage) {
				t.Fatalf("错误 = %v", err)
			}
		})
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
		receiveErr = service.Receive(ctx, "device-001", secret, segmentPayload("delete-race", "2026-09-27T11:59:00Z", "1"))
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
		receiveErr = service.Receive(ctx, "device-001", oldSecret, segmentPayload("reset-race", "2026-09-27T11:59:00Z", "1"))
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
	if err := service.Receive(ctx, "device-001", oldSecret, segmentPayload("old-after-reset", "2026-09-27T11:59:01Z", "2")); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("重置后旧密钥错误 = %v", err)
	}
	if err := service.Receive(ctx, "device-001", newSecret, segmentPayload("new-after-reset", "2026-09-27T11:59:02Z", "3")); err != nil {
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
