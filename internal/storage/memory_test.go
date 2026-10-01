package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
)

func newTestServices(t *testing.T) (*device.Service, *telemetry.Service, string) {
	t.Helper()
	ctx := context.Background()
	store := storage.NewMemoryStore()
	devices := device.NewService(store)
	secret, err := devices.Create(ctx, "device-001", "测试设备")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	telemetryService := telemetry.NewServiceWithClock(store, func() time.Time {
		return now
	}, telemetry.Config{
		MaxFutureSkew: 5 * time.Minute,
	})
	return devices, telemetryService, secret
}

func TestDeleteKeepsHistoryAndPreventsIDReuse(t *testing.T) {
	ctx := context.Background()
	devices, telemetryService, secret := newTestServices(t)

	payload := []byte(`{"version":"1","device_id":"device-001","message_id":"m-1","sampled_at":"2026-09-27T11:59:00Z","metrics":{"segment-1":{"value":1,"unit":"V"}}}`)
	if err := telemetryService.Receive(ctx, "device-001", secret, payload); err != nil {
		t.Fatal(err)
	}
	if err := devices.Delete(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	if _, err := devices.Create(ctx, "device-001", "新设备"); !errors.Is(err, device.ErrIDDeleted) {
		t.Fatalf("复用已删除 ID 的错误 = %v", err)
	}
	history, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].MessageID != "m-1" {
		t.Fatalf("删除后历史 = %#v", history)
	}
}

func TestQueryReturnsIndependentSnapshots(t *testing.T) {
	ctx := context.Background()
	devices, telemetryService, secret := newTestServices(t)
	payload := []byte(`{"version":"1","device_id":"device-001","message_id":"m-1","sampled_at":"2026-09-27T11:59:00Z","metrics":{"segment-1":{"value":1,"unit":"V"}}}`)
	if err := telemetryService.Receive(ctx, "device-001", secret, payload); err != nil {
		t.Fatal(err)
	}
	first, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	first.Name = "外部修改"
	state := first.Latest.Metrics["segment-1"]
	state.MessageID = "外部修改"
	first.Latest.Metrics["segment-1"] = state
	first.Latest.Metrics["segment-1"] = device.MetricState{Value: 99, Unit: "V"}

	second, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != "测试设备" || second.Latest.Metrics["segment-1"].MessageID != "m-1" || second.Latest.Metrics["segment-1"].Value != 1 {
		t.Fatalf("设备查询副本污染内部状态: name=%q latest=%#v", second.Name, second.Latest)
	}

	history, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("历史记录数量 = %d，期望 1", len(history))
	}
	history[0].Metrics["segment-1"] = telemetry.MetricValue{Value: 88, Unit: "V"}
	nextHistory, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(nextHistory) != 1 {
		t.Fatalf("再次查询历史记录数量 = %d，期望 1", len(nextHistory))
	}
	if nextHistory[0].Metrics["segment-1"].Value != 1 {
		t.Fatalf("历史查询副本污染内部状态: %#v", nextHistory)
	}
}

func TestConfigUpdatesPreserveLatestState(t *testing.T) {
	ctx := context.Background()
	devices, telemetryService, secret := newTestServices(t)
	payload := []byte(`{"version":"1","device_id":"device-001","message_id":"m-1","sampled_at":"2026-09-27T11:59:00Z","metrics":{"segment-1":{"value":1,"unit":"V"}}}`)
	if err := telemetryService.Receive(ctx, "device-001", secret, payload); err != nil {
		t.Fatal(err)
	}
	if err := devices.Update(ctx, "device-001", "新名称"); err != nil {
		t.Fatal(err)
	}
	if err := devices.Disable(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	if err := devices.Enable(ctx, "device-001"); err != nil {
		t.Fatal(err)
	}
	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Name != "新名称" || d.Latest == nil || d.Latest.Metrics["segment-1"].MessageID != "m-1" || d.Latest.Metrics["segment-1"].Value != 1 {
		t.Fatalf("配置修改覆盖了 Latest: %#v", d)
	}

	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "secretDigest") {
		t.Fatalf("设备 JSON 暴露了凭据: %s", encoded)
	}
}

func TestConcurrentConfigUpdatesPreserveLatestState(t *testing.T) {
	ctx := context.Background()
	devices, telemetryService, secret := newTestServices(t)
	payload := []byte(`{"version":"1","device_id":"device-001","message_id":"m-1","sampled_at":"2026-09-27T11:59:00Z","metrics":{"segment-1":{"value":1,"unit":"V"}}}`)
	if err := telemetryService.Receive(ctx, "device-001", secret, payload); err != nil {
		t.Fatal(err)
	}

	const updates = 24
	errs := make(chan error, updates)
	var group sync.WaitGroup
	for i := 0; i < updates; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			if index%2 == 0 {
				errs <- devices.Update(ctx, "device-001", "名称")
				return
			}
			if index%4 == 1 {
				errs <- devices.Disable(ctx, "device-001")
				return
			}
			errs <- devices.Enable(ctx, "device-001")
		}(i)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	d, err := devices.Get(ctx, "device-001")
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest == nil || d.Latest.Metrics["segment-1"].MessageID != "m-1" || d.Latest.Metrics["segment-1"].Value != 1 {
		t.Fatalf("并发配置更新覆盖了 Latest: %#v", d.Latest)
	}
}

func TestHistoryFiltersRangeAndLimit(t *testing.T) {
	ctx := context.Background()
	_, telemetryService, secret := newTestServices(t)
	for i, sampledAt := range []string{"2026-09-27T11:00:00Z", "2026-09-27T11:01:00Z", "2026-09-27T11:02:00Z"} {
		payload := []byte(`{"version":"1","device_id":"device-001","message_id":"m-` + string(rune('1'+i)) + `","sampled_at":"` + sampledAt + `","metrics":{"segment-1":{"value":1,"unit":"V"}}}`)
		if err := telemetryService.Receive(ctx, "device-001", secret, payload); err != nil {
			t.Fatal(err)
		}
	}
	from := time.Date(2026, 9, 27, 11, 0, 30, 0, time.UTC)
	to := time.Date(2026, 9, 27, 11, 2, 0, 0, time.UTC)
	history, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: "device-001", From: &from, To: &to, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || !history[0].SampledAt.Equal(to) {
		t.Fatalf("范围和数量限制结果 = %#v", history)
	}
}
