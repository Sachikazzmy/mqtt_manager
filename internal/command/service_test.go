package command_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"Project/internal/command"
	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
)

type testRig struct {
	store     *storage.MemoryStore
	devices   *device.Service
	telemetry *telemetry.Service
	commands  *command.Service
}

func newTestRig(t *testing.T, metrics string) *testRig {
	t.Helper()
	ctx := context.Background()
	store := storage.NewMemoryStore()
	devices := device.NewService(store)
	secret, err := devices.Create(ctx, "linux-01", "test device")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 30, 4, 1, 0, 0, time.UTC)
	telemetryService := telemetry.NewServiceWithClock(store, func() time.Time { return now }, telemetry.Config{MaxFutureSkew: 5 * time.Minute})
	payload := []byte(`{"version":"1","device_id":"linux-01","message_id":"initial","sampled_at":"2026-09-30T04:00:00Z","metrics":` + metrics + `}`)
	if err := telemetryService.Receive(ctx, "linux-01", secret, payload); err != nil {
		t.Fatal(err)
	}
	return &testRig{store: store, devices: devices, telemetry: telemetryService, commands: command.NewService(devices, store)}
}

func TestCommandLifecycleNeverWritesTelemetryAndResultsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	rig := newTestRig(t, `{"segment-1":{"value":23.6,"unit":"V","modifiable":true},"segment-2":{"value":0,"unit":"V","modifiable":true},"segment-3":{"value":4,"unit":"V"}}`)

	set, err := rig.commands.SetMetricWithID(ctx, "set-1", "linux-01", "segment-1", 120)
	if err != nil || set.Status != command.StatusWaitingToSend || set.Value == nil || *set.Value != 120 {
		t.Fatalf("set command = %#v, %v", set, err)
	}
	retried, err := rig.commands.SetMetricWithID(ctx, "set-1", "linux-01", "segment-1", 120)
	if err != nil || retried.CommandID != set.CommandID {
		t.Fatalf("相同 ID 和内容的请求应返回原命令: %#v %v", retried, err)
	}
	if _, err := rig.commands.SetMetricWithID(ctx, "set-1", "linux-01", "segment-1", 121); !errors.Is(err, command.ErrCommandConflict) {
		t.Fatalf("相同 ID 不同内容必须拒绝: %v", err)
	}
	if _, err := rig.commands.ClearOverride(ctx, "linux-01", "segment-1"); !errors.Is(err, command.ErrCommandPending) {
		t.Fatalf("同指标未决期间不得提交后续命令: %v", err)
	}
	if _, err := rig.commands.SetMetric(ctx, "linux-01", "segment-3", 8); !errors.Is(err, command.ErrMetricNotMutable) {
		t.Fatalf("设备声明不可修改的指标应拒绝: %v", err)
	}
	if _, err := rig.commands.SetMetric(ctx, "linux-01", "segment-4", 8); !errors.Is(err, command.ErrMetricNotReady) {
		t.Fatalf("没有 Latest 的指标应拒绝: %v", err)
	}
	if _, err := rig.commands.SetMetric(ctx, "linux-01", "segment-1", math.Inf(1)); !errors.Is(err, command.ErrInvalidCommand) {
		t.Fatalf("非有限命令数值应拒绝: %v", err)
	}

	var sent []byte
	err = rig.store.DispatchDue(ctx, set.CreatedAt, 1, command.MaxAttempts, command.RetryBase, command.RetryMaximum, func(_ context.Context, entry command.Command) error {
		if entry.CommandID != set.CommandID {
			t.Fatalf("发布 command_id=%s，期望 %s", entry.CommandID, set.CommandID)
		}
		payloadBytes := mustJSON(t, command.Payload{Version: "1", CommandID: entry.CommandID, Action: entry.Action, MetricKey: entry.MetricKey, Value: entry.Value})
		var payload map[string]any
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["version"] != "1" || payload["action"] != "set_metric" || payload["metric_key"] != "segment-1" || payload["value"] != float64(120) || payload["device_id"] != nil {
			t.Fatalf("发布内容不符合命令协议: %#v", payload)
		}
		sent = payloadBytes
		return nil
	})
	if err != nil || len(sent) == 0 {
		t.Fatalf("发布命令: payload=%s err=%v", sent, err)
	}
	acked, err := rig.commands.Get(ctx, "linux-01", set.CommandID)
	if err != nil || acked.Status != command.StatusBrokerAcked || acked.BrokerAckedAt == nil {
		t.Fatalf("Broker PUBACK 状态 = %#v, %v", acked, err)
	}

	before, err := rig.devices.Get(ctx, "linux-01")
	if err != nil {
		t.Fatal(err)
	}
	if before.Latest.Metrics["segment-1"].Value != 23.6 || before.Latest.Metrics["segment-1"].MessageID != "initial" {
		t.Fatalf("命令发布不应改变实际遥测 Latest: %#v", before.Latest.Metrics["segment-1"])
	}

	wrongValue := []byte(`{"version":"1","command_id":"set-1","status":"applied","metric_key":"segment-1","value":121}`)
	if disposition, err := rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", wrongValue); err != nil || disposition.Anomaly == "" {
		t.Fatalf("不匹配的 applied value 应持久记录异常且不更新状态: %#v %v", disposition, err)
	}
	stillAcked, err := rig.commands.Get(ctx, "linux-01", "set-1")
	if err != nil || stillAcked.Status != command.StatusBrokerAcked {
		t.Fatalf("无效结果不得改变命令状态: %#v %v", stillAcked, err)
	}

	applied := []byte(`{"version":"1","command_id":"set-1","status":"applied","metric_key":"segment-1","value":120}`)
	disposition, err := rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", applied)
	if err != nil || disposition.Command == nil || disposition.Command.Status != command.StatusApplied {
		t.Fatalf("有效 applied 结果 = %#v, %v", disposition, err)
	}
	disposition, err = rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", applied)
	if err != nil || !disposition.Duplicate {
		t.Fatalf("重复结果应幂等: %#v %v", disposition, err)
	}
	conflict := []byte(`{"version":"1","command_id":"set-1","status":"rejected","metric_key":"segment-1","value":120}`)
	disposition, err = rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", conflict)
	if err != nil || disposition.Anomaly == "" {
		t.Fatalf("冲突终态应留下异常，不得覆盖 applied: %#v %v", disposition, err)
	}

	after, err := rig.devices.Get(ctx, "linux-01")
	if err != nil || after.Latest.Metrics["segment-1"].Value != 23.6 || after.Latest.Metrics["segment-1"].MessageID != "initial" {
		t.Fatalf("applied 不是实际采样，Latest 只能由 telemetry 更新: %#v %v", after.Latest, err)
	}
	history, err := rig.telemetry.History(ctx, telemetry.HistoryQuery{DeviceID: "linux-01", Limit: 10})
	if err != nil || len(history) != 1 {
		t.Fatalf("命令不得新增遥测历史: %#v, %v", history, err)
	}
	anomalies := rig.store.CommandResultAnomalies()
	if len(anomalies) != 2 {
		t.Fatalf("无效 value 与冲突终态必须持久记录，异常=%#v", anomalies)
	}

	clear, err := rig.commands.ClearOverride(ctx, "linux-01", "segment-1")
	if err != nil || clear.Value != nil || clear.Status != command.StatusWaitingToSend {
		t.Fatalf("clear_override 必须不带 value: %#v %v", clear, err)
	}
	clearBody, err := json.Marshal(command.Payload{Version: "1", CommandID: clear.CommandID, Action: command.ActionClearOverride, MetricKey: clear.MetricKey})
	if err != nil || strings.Contains(string(clearBody), `"value"`) {
		t.Fatalf("clear_override 发布消息不得带 value: %s %v", clearBody, err)
	}
	clearResult := []byte(`{"version":"1","command_id":"` + clear.CommandID + `","status":"applied","metric_key":"segment-1"}`)
	disposition, err = rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", clearResult)
	if err != nil || disposition.Command == nil || disposition.Command.Status != command.StatusApplied || disposition.Command.Value != nil {
		t.Fatalf("无 value 的 clear_override 结果必须是 applied，不能变为 0: %#v %v", disposition, err)
	}
	final, _ := rig.devices.Get(ctx, "linux-01")
	if final.Latest.Metrics["segment-1"].Value != 23.6 {
		t.Fatalf("clear_override 结果不得生成采样: %#v", final.Latest.Metrics["segment-1"])
	}
}

func TestRejectedResultAndResultBeforeBrokerAck(t *testing.T) {
	ctx := context.Background()
	rig := newTestRig(t, `{"segment-1":{"value":0,"unit":"V","modifiable":true}}`)
	entry, err := rig.commands.SetMetricWithID(ctx, "early-result", "linux-01", "segment-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	missingEcho := []byte(`{"version":"1","command_id":"early-result","status":"rejected","metric_key":"segment-1"}`)
	disposition, err := rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", missingEcho)
	if err != nil || disposition.Anomaly == "" {
		t.Fatalf("set_metric rejected 结果必须回显请求 value: %#v %v", disposition, err)
	}
	rejected := []byte(`{"version":"1","command_id":"early-result","status":"rejected","metric_key":"segment-1","value":0}`)
	disposition, err = rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", rejected)
	if err != nil || disposition.Command == nil || disposition.Command.Status != command.StatusRejected {
		t.Fatalf("rejected result = %#v, %v", disposition, err)
	}
	if err := rig.store.MarkBrokerAck(ctx, "linux-01", entry.CommandID, time.Now()); err != nil {
		t.Fatal(err)
	}
	final, err := rig.commands.Get(ctx, "linux-01", entry.CommandID)
	if err != nil || final.Status != command.StatusRejected {
		t.Fatalf("迟到的 PUBACK 状态写入不得降低 rejected: %#v, %v", final, err)
	}
	if _, err := rig.commands.SetMetric(ctx, "linux-01", "segment-1", 1); err != nil {
		t.Fatalf("rejected 已明确结束，可提交新命令: %v", err)
	}
}

func TestCapabilityChangeBeforeFirstSendCancelsQueuedCommand(t *testing.T) {
	ctx := context.Background()
	rig := newTestRig(t, `{"segment-1":{"value":1,"unit":"V","modifiable":true}}`)
	entry, err := rig.commands.SetMetric(ctx, "linux-01", "segment-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := rig.devices.ResetSecret(ctx, "linux-01")
	if err != nil {
		t.Fatal(err)
	}
	update := []byte(`{"version":"1","device_id":"linux-01","message_id":"capability-change","sampled_at":"2026-09-30T04:01:00Z","metrics":{"segment-1":{"value":1,"unit":"V","modifiable":false}}}`)
	if err := rig.telemetry.Receive(ctx, "linux-01", secret, update); err != nil {
		t.Fatal(err)
	}
	published := false
	err = rig.store.DispatchDue(ctx, entry.CreatedAt, 1, command.MaxAttempts, command.RetryBase, command.RetryMaximum, func(context.Context, command.Command) error {
		published = true
		return nil
	})
	if err != nil || published {
		t.Fatalf("Latest 已撤回修改能力后不得发布: published=%v err=%v", published, err)
	}
	current, err := rig.commands.Get(ctx, "linux-01", entry.CommandID)
	if err != nil || current.Status != command.StatusCancelled {
		t.Fatalf("未发送命令应本地取消: %#v %v", current, err)
	}
}

func TestRetryIsBoundedUnknownCanAcceptLateResultAndLifecyclePauses(t *testing.T) {
	ctx := context.Background()
	rig := newTestRig(t, `{"segment-1":{"value":1,"unit":"V","modifiable":true},"segment-2":{"value":2,"unit":"V","modifiable":true}}`)
	entry, err := rig.commands.SetMetricWithID(ctx, "retry-same-id", "linux-01", "segment-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	var sentIDs []string
	now := entry.CreatedAt
	for attempt := 0; attempt < command.MaxAttempts; attempt++ {
		err = rig.store.DispatchDue(ctx, now, 1, command.MaxAttempts, command.RetryBase, command.RetryMaximum, func(_ context.Context, current command.Command) error {
			sentIDs = append(sentIDs, current.CommandID)
			return errors.New("connection lost after possible publish")
		})
		if err != nil {
			t.Fatal(err)
		}
		current, getErr := rig.commands.Get(ctx, "linux-01", entry.CommandID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		now = current.NextAttemptAt
	}
	if len(sentIDs) != command.MaxAttempts {
		t.Fatalf("发送尝试数=%d，期望有限次数 %d", len(sentIDs), command.MaxAttempts)
	}
	for _, id := range sentIDs {
		if id != entry.CommandID {
			t.Fatalf("重试生成了新 command_id: %#v", sentIDs)
		}
	}
	if err := rig.store.Expire(ctx, now, command.MaxAttempts); err != nil {
		t.Fatal(err)
	}
	stillWaiting, err := rig.commands.Get(ctx, "linux-01", entry.CommandID)
	if err != nil || stillWaiting.Status == command.StatusResultUnknown {
		t.Fatalf("耗尽发送次数后仍应等待迟到结果至期限: %#v %v", stillWaiting, err)
	}
	if err := rig.store.Expire(ctx, entry.DeadlineAt.Add(time.Second), command.MaxAttempts); err != nil {
		t.Fatal(err)
	}
	unknown, err := rig.commands.Get(ctx, "linux-01", entry.CommandID)
	if err != nil || unknown.Status != command.StatusResultUnknown {
		t.Fatalf("重试耗尽后状态应为 result_unknown: %#v %v", unknown, err)
	}
	if _, err := rig.commands.SetMetric(ctx, "linux-01", "segment-1", 8); !errors.Is(err, command.ErrCommandPending) {
		t.Fatalf("执行情况未知时同指标必须保持未决互斥: %v", err)
	}
	late := []byte(`{"version":"1","command_id":"retry-same-id","status":"applied","metric_key":"segment-1","value":7}`)
	disposition, err := rig.commands.ProcessResultFromBroker(ctx, "factory/linux-01/command_result", late)
	if err != nil || disposition.Command == nil || disposition.Command.Status != command.StatusApplied {
		t.Fatalf("超时后的有效迟到结果应完成确认: %#v %v", disposition, err)
	}

	paused, err := rig.commands.SetMetricWithID(ctx, "paused", "linux-01", "segment-2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.store.DispatchDue(ctx, paused.CreatedAt, 1, command.MaxAttempts, command.RetryBase, command.RetryMaximum, func(context.Context, command.Command) error {
		return errors.New("发布结果不确定")
	}); err != nil {
		t.Fatal(err)
	}
	if err := rig.devices.Disable(ctx, "linux-01"); err != nil {
		t.Fatal(err)
	}
	if err := rig.store.Expire(ctx, paused.CreatedAt.Add(time.Second), command.MaxAttempts); err != nil {
		t.Fatal(err)
	}
	stillQueued, err := rig.commands.Get(ctx, "linux-01", paused.CommandID)
	if err != nil || stillQueued.Status != command.StatusWaitingToSend {
		t.Fatalf("停用期间仍未到期限的命令应等待恢复: %#v %v", stillQueued, err)
	}
	retriedWhileDisabled := false
	if err := rig.store.DispatchDue(ctx, stillQueued.NextAttemptAt, 1, command.MaxAttempts, command.RetryBase, command.RetryMaximum, func(context.Context, command.Command) error {
		retriedWhileDisabled = true
		return nil
	}); err != nil || retriedWhileDisabled {
		t.Fatalf("设备停用时不得重试发送: sent=%v err=%v", retriedWhileDisabled, err)
	}
	if err := rig.store.Expire(ctx, paused.DeadlineAt.Add(time.Second), command.MaxAttempts); err != nil {
		t.Fatal(err)
	}
	resumed, err := rig.commands.Get(ctx, "linux-01", paused.CommandID)
	if err != nil || resumed.Status != command.StatusResultUnknown {
		t.Fatalf("禁用期间期限结束应转为未知且不重发: %#v %v", resumed, err)
	}
	if err := rig.devices.Enable(ctx, "linux-01"); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.commands.SetMetric(ctx, "linux-01", "segment-2", 11); !errors.Is(err, command.ErrCommandPending) {
		t.Fatalf("恢复后执行未知命令仍须阻止同指标新命令: %v", err)
	}
	deleted, err := rig.commands.SetMetricWithID(ctx, "deleted", "linux-01", "segment-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.devices.Delete(ctx, "linux-01"); err != nil {
		t.Fatal(err)
	}
	cancelled, err := rig.commands.Get(ctx, "linux-01", deleted.CommandID)
	if err != nil || cancelled.Status != command.StatusCancelled {
		t.Fatalf("删除设备应取消未解决命令: %#v %v", cancelled, err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	result, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
