package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"Project/internal/command"
	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresCommandOutboxSurvivesRestartAndKeepsTelemetrySeparate(t *testing.T) {
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("需要隔离测试数据库 PROJECT01_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	store, err := storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx, "../../migrations"); err != nil {
		store.Close()
		t.Fatal(err)
	}
	id := fmt.Sprintf("cmdtest-%d", time.Now().UnixNano())
	devices := device.NewService(store)
	secret, err := devices.Create(ctx, id, "command integration")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	clock := time.Date(2026, 9, 30, 4, 1, 0, 0, time.UTC)
	telemetryService := telemetry.NewServiceWithClock(store, func() time.Time { return clock }, telemetry.Config{MaxFutureSkew: 5 * time.Minute})
	payload := []byte(fmt.Sprintf(`{"version":"1","device_id":%q,"message_id":"sample-1","sampled_at":"2026-09-30T04:00:00Z","metrics":{"segment-1":{"value":5,"unit":"V","modifiable":true}}}`, id))
	if err := telemetryService.Receive(ctx, id, secret, payload); err != nil {
		store.Close()
		t.Fatal(err)
	}
	adminPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `UPDATE device_metrics SET min_value=0,max_value=10 WHERE device_id=$1 AND metric_key='segment-1'`, id); err != nil {
		adminPool.Close()
		store.Close()
		t.Fatal(err)
	}
	adminPool.Close()
	commands := command.NewService(devices, store)
	if _, err := commands.SetMetric(ctx, id, "segment-1", 11); !errors.Is(err, command.ErrInvalidCommand) {
		store.Close()
		t.Fatalf("当前数据库已配置范围时应拒绝越界值: %v", err)
	}
	entry, err := commands.SetMetricWithID(ctx, "persisted-command", id, "segment-1", 10)
	if err != nil || entry.Status != command.StatusWaitingToSend {
		store.Close()
		t.Fatalf("边界值命令应持久化: %#v %v", entry, err)
	}
	if _, err := commands.SetMetricWithID(ctx, "persisted-command", id, "segment-1", 9); !errors.Is(err, command.ErrCommandConflict) {
		store.Close()
		t.Fatalf("相同 ID 不同内容应拒绝: %v", err)
	}
	var attemptsVisibleBeforePublish int
	err = store.DispatchDue(ctx, entry.CreatedAt, 1, command.MaxAttempts, command.RetryBase, command.RetryMaximum, func(_ context.Context, item command.Command) error {
		current, getErr := commands.Get(ctx, id, item.CommandID)
		if getErr != nil {
			return getErr
		}
		attemptsVisibleBeforePublish = current.Attempts
		return errors.New("模拟发布后确认状态不确定")
	})
	if err != nil || attemptsVisibleBeforePublish != 1 {
		store.Close()
		t.Fatalf("发布前必须持久化尝试次数: attempts=%d err=%v", attemptsVisibleBeforePublish, err)
	}
	entry, err = commands.Get(ctx, id, "persisted-command")
	if err != nil || entry.Attempts != 1 || entry.Status != command.StatusWaitingToSend {
		store.Close()
		t.Fatalf("不确定的发布结果仍须保留尝试和原命令: %#v %v", entry, err)
	}
	adminPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `UPDATE device_commands SET value=9 WHERE device_id=$1 AND command_id='persisted-command'`, id); err == nil {
		adminPool.Close()
		store.Close()
		t.Fatal("数据库必须拒绝改写已持久化命令内容")
	}
	adminPool.Close()
	store.Close()

	store, err = storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	devices = device.NewService(store)
	commands = command.NewService(devices, store)
	telemetryService = telemetry.NewService(store)
	entry, err = commands.Get(ctx, id, "persisted-command")
	if err != nil || entry.Status != command.StatusWaitingToSend || entry.Attempts != 1 {
		t.Fatalf("重启后 outbox 应保留待发送命令: %#v %v", entry, err)
	}
	var published []byte
	publishEntered := make(chan struct{})
	allowPublishReturn := make(chan struct{})
	dispatchDone := make(chan error, 1)
	go func() {
		dispatchDone <- store.DispatchDue(ctx, entry.NextAttemptAt, 1, command.MaxAttempts, command.RetryBase, command.RetryMaximum, func(_ context.Context, item command.Command) error {
			published = []byte(fmt.Sprintf(`{"version":"1","command_id":%q,"action":%q,"metric_key":%q,"value":10}`, item.CommandID, item.Action, item.MetricKey))
			close(publishEntered)
			<-allowPublishReturn
			return nil
		})
	}()
	select {
	case <-publishEntered:
	case <-time.After(5 * time.Second):
		close(allowPublishReturn)
		t.Fatal("outbox 未进入受控发布回调")
	}
	adminPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		close(allowPublishReturn)
		t.Fatal(err)
	}
	probeTx, err := adminPool.Begin(ctx)
	if err != nil {
		adminPool.Close()
		close(allowPublishReturn)
		t.Fatal(err)
	}
	var lockAvailable bool
	err = probeTx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, id).Scan(&lockAvailable)
	_ = probeTx.Rollback(ctx)
	if err != nil || lockAvailable {
		adminPool.Close()
		close(allowPublishReturn)
		t.Fatalf("发送期间必须持有设备 lifecycle 协调锁: available=%v err=%v", lockAvailable, err)
	}
	disableDone := make(chan error, 1)
	go func() { disableDone <- store.SetEnabled(ctx, id, false, time.Now().UTC()) }()
	select {
	case disableErr := <-disableDone:
		adminPool.Close()
		close(allowPublishReturn)
		t.Fatalf("生命周期变更不应越过正在进行的有界发布: %v", disableErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(allowPublishReturn)
	dispatchErr := <-dispatchDone
	disableErr := <-disableDone
	adminPool.Close()
	if dispatchErr != nil || disableErr != nil || len(published) == 0 {
		t.Fatalf("持久化命令未能派发: %s %v", published, err)
	}
	entry, err = commands.Get(ctx, id, "persisted-command")
	if err != nil || entry.Status != command.StatusBrokerAcked || entry.BrokerAckedAt == nil || entry.Attempts != 2 {
		t.Fatalf("PUBACK 后只能标记 Broker 已确认: %#v %v", entry, err)
	}
	adminPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `UPDATE devices SET enabled=false,pending_operation='disable' WHERE id=$1`, id); err != nil {
		adminPool.Close()
		t.Fatal(err)
	}
	adminPool.Close()

	result := []byte(`{"version":"1","command_id":"persisted-command","status":"applied","metric_key":"segment-1","value":10}`)
	if _, err := commands.ProcessResultFromBroker(ctx, "factory/"+id+"/command_result", result); !errors.Is(err, command.ErrDeviceTransition) {
		t.Fatalf("生命周期过渡期间的结果必须保持未确认并重投: %v", err)
	}
	adminPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `UPDATE devices SET enabled=true,pending_operation=NULL WHERE id=$1`, id); err != nil {
		adminPool.Close()
		t.Fatal(err)
	}
	adminPool.Close()
	disposition, err := commands.ProcessResultFromBroker(ctx, "factory/"+id+"/command_result", result)
	if err != nil || disposition.Command == nil || disposition.Command.Status != command.StatusApplied {
		t.Fatalf("applied 结果 = %#v %v", disposition, err)
	}
	disposition, err = commands.ProcessResultFromBroker(ctx, "factory/"+id+"/command_result", result)
	if err != nil || !disposition.Duplicate {
		t.Fatalf("重复结果应幂等: %#v %v", disposition, err)
	}
	wrongTopic := []byte(`{"version":"1","command_id":"persisted-command","status":"rejected","metric_key":"segment-1","value":10}`)
	disposition, err = commands.ProcessResultFromBroker(ctx, "factory/another-device/command_result", wrongTopic)
	if err != nil || disposition.Anomaly == "" {
		t.Fatalf("设备身份由结果 Topic 提供，错误 Topic 不得完成其他设备命令: %#v %v", disposition, err)
	}
	entry, err = commands.Get(ctx, id, "persisted-command")
	if err != nil || entry.Status != command.StatusApplied {
		t.Fatalf("错误设备结果不得覆盖状态: %#v %v", entry, err)
	}
	deviceValue, err := devices.Get(ctx, id)
	if err != nil || deviceValue.Latest.Metrics["segment-1"].Value != 5 || deviceValue.Latest.Metrics["segment-1"].MessageID != "sample-1" {
		t.Fatalf("命令结果不得写 Latest: %#v %v", deviceValue.Latest, err)
	}
	history, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: id, Limit: 10})
	if err != nil || len(history) != 1 {
		t.Fatalf("命令结果不得新增采样历史: %#v %v", history, err)
	}
}
