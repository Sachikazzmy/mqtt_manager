package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"Project/internal/device"
	"Project/internal/storage"
	"Project/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresPersistenceAndConcurrency(t *testing.T) {
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("需要隔离测试数据库 PROJECT01_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	store, err := storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("stage4-%d", time.Now().UnixNano())
	secret := "one-time-secret"
	now := time.Now().UTC()
	if err = store.Create(ctx, device.Device{ID: id, Name: "test", Enabled: true, CreatedAt: now, UpdatedAt: now}, device.HashSecret(secret)); err != nil {
		t.Fatal(err)
	}
	sampled := now.Add(-time.Minute).Truncate(time.Microsecond)
	makeSample := func(messageID string, at time.Time) telemetry.Sample {
		return telemetry.Sample{DeviceID: id, MessageID: messageID, SampledAt: at, ReceivedAt: now, Metrics: map[string]telemetry.MetricValue{"segment-1": {Value: 23, Unit: "V"}}}
	}
	first := makeSample("a", sampled)
	if err = store.Commit(ctx, id, secret, first); err != nil {
		t.Fatal(err)
	}
	store.Close()
	store, err = storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	d, err := store.Get(ctx, id)
	if err != nil || d.Latest == nil || d.Latest.Metrics["segment-1"].MessageID != "a" {
		t.Fatalf("重连后 Latest: %#v %v", d, err)
	}
	if err = store.Commit(ctx, id, "wrong", makeSample("bad", sampled)); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("密钥摘要未保留: %v", err)
	}
	if err = store.Commit(ctx, id, secret, first); !errors.Is(err, telemetry.ErrDuplicateMessage) {
		t.Fatalf("重连去重: %v", err)
	}
	if err = store.Commit(ctx, id, secret, makeSample("bad\x00id", sampled)); !errors.Is(err, telemetry.ErrInvalidMessage) {
		t.Fatalf("数据库不支持字符应为永久输入错误: %v", err)
	}
	var wg sync.WaitGroup
	successes := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			successes <- store.Commit(ctx, id, secret, makeSample("concurrent", sampled.Add(time.Second)))
		}()
	}
	wg.Wait()
	close(successes)
	accepted, duplicates := 0, 0
	for e := range successes {
		switch {
		case e == nil:
			accepted++
		case errors.Is(e, telemetry.ErrDuplicateMessage):
			duplicates++
		default:
			t.Errorf("并发写入: %v", e)
		}
	}
	if accepted != 1 || duplicates != 19 {
		t.Fatalf("并发去重: accepted=%d duplicate=%d", accepted, duplicates)
	}
	if err = store.Commit(ctx, id, secret, makeSample("old", sampled.Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	d, err = store.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if d.Latest.Metrics["segment-1"].MessageID != "concurrent" || d.LastValidReceivedAt == nil || d.LastValidReceivedAt.Before(now.Truncate(time.Microsecond)) {
		t.Fatalf("乱序覆盖 Latest 或接收时间: %#v", d.Latest)
	}
	if err = store.Commit(ctx, id, secret, makeSample("z", sampled.Add(2*time.Second+200*time.Nanosecond))); err != nil {
		t.Fatal(err)
	}
	if err = store.Commit(ctx, id, secret, makeSample("é", sampled.Add(2*time.Second+800*time.Nanosecond))); err != nil {
		t.Fatal(err)
	}
	d, err = store.Get(ctx, id)
	if err != nil || d.Latest.Metrics["segment-1"].MessageID != "é" {
		t.Fatalf("微秒精度/C 排序结果: %#v %v", d.Latest, err)
	}
	from, to := sampled.Add(-time.Minute), sampled.Add(time.Minute)
	history, err := store.History(ctx, telemetry.HistoryQuery{DeviceID: id, From: &from, To: &to, Limit: 2})
	if err != nil || len(history) != 2 || history[0].MessageID != "z" || history[1].MessageID != "é" {
		t.Fatalf("历史排序和 LIMIT: %#v %v", history, err)
	}
	history, err = store.History(ctx, telemetry.HistoryQuery{DeviceID: id, From: &from, To: &to, Limit: 1})
	if err != nil || len(history) != 1 || history[0].MessageID != "é" {
		t.Fatalf("历史应选最新 N 条: %#v %v", history, err)
	}
	if err = store.SetEnabled(ctx, id, false, now); err != nil {
		t.Fatal(err)
	}
	if err = store.Commit(ctx, id, secret, makeSample("disabled", sampled)); !errors.Is(err, telemetry.ErrDeviceDisabled) {
		t.Fatalf("禁用后的写入: %v", err)
	}
	if err = store.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedStore.Close()
	if err = restartedStore.CheckCreate(ctx, id); !errors.Is(err, device.ErrIDDeleted) {
		t.Fatalf("墓碑: %v", err)
	}
	history, err = restartedStore.History(ctx, telemetry.HistoryQuery{DeviceID: id, From: &from, To: &to, Limit: 10})
	if err != nil || len(history) != 5 {
		t.Fatalf("删除后历史: %d %v", len(history), err)
	}
}

func TestPostgresPartialMetricLatestAndHistorySurviveRestart(t *testing.T) {
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("需要隔离测试数据库 PROJECT01_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	store, err := storage.OpenPostgresWithMetricLimit(ctx, dsn, device.DefaultMetricLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx, "../../migrations"); err != nil {
		store.Close()
		t.Fatal(err)
	}
	id := fmt.Sprintf("partial-%d", time.Now().UnixNano())
	devices := device.NewService(store)
	secret, err := devices.Create(ctx, id, "partial telemetry")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	initial, err := store.Get(ctx, id)
	if err != nil || len(initial.MetricDefinitions) != 0 || initial.Latest != nil {
		store.Close()
		t.Fatalf("未上报的 segment 槽不应出现在 PostgreSQL 设备视图: %#v %v", initial, err)
	}
	clock := time.Date(2026, 9, 27, 10, 0, 5, 0, time.UTC)
	telemetryService := telemetry.NewServiceWithClock(store, func() time.Time { return clock }, telemetry.Config{MaxFutureSkew: 5 * time.Minute})
	payload := func(messageID, sampledAt, metrics string) []byte {
		return []byte(fmt.Sprintf(`{"version":"1","device_id":%q,"message_id":%q,"sampled_at":%q,"metrics":%s}`, id, messageID, sampledAt, metrics))
	}
	if err = telemetryService.Receive(ctx, id, secret, payload("m-10", "2026-09-27T10:00:00Z", `{"segment-1":{"value":1,"unit":"V","modifiable":true}}`)); err != nil {
		store.Close()
		t.Fatal(err)
	}
	clock = time.Date(2026, 9, 27, 11, 0, 5, 0, time.UTC)
	if err = telemetryService.Receive(ctx, id, secret, payload("m-11", "2026-09-27T11:00:00Z", `{"segment-2":{"value":0,"unit":"kPa","modifiable":false},"segment-3":{"value":3,"unit":"A"}}`)); err != nil {
		store.Close()
		t.Fatal(err)
	}
	d, err := store.Get(ctx, id)
	if err != nil || d.Latest == nil || len(d.Latest.Metrics) != 3 {
		store.Close()
		t.Fatalf("逐指标 Latest 未合并不同消息: %#v %v", d, err)
	}
	if state := d.Latest.Metrics["segment-1"]; state.Value != 1 || state.SampledAt.Hour() != 10 || state.MessageID != "m-10" || !state.Modifiable {
		store.Close()
		t.Fatalf("未上报指标的采样时间和来源应保持不变: %#v", state)
	}
	if state := d.Latest.Metrics["segment-2"]; state.Value != 0 || state.SampledAt.Hour() != 11 || state.MessageID != "m-11" || state.Modifiable {
		store.Close()
		t.Fatalf("零值指标状态错误: %#v", state)
	}
	history, err := telemetryService.History(ctx, telemetry.HistoryQuery{DeviceID: id, Limit: 10})
	if err != nil || len(history) != 2 || len(history[0].Metrics) != 1 || len(history[1].Metrics) != 2 {
		store.Close()
		t.Fatalf("历史应是两条原始子集消息: %#v %v", history, err)
	}

	// 同一多指标消息的并发重投只能有一个事务写历史并更新所有对应 Latest。
	clock = time.Date(2026, 9, 27, 12, 0, 5, 0, time.UTC)
	concurrentPayload := payload("m-12", "2026-09-27T12:00:00Z", `{"segment-1":{"value":2,"unit":"V","modifiable":false},"segment-2":{"value":4,"unit":"kPa","modifiable":true}}`)
	const attempts = 12
	results := make(chan error, attempts)
	var group sync.WaitGroup
	for index := 0; index < attempts; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- telemetryService.Receive(ctx, id, secret, concurrentPayload)
		}()
	}
	group.Wait()
	close(results)
	accepted, duplicates := 0, 0
	for result := range results {
		switch {
		case result == nil:
			accepted++
		case errors.Is(result, telemetry.ErrDuplicateMessage):
			duplicates++
		default:
			t.Errorf("并发重复写入失败: %v", result)
		}
	}
	if accepted != 1 || duplicates != attempts-1 {
		store.Close()
		t.Fatalf("并发多指标去重: accepted=%d duplicates=%d", accepted, duplicates)
	}

	// 两个并发首次上报尝试为同一 key 绑定不同单位，只能有一个定义和样本提交。
	clock = time.Date(2026, 9, 27, 13, 0, 5, 0, time.UTC)
	racingPayloads := map[string][]byte{
		"m-13a": payload("m-13a", "2026-09-27T13:00:00Z", `{"segment-4":{"value":5,"unit":"A"}}`),
		"m-13b": payload("m-13b", "2026-09-27T13:00:00Z", `{"segment-4":{"value":6,"unit":"V"}}`),
	}
	start := make(chan struct{})
	writeResults := make(chan struct {
		messageID string
		err       error
	}, len(racingPayloads))
	group.Add(2)
	for messageID, body := range racingPayloads {
		go func(messageID string, body []byte) {
			defer group.Done()
			<-start
			writeResults <- struct {
				messageID string
				err       error
			}{messageID, telemetryService.Receive(ctx, id, secret, body)}
		}(messageID, body)
	}
	close(start)
	group.Wait()
	close(writeResults)
	var acceptedMessage string
	var rejected int
	for result := range writeResults {
		switch {
		case result.err == nil:
			acceptedMessage = result.messageID
		case errors.Is(result.err, telemetry.ErrInvalidMessage):
			rejected++
		default:
			t.Errorf("并发首次绑定失败: %v", result.err)
		}
	}
	if acceptedMessage == "" || rejected != 1 {
		store.Close()
		t.Fatalf("同一指标只能绑定一个单位: accepted=%q rejected=%d", acceptedMessage, rejected)
	}

	store.Close()
	store, err = storage.OpenPostgresWithMetricLimit(ctx, dsn, device.DefaultMetricLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	d, err = store.Get(ctx, id)
	if err != nil || d.Latest == nil || d.Latest.Metrics["segment-1"].MessageID != "m-12" || d.Latest.Metrics["segment-2"].MessageID != "m-12" || d.Latest.Metrics["segment-3"].MessageID != "m-11" {
		t.Fatalf("重启后每项 Latest 应保留各自来源: %#v %v", d, err)
	}
	if d.Latest.Metrics["segment-1"].Modifiable || !d.Latest.Metrics["segment-2"].Modifiable || d.Latest.Metrics["segment-3"].Modifiable {
		t.Fatalf("重启后 modifiable 必须跟随各自 Latest 来源，旧协议默认为 false: %#v", d.Latest.Metrics)
	}
	if state := d.Latest.Metrics["segment-4"]; state.MessageID != acceptedMessage {
		t.Fatalf("首次单位绑定应与唯一有效 Latest 一致: accepted=%q latest=%#v", acceptedMessage, state)
	}
	telemetryAfterRestart := telemetry.NewService(store)
	from, to := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC), time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC)
	history, err = telemetryAfterRestart.History(ctx, telemetry.HistoryQuery{DeviceID: id, From: &from, To: &to, Limit: 10})
	wantHistory := 4
	if err != nil || len(history) != wantHistory {
		t.Fatalf("重启后历史条数 = %d, want %d, err=%v", len(history), wantHistory, err)
	}
}

func TestPostgresPendingEnableKeepsTelemetryRetryable(t *testing.T) {
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("需要隔离测试数据库 PROJECT01_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	store, err := storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(ctx, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("transition-%d", time.Now().UnixNano())
	devices := device.NewService(store)
	_, err = devices.Create(ctx, id, "pending enable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, `UPDATE devices SET enabled=false,pending_operation='enable' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	service := telemetry.NewServiceWithClock(store, func() time.Time { return now }, telemetry.Config{MaxFutureSkew: 5 * time.Minute})
	payload := []byte(fmt.Sprintf(`{"version":"1","device_id":%q,"message_id":"during-enable","sampled_at":%q,"metrics":{"segment-1":{"value":0,"unit":"V"}}}`, id, now.Add(-time.Second).Format(time.RFC3339Nano)))
	topic := "factory/" + id + "/telemetry"
	if _, err = service.ReceiveFromBroker(ctx, topic, payload); !errors.Is(err, telemetry.ErrDeviceTransition) {
		t.Fatalf("启用状态提交期间应返回可重试错误: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE devices SET enabled=true,pending_operation=NULL WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = service.ReceiveFromBroker(ctx, topic, payload); err != nil {
		t.Fatalf("状态提交完成后重投应成功: %v", err)
	}
}

func TestPostgresMigratesLegacySnapshotToPerMetricLatest(t *testing.T) {
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("需要隔离测试数据库 PROJECT01_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	baseConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminPool, err := pgxpool.NewWithConfig(ctx, baseConfig)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("metric_migration_%d", time.Now().UnixNano())
	if _, err = adminPool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		adminPool.Close()
		t.Fatal(err)
	}
	var store *storage.PostgresStore
	defer func() {
		if store != nil {
			store.Close()
		}
		_, _ = adminPool.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		adminPool.Close()
	}()

	schemaConn := baseConfig.ConnConfig.Copy()
	if schemaConn.RuntimeParams == nil {
		schemaConn.RuntimeParams = make(map[string]string)
	}
	schemaConn.RuntimeParams["search_path"] = schema
	schemaDSN := schemaConn.ConnString()
	store, err = storage.OpenPostgres(ctx, schemaDSN)
	if err != nil {
		t.Fatal(err)
	}
	legacyMigrations := t.TempDir()
	for _, name := range []string{"0001_devices_telemetry.sql", "0002_reset_gate.sql"} {
		body, readErr := os.ReadFile(filepath.Join("../../migrations", name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if writeErr := os.WriteFile(filepath.Join(legacyMigrations, name), body, 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err = store.Migrate(ctx, legacyMigrations); err != nil {
		t.Fatal(err)
	}
	legacyPool, err := pgxpool.New(ctx, schemaDSN)
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("legacy-%d", time.Now().UnixNano())
	deletedID := fmt.Sprintf("legacy-deleted-%d", time.Now().UnixNano())
	secret := "legacy-device-secret"
	sampled := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	received := sampled.Add(5 * time.Second)
	latestSampled := sampled.Add(time.Hour)
	latestReceived := latestSampled.Add(5 * time.Second)
	secretDigest := device.HashSecret(secret)
	legacyMetrics := `{"pressure":{"value":101.3,"unit":"kPa"}}`
	if _, err = legacyPool.Exec(ctx, `INSERT INTO devices(id,name,enabled,secret_digest,created_at,updated_at,latest_sampled_at,latest_received_at,last_valid_received_at,latest_message_id,latest_metrics)
VALUES($1,'legacy device',true,$2,$3,$3,$4,$5,$5,'legacy-message-11',$6::jsonb)`, id, secretDigest[:], sampled, latestSampled, latestReceived, legacyMetrics); err != nil {
		legacyPool.Close()
		t.Fatal(err)
	}
	firstPartial := `{"temperature":{"value":20,"unit":"C"},"current":{"value":2.5,"unit":"A"}}`
	if _, err = legacyPool.Exec(ctx, `INSERT INTO telemetry_samples(device_id,message_id,sampled_at,received_at,metrics) VALUES($1,'legacy-message-10',$2,$3,$4::jsonb)`, id, sampled, received, firstPartial); err != nil {
		legacyPool.Close()
		t.Fatal(err)
	}
	if _, err = legacyPool.Exec(ctx, `INSERT INTO telemetry_samples(device_id,message_id,sampled_at,received_at,metrics) VALUES($1,'legacy-message-11',$2,$3,$4::jsonb)`, id, latestSampled, latestReceived, legacyMetrics); err != nil {
		legacyPool.Close()
		t.Fatal(err)
	}
	deletedAt := latestReceived.Add(time.Hour)
	deletedSampled := sampled.Add(2 * time.Minute)
	if _, err = legacyPool.Exec(ctx, `INSERT INTO devices(id,name,enabled,secret_digest,created_at,updated_at,deleted_at)
VALUES($1,'deleted legacy device',false,$2,$3,$3,$4)`, deletedID, secretDigest[:], sampled, deletedAt); err != nil {
		legacyPool.Close()
		t.Fatal(err)
	}
	deletedMetrics := []struct {
		key  string
		unit string
	}{
		{key: "temperature", unit: "C"},
		{key: "pressure", unit: "kPa"},
		{key: "current", unit: "A"},
		{key: "temperature", unit: "C"},
		{key: "pressure", unit: "kPa"},
	}
	for index, metric := range deletedMetrics {
		sampleAt := deletedSampled.Add(time.Duration(index) * time.Second)
		messageID := fmt.Sprintf("deleted-history-%d", index)
		metricJSON := fmt.Sprintf(`{"%s":{"value":%d,"unit":%q}}`, metric.key, index+1, metric.unit)
		if _, err = legacyPool.Exec(ctx, `INSERT INTO telemetry_samples(device_id,message_id,sampled_at,received_at,metrics)
VALUES($1,$2,$3,$4,$5::jsonb)`, deletedID, messageID, sampleAt, sampleAt.Add(time.Second), metricJSON); err != nil {
			legacyPool.Close()
			t.Fatal(err)
		}
	}
	legacyPool.Close()
	store.Close()
	store = nil

	store, err = storage.OpenPostgres(ctx, schemaDSN)
	if err != nil {
		t.Fatal(err)
	}
	metricMigrationDir := t.TempDir()
	metricMigration, err := os.ReadFile(filepath.Join("../../migrations", "0003_device_metric_definitions.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(metricMigrationDir, "0003_device_metric_definitions.sql"), metricMigration, 0600); err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx, metricMigrationDir); err != nil {
		t.Fatal(err)
	}
	verifyPool, err := pgxpool.New(ctx, schemaDSN)
	if err != nil {
		t.Fatal(err)
	}
	var latestCount int
	if err = verifyPool.QueryRow(ctx, `SELECT count(*) FROM device_metric_latest WHERE device_id=$1`, deletedID).Scan(&latestCount); err != nil {
		verifyPool.Close()
		t.Fatal(err)
	}
	if latestCount != 0 {
		verifyPool.Close()
		t.Fatalf("0003 不得从已删除设备历史重建 Latest，行数=%d", latestCount)
	}
	// Simulate a database where the earlier 0003 implementation already rebuilt a
	// tombstone row; 0004 must remove it without deleting the retained history.
	if _, err = verifyPool.Exec(ctx, `INSERT INTO device_metric_latest(device_id,metric_key,value,unit,sampled_at,received_at,message_id)
VALUES($1,'temperature',99,'C',$2,$3,'stale-deleted-latest')`, deletedID, deletedSampled, deletedSampled.Add(time.Second)); err != nil {
		verifyPool.Close()
		t.Fatal(err)
	}
	verifyPool.Close()
	if err = store.Migrate(ctx, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	verifyPool, err = pgxpool.New(ctx, schemaDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyPool.QueryRow(ctx, `SELECT count(*) FROM device_metric_latest WHERE device_id=$1`, deletedID).Scan(&latestCount); err != nil {
		verifyPool.Close()
		t.Fatal(err)
	}
	verifyPool.Close()
	if latestCount != 0 {
		t.Fatalf("0004 应清除墓碑设备遗留 Latest，行数=%d", latestCount)
	}
	if _, err = store.Get(ctx, deletedID); !errors.Is(err, device.ErrNotFound) {
		t.Fatalf("删除墓碑不应通过设备查询显示: %v", err)
	}
	deletedHistory, err := store.History(ctx, telemetry.HistoryQuery{DeviceID: deletedID, From: &deletedSampled, To: &deletedAt, Limit: 10})
	if err != nil || len(deletedHistory) != len(deletedMetrics) || deletedHistory[0].MessageID != "deleted-history-0" || deletedHistory[len(deletedHistory)-1].MessageID != "deleted-history-4" {
		t.Fatalf("迁移须保留墓碑设备原始历史: %#v %v", deletedHistory, err)
	}
	d, err := store.Get(ctx, id)
	if err != nil || d.Latest == nil || len(d.Latest.Metrics) != 3 {
		t.Fatalf("多条部分历史应重建三个独立 Latest 项: %#v %v", d, err)
	}
	for _, key := range []string{"temperature", "current"} {
		state := d.Latest.Metrics[key]
		if state.MessageID != "legacy-message-10" || !state.SampledAt.Equal(sampled) || !state.ReceivedAt.Equal(received) {
			t.Fatalf("迁移应保留 %s 的旧值时间和消息 ID: %#v", key, state)
		}
	}
	if state := d.Latest.Metrics["pressure"]; state.MessageID != "legacy-message-11" || !state.SampledAt.Equal(latestSampled) || !state.ReceivedAt.Equal(latestReceived) {
		t.Fatalf("最新历史消息中的 pressure 状态错误: %#v", state)
	}
	if d.LastValidReceivedAt == nil || !d.LastValidReceivedAt.Equal(latestReceived) || len(d.MetricDefinitions) != 3 {
		t.Fatalf("迁移应保留设备接收时间并创建默认定义: %#v", d)
	}
	history, err := store.History(ctx, telemetry.HistoryQuery{DeviceID: id, From: &sampled, To: &latestReceived, Limit: 10})
	if err != nil || len(history) != 2 || len(history[0].Metrics) != 2 || len(history[1].Metrics) != 1 {
		t.Fatalf("迁移不得改写旧历史子集: %#v %v", history, err)
	}
	// 旧摘要不轮换：以迁移前的密钥写入新消息，验证 credentials 仍可用。
	newSample := telemetry.Sample{DeviceID: id, MessageID: "after-migration", SampledAt: sampled.Add(time.Minute), ReceivedAt: latestReceived.Add(time.Minute), Metrics: map[string]telemetry.MetricValue{"temperature": {Value: 21, Unit: "C"}}}
	if err = store.Commit(ctx, id, secret, newSample); err != nil {
		t.Fatalf("原设备密钥在迁移后仍应有效: %v", err)
	}
}

type recoveryBroker struct {
	mu         sync.Mutex
	accounts   map[string]bool
	failCreate bool
	failSet    map[string]bool
}

func (b *recoveryBroker) CreateDevice(_ context.Context, id, secret string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.accounts[id] = true
	if b.failCreate {
		return errors.New("response lost")
	}
	return nil
}
func (b *recoveryBroker) SetDeviceEnabled(_ context.Context, id string, enabled bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failSet[id] {
		return errors.New("managed role conflict")
	}
	b.accounts[id] = enabled
	return nil
}
func (b *recoveryBroker) ResetDeviceSecret(_ context.Context, id, secret string, enabled bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.accounts[id] = enabled
	return nil
}
func (b *recoveryBroker) DeleteDevice(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.accounts, id)
	return nil
}

func TestPostgresLifecycleRecovery(t *testing.T) {
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("需要隔离测试数据库 PROJECT01_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	store, err := storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(ctx, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	broker := &recoveryBroker{accounts: map[string]bool{}, failCreate: true}
	service := device.NewManagedService(store, broker)
	id := fmt.Sprintf("pending-%d", time.Now().UnixNano())
	secret, err := service.Create(ctx, id, "test")
	if err == nil || secret != "" {
		t.Fatalf("响应丢失不应交付密钥: %q %v", secret, err)
	}
	pending, err := store.PendingOperations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range pending {
		if p.ID == id && p.Operation == "create" {
			found = true
		}
	}
	if !found {
		t.Fatal("创建意图未持久化")
	}
	restarted := device.NewManagedService(store, broker)
	if err = restarted.RecoverPending(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.CheckCreate(ctx, id); !errors.Is(err, device.ErrIDDeleted) {
		t.Fatalf("中断创建应撤销并保留墓碑: %v", err)
	}
	broker.mu.Lock()
	_, exists := broker.accounts[id]
	broker.mu.Unlock()
	if exists {
		t.Fatal("Broker 残留账户")
	}
	id = fmt.Sprintf("reset-%d", time.Now().UnixNano())
	now := time.Now().UTC()
	if err = store.Create(ctx, device.Device{ID: id, Name: "test", Enabled: true, CreatedAt: now, UpdatedAt: now}, device.HashSecret("old")); err != nil {
		t.Fatal(err)
	}
	broker.accounts[id] = true
	if err = store.BeginOperation(ctx, id, "reset", now); err != nil {
		t.Fatal(err)
	}
	broker.accounts[id] = true // 模拟 Broker 已轮换且响应丢失。
	if err = restarted.RecoverPending(ctx); err != nil {
		t.Fatal(err)
	}
	d, err := store.Get(ctx, id)
	if err != nil || d.Enabled {
		t.Fatalf("重置中断后应保持禁用: %#v %v", d, err)
	}
	broker.mu.Lock()
	enabled := broker.accounts[id]
	broker.mu.Unlock()
	if enabled {
		t.Fatal("重置中断后 Broker 仍启用")
	}
	if err = restarted.Enable(ctx, id); !errors.Is(err, device.ErrSecretResetRequired) {
		t.Fatalf("重置中断后不应直接启用: %v", err)
	}
	if err = store.SetEnabled(ctx, id, true, now); err == nil {
		t.Fatal("数据库约束也必须阻止绕过服务层直接启用")
	}
	newSecret, err := restarted.ResetSecret(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Enable(ctx, id); err != nil {
		t.Fatalf("重新重置后应可启用: %v", err)
	}
	sample := telemetry.Sample{DeviceID: id, MessageID: "after-reset", SampledAt: now, ReceivedAt: now, Metrics: map[string]telemetry.MetricValue{"segment-1": {Value: 20, Unit: "V"}}}
	if err = store.Commit(ctx, id, "old", sample); !errors.Is(err, telemetry.ErrInvalidSecret) {
		t.Fatalf("旧摘要仍有效: %v", err)
	}
	if err = store.Commit(ctx, id, newSecret, sample); err != nil {
		t.Fatalf("新密钥不能入库: %v", err)
	}
}

func TestPostgresRecoveryContinuesAfterOneDeviceFails(t *testing.T) {
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("需要隔离测试数据库 PROJECT01_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	store, err := storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(ctx, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UnixNano()
	first := fmt.Sprintf("a-fail-%d", stamp)
	alsoFail := fmt.Sprintf("b-fail-%d", stamp)
	second := fmt.Sprintf("z-good-%d", stamp)
	now := time.Now().UTC()
	for _, id := range []string{first, alsoFail, second} {
		if err = store.Create(ctx, device.Device{ID: id, Name: "test", Enabled: true, CreatedAt: now, UpdatedAt: now}, device.HashSecret("old")); err != nil {
			t.Fatal(err)
		}
		if err = store.BeginOperation(ctx, id, "disable", now); err != nil {
			t.Fatal(err)
		}
	}
	broker := &recoveryBroker{accounts: map[string]bool{first: true, alsoFail: true, second: true}, failSet: map[string]bool{first: true, alsoFail: true}}
	service := device.NewManagedService(store, broker)
	if err = service.RecoverPending(ctx); err == nil || !strings.Contains(err.Error(), first) || !strings.Contains(err.Error(), alsoFail) {
		t.Fatalf("应汇总两个失败设备: %v", err)
	}
	pending, err := store.PendingOperations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstPending, alsoPending, secondPending := false, false, false
	for _, p := range pending {
		if p.ID == first {
			firstPending = true
		}
		if p.ID == alsoFail {
			alsoPending = true
		}
		if p.ID == second {
			secondPending = true
		}
	}
	if !firstPending || !alsoPending || secondPending {
		t.Fatalf("失败项应保留，后续项应完成: %#v", pending)
	}
	broker.mu.Lock()
	secondEnabled := broker.accounts[second]
	broker.mu.Unlock()
	if secondEnabled {
		t.Fatal("后续设备的 Broker 账户未禁用")
	}
	broker.mu.Lock()
	delete(broker.failSet, first)
	delete(broker.failSet, alsoFail)
	broker.mu.Unlock()
	if err = service.RecoverPending(ctx); err != nil {
		t.Fatalf("故障修复后应完成剩余恢复: %v", err)
	}
}

func TestPostgresContainerRecreation(t *testing.T) {
	phase := os.Getenv("PROJECT01_TEST_RESTART_PHASE")
	if phase == "" {
		t.Skip("手动两阶段容器重建验证")
	}
	dsn := os.Getenv("PROJECT01_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("需要隔离数据库")
	}
	id := os.Getenv("PROJECT01_TEST_RESTART_ID")
	if id == "" {
		t.Fatal("需要测试设备 ID")
	}
	ctx := context.Background()
	store, err := storage.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(ctx, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	secret := "restart-secret"
	now := time.Now().UTC().Truncate(time.Microsecond)
	sample := telemetry.Sample{DeviceID: id, MessageID: "first", SampledAt: now.Add(-time.Minute), ReceivedAt: now, Metrics: map[string]telemetry.MetricValue{"segment-1": {Value: 21, Unit: "V"}}}
	switch phase {
	case "seed":
		if err = store.Create(ctx, device.Device{ID: id, Name: "recreation", Enabled: true, CreatedAt: now, UpdatedAt: now}, device.HashSecret(secret)); err != nil {
			t.Fatal(err)
		}
		if err = store.Commit(ctx, id, secret, sample); err != nil {
			t.Fatal(err)
		}
	case "check":
		d, e := store.Get(ctx, id)
		if e != nil || d.Latest == nil || d.Latest.Metrics["segment-1"].MessageID != "first" {
			t.Fatalf("重建后设备/Latest: %#v %v", d, e)
		}
		if err = store.Commit(ctx, id, secret, sample); !errors.Is(err, telemetry.ErrDuplicateMessage) {
			t.Fatalf("重建后原密钥/去重: %v", err)
		}
		sample.MessageID = "second"
		sample.SampledAt = now
		if err = store.Commit(ctx, id, secret, sample); err != nil {
			t.Fatalf("重建后原密钥不能写入: %v", err)
		}
	default:
		t.Fatalf("无效阶段 %q", phase)
	}
}
