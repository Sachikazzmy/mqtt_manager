package storage

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"Project/internal/device"
	"Project/internal/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	queryTimeout            = 5 * time.Second
	DefaultMigrationTimeout = 15 * time.Minute
)

type PostgresStore struct {
	pool        *pgxpool.Pool
	metricLimit int
}

var _ device.Repository = (*PostgresStore)(nil)
var _ telemetry.Repository = (*PostgresStore)(nil)

func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	return OpenPostgresWithMetricLimit(ctx, dsn, device.DefaultMetricLimit)
}

func OpenPostgresWithMetricLimit(ctx context.Context, dsn string, metricLimit int) (*PostgresStore, error) {
	if err := device.ValidateMetricLimit(metricLimit); err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析数据库连接配置: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 0
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &PostgresStore{pool: pool, metricLimit: metricLimit}
	pingCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接 PostgreSQL: %w", err)
	}
	return s, nil
}
func (s *PostgresStore) Close() { s.pool.Close() }
func (s *PostgresStore) Ping(ctx context.Context) error {
	q, c := context.WithTimeout(ctx, queryTimeout)
	defer c()
	return s.pool.Ping(q)
}

// Migrate 在单个数据库事务和 advisory lock 下顺序应用版本化 SQL 文件。
func (s *PostgresStore) Migrate(ctx context.Context, dir string) error {
	return s.MigrateWithTimeout(ctx, dir, DefaultMigrationTimeout)
}

// MigrateWithTimeout 使用独立于普通数据库查询期限的可配置迁移期限。
func (s *PostgresStore) MigrateWithTimeout(ctx context.Context, dir string, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("迁移超时时间必须大于零")
	}
	q, c := context.WithTimeout(ctx, timeout)
	defer c()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return err
	}
	defer tx.Rollback(q)
	if _, err = tx.Exec(q, `SELECT pg_advisory_xact_lock(70311401)`); err != nil {
		return err
	}
	if _, err = tx.Exec(q, `CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		var exists bool
		if err = tx.QueryRow(q, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		body, readErr := os.ReadFile(filepath.Join(dir, name))
		if readErr != nil {
			return readErr
		}
		if _, err = tx.Exec(q, string(body)); err != nil {
			return fmt.Errorf("迁移 %s: %w", name, err)
		}
		if _, err = tx.Exec(q, `INSERT INTO schema_migrations(version) VALUES($1)`, name); err != nil {
			return err
		}
	}
	return tx.Commit(q)
}

func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, queryTimeout)
}
func (s *PostgresStore) CheckCreate(ctx context.Context, id string) error {
	q, c := bounded(ctx)
	defer c()
	var deleted *time.Time
	err := s.pool.QueryRow(q, `SELECT deleted_at FROM devices WHERE id=$1`, id).Scan(&deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if deleted != nil {
		return device.ErrIDDeleted
	}
	return device.ErrExists
}
func (s *PostgresStore) Create(ctx context.Context, d device.Device, digest device.SecretDigest) error {
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return err
	}
	defer tx.Rollback(q)
	tag, err := tx.Exec(q, `INSERT INTO devices(id,name,enabled,secret_digest,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, d.ID, d.Name, d.Enabled, digest[:], d.CreatedAt, d.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if rollbackErr := tx.Rollback(q); rollbackErr != nil {
			return fmt.Errorf("结束重复设备创建事务: %w", rollbackErr)
		}
		return s.CheckCreate(q, d.ID)
	}
	return tx.Commit(q)
}

const deviceColumns = `id,name,enabled,created_at,updated_at,last_valid_received_at`

func scanDevice(row pgx.Row) (device.Device, error) {
	var d device.Device
	var last *time.Time
	err := row.Scan(&d.ID, &d.Name, &d.Enabled, &d.CreatedAt, &d.UpdatedAt, &last)
	if err != nil {
		return d, err
	}
	if last != nil {
		value := last.UTC()
		d.LastValidReceivedAt = &value
	}
	return d, nil
}
func (s *PostgresStore) Get(ctx context.Context, id string) (device.Device, error) {
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.BeginTx(q, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return device.Device{}, err
	}
	defer tx.Rollback(q)
	d, err := scanDevice(tx.QueryRow(q, `SELECT `+deviceColumns+` FROM devices WHERE id=$1 AND deleted_at IS NULL`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return d, device.ErrNotFound
	}
	if err != nil {
		return d, err
	}
	if err = loadMetricData(q, tx, []string{id}, []*device.Device{&d}); err != nil {
		return device.Device{}, err
	}
	if err = tx.Commit(q); err != nil {
		return device.Device{}, err
	}
	return d, nil
}
func (s *PostgresStore) List(ctx context.Context) ([]device.Device, error) {
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.BeginTx(q, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(q)
	rows, err := tx.Query(q, `SELECT `+deviceColumns+` FROM devices WHERE deleted_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []device.Device{}
	for rows.Next() {
		d, e := scanDevice(rows)
		if e != nil {
			return nil, e
		}
		result = append(result, d)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(result) == 0 {
		if err = tx.Commit(q); err != nil {
			return nil, err
		}
		return result, nil
	}
	ids := make([]string, len(result))
	devices := make([]*device.Device, len(result))
	for index := range result {
		ids[index] = result[index].ID
		devices[index] = &result[index]
	}
	if err = loadMetricData(q, tx, ids, devices); err != nil {
		return nil, err
	}
	if err = tx.Commit(q); err != nil {
		return nil, err
	}
	return result, nil
}

type metricDataQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func loadMetricData(ctx context.Context, queryer metricDataQuerier, ids []string, devices []*device.Device) error {
	byID := make(map[string]*device.Device, len(devices))
	for _, item := range devices {
		byID[item.ID] = item
	}
	definitionRows, err := queryer.Query(ctx, `SELECT device_id,metric_key,display_name,unit,min_value,max_value,enabled,display_order FROM device_metrics WHERE device_id = ANY($1) ORDER BY device_id,display_order,metric_key`, ids)
	if err != nil {
		return err
	}
	for definitionRows.Next() {
		var id string
		var definition device.MetricDefinition
		if err = definitionRows.Scan(&id, &definition.Key, &definition.DisplayName, &definition.Unit, &definition.MinValue, &definition.MaxValue, &definition.Enabled, &definition.DisplayOrder); err != nil {
			definitionRows.Close()
			return err
		}
		if target := byID[id]; target != nil {
			target.MetricDefinitions = append(target.MetricDefinitions, definition)
		}
	}
	if err = definitionRows.Err(); err != nil {
		definitionRows.Close()
		return err
	}
	definitionRows.Close()

	latestRows, err := queryer.Query(ctx, `SELECT device_id,metric_key,value,unit,modifiable,sampled_at,received_at,message_id FROM device_metric_latest WHERE device_id = ANY($1) ORDER BY device_id,metric_key`, ids)
	if err != nil {
		return err
	}
	defer latestRows.Close()
	for latestRows.Next() {
		var id, key string
		var state device.MetricState
		if err = latestRows.Scan(&id, &key, &state.Value, &state.Unit, &state.Modifiable, &state.SampledAt, &state.ReceivedAt, &state.MessageID); err != nil {
			return err
		}
		if target := byID[id]; target != nil {
			if target.Latest == nil {
				target.Latest = &device.LatestState{Metrics: make(map[string]device.MetricState)}
			}
			target.Latest.Metrics[key] = state
		}
	}
	return latestRows.Err()
}
func (s *PostgresStore) update(ctx context.Context, sql string, args ...any) error {
	q, c := bounded(ctx)
	defer c()
	tag, err := s.pool.Exec(q, sql, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return device.ErrNotFound
	}
	return nil
}
func (s *PostgresStore) UpdateName(ctx context.Context, id, name string, at time.Time) error {
	return s.update(ctx, `UPDATE devices SET name=$2,updated_at=GREATEST(updated_at,$3) WHERE id=$1 AND deleted_at IS NULL AND pending_operation IS NULL`, id, name, at)
}
func (s *PostgresStore) SetEnabled(ctx context.Context, id string, enabled bool, at time.Time) error {
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return err
	}
	defer tx.Rollback(q)
	if _, err = tx.Exec(q, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id); err != nil {
		return err
	}
	tag, err := tx.Exec(q, `UPDATE devices SET enabled=$2,updated_at=GREATEST(updated_at,$3) WHERE id=$1 AND deleted_at IS NULL AND pending_operation IS NULL`, id, enabled, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return device.ErrNotFound
	}
	return tx.Commit(q)
}
func (s *PostgresStore) ResetSecret(ctx context.Context, id string, digest device.SecretDigest, at time.Time) error {
	return s.update(ctx, `UPDATE devices SET secret_digest=$2,updated_at=GREATEST(updated_at,$3) WHERE id=$1 AND deleted_at IS NULL AND pending_operation IS NULL`, id, digest[:], at)
}
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return err
	}
	defer tx.Rollback(q)
	if _, err = tx.Exec(q, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id); err != nil {
		return err
	}
	tag, err := tx.Exec(q, `UPDATE devices SET deleted_at=now(),enabled=false,secret_digest=decode(repeat('00',32),'hex'),latest_sampled_at=NULL,latest_received_at=NULL,last_valid_received_at=NULL,latest_message_id=NULL,latest_metrics=NULL WHERE id=$1 AND deleted_at IS NULL AND pending_operation IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return device.ErrNotFound
	}
	if _, err = tx.Exec(q, `DELETE FROM device_metric_latest WHERE device_id=$1`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(q, `UPDATE device_commands SET status='cancelled',last_error='设备已删除'
WHERE device_id=$1 AND status IN ('waiting_to_send','broker_acked','result_unknown')`, id); err != nil {
		return err
	}
	return tx.Commit(q)
}

func (s *PostgresStore) Commit(ctx context.Context, id, secret string, sample telemetry.Sample) error {
	return s.commit(ctx, id, &secret, sample)
}
func (s *PostgresStore) CommitFromBroker(ctx context.Context, id string, sample telemetry.Sample) error {
	return s.commit(ctx, id, nil, sample)
}
func (s *PostgresStore) commit(ctx context.Context, id string, secret *string, sample telemetry.Sample) (err error) {
	if sample.DeviceID != id {
		return telemetry.ErrDeviceMismatch
	}
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return err
	}
	defer tx.Rollback(q)
	var enabled bool
	var digest []byte
	var pending *string
	err = tx.QueryRow(q, `SELECT enabled,secret_digest,pending_operation FROM devices WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, id).Scan(&enabled, &digest, &pending)
	if errors.Is(err, pgx.ErrNoRows) {
		return device.ErrNotFound
	}
	if err != nil {
		return err
	}
	if secret != nil {
		provided := device.HashSecret(*secret)
		if !hmac.Equal(digest, provided[:]) {
			return telemetry.ErrInvalidSecret
		}
	}
	if pending != nil {
		return telemetry.ErrDeviceTransition
	}
	if !enabled {
		return telemetry.ErrDeviceDisabled
	}
	metrics, e := json.Marshal(sample.Metrics)
	if e != nil {
		return e
	}
	sampled := sample.SampledAt.UTC().Truncate(time.Microsecond)
	received := sample.ReceivedAt.UTC().Truncate(time.Microsecond)
	tag, e := tx.Exec(q, `INSERT INTO telemetry_samples(device_id,message_id,sampled_at,received_at,metrics) VALUES($1,$2,$3,$4,$5) ON CONFLICT (device_id,message_id) DO NOTHING`, id, sample.MessageID, sampled, received, metrics)
	if e != nil {
		return classifyTelemetryWriteError(e)
	}
	if tag.RowsAffected() == 0 {
		return telemetry.ErrDuplicateMessage
	}
	definitions, e := loadMetricDefinitionsTx(q, tx, id)
	if e != nil {
		return e
	}
	resolvedDefinitions, e := telemetry.ResolveMetricsAgainstDefinitions(sample.Metrics, definitions, s.metricLimit)
	if e != nil {
		return e
	}
	existingDefinitions := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		existingDefinitions[definition.Key] = struct{}{}
	}
	for _, definition := range resolvedDefinitions {
		if _, exists := existingDefinitions[definition.Key]; exists {
			continue
		}
		if _, e = tx.Exec(q, `INSERT INTO device_metrics(device_id,metric_key,display_name,unit,min_value,max_value,enabled,display_order) VALUES($1,$2,$3,$4,$5,$6,true,$7)`, id, definition.Key, definition.DisplayName, definition.Unit, definition.MinValue, definition.MaxValue, definition.DisplayOrder); e != nil {
			return classifyTelemetryWriteError(e)
		}
	}
	keys := make([]string, 0, len(sample.Metrics))
	for key := range sample.Metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		metric := sample.Metrics[key]
		_, e = tx.Exec(q, `INSERT INTO device_metric_latest(device_id,metric_key,value,unit,modifiable,sampled_at,received_at,message_id)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (device_id,metric_key) DO UPDATE SET
 value=EXCLUDED.value,unit=EXCLUDED.unit,modifiable=EXCLUDED.modifiable,sampled_at=EXCLUDED.sampled_at,received_at=EXCLUDED.received_at,message_id=EXCLUDED.message_id
WHERE (device_metric_latest.sampled_at,device_metric_latest.received_at,device_metric_latest.message_id)
    < (EXCLUDED.sampled_at,EXCLUDED.received_at,EXCLUDED.message_id)`, id, key, metric.Value, metric.Unit, metric.Modifiable, sampled, received, sample.MessageID)
		if e != nil {
			return classifyTelemetryWriteError(e)
		}
	}
	_, e = tx.Exec(q, `UPDATE devices SET last_valid_received_at=GREATEST(COALESCE(last_valid_received_at,$2),$2) WHERE id=$1`, id, received)
	if e != nil {
		return classifyTelemetryWriteError(e)
	}
	return tx.Commit(q)
}

func loadMetricDefinitionsTx(ctx context.Context, tx pgx.Tx, id string) ([]device.MetricDefinition, error) {
	rows, err := tx.Query(ctx, `SELECT metric_key,display_name,unit,min_value,max_value,enabled,display_order FROM device_metrics WHERE device_id=$1 ORDER BY display_order,metric_key`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	definitions := []device.MetricDefinition{}
	for rows.Next() {
		var definition device.MetricDefinition
		if err = rows.Scan(&definition.Key, &definition.DisplayName, &definition.Unit, &definition.MinValue, &definition.MaxValue, &definition.Enabled, &definition.DisplayOrder); err != nil {
			return nil, err
		}
		definitions = append(definitions, definition)
	}
	return definitions, rows.Err()
}

// 只把已知的输入编码错误归为永久拒收；连接、超时和提交结果不确定仍交给 MQTT 重投。
func classifyTelemetryWriteError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "22021" {
		return fmt.Errorf("%w：文本包含 PostgreSQL 不接受的字符", telemetry.ErrInvalidMessage)
	}
	return err
}

func (s *PostgresStore) History(ctx context.Context, query telemetry.HistoryQuery) ([]telemetry.Sample, error) {
	if query.Limit <= 0 || query.Limit > telemetry.MaxHistoryLimit {
		return nil, telemetry.ErrHistoryLimit
	}
	if query.From == nil || query.To == nil || query.From.After(*query.To) || query.To.Sub(*query.From) > telemetry.MaxHistoryWindow {
		return nil, telemetry.ErrHistoryWindow
	}
	q, c := bounded(ctx)
	defer c()
	rows, err := s.pool.Query(q, `SELECT device_id,message_id,sampled_at,received_at,metrics FROM (
 SELECT device_id,message_id,sampled_at,received_at,metrics FROM telemetry_samples
 WHERE ($1='' OR device_id=$1) AND sampled_at >= $2 AND sampled_at <= $3
 ORDER BY sampled_at DESC,received_at DESC,message_id DESC,device_id DESC LIMIT $4
) recent ORDER BY sampled_at,received_at,message_id,device_id`, query.DeviceID, query.From.UTC(), query.To.UTC(), query.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []telemetry.Sample{}
	for rows.Next() {
		var sample telemetry.Sample
		var metrics []byte
		if err = rows.Scan(&sample.DeviceID, &sample.MessageID, &sample.SampledAt, &sample.ReceivedAt, &metrics); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(metrics, &sample.Metrics); err != nil {
			return nil, err
		}
		out = append(out, sample)
	}
	return out, rows.Err()
}
