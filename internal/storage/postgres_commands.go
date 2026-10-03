package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"Project/internal/command"
	"Project/internal/device"
	"github.com/jackc/pgx/v5"
)

var _ command.Repository = (*PostgresStore)(nil)

const commandColumns = `device_id,command_id,action,metric_key,value,status,created_at,attempts,last_attempt_at,next_attempt_at,deadline_at,broker_acked_at,result_received_at,last_error`

func scanCommand(row pgx.Row) (command.Command, error) {
	var entry command.Command
	err := row.Scan(
		&entry.DeviceID, &entry.CommandID, &entry.Action, &entry.MetricKey, &entry.Value,
		&entry.Status, &entry.CreatedAt, &entry.Attempts, &entry.LastAttemptAt,
		&entry.NextAttemptAt, &entry.DeadlineAt, &entry.BrokerAckedAt,
		&entry.ResultReceivedAt, &entry.LastError,
	)
	if err == nil {
		entry.CreatedAt = entry.CreatedAt.UTC()
		entry.NextAttemptAt = entry.NextAttemptAt.UTC()
		entry.DeadlineAt = entry.DeadlineAt.UTC()
	}
	return entry, err
}

func (s *PostgresStore) CreateCommand(ctx context.Context, entry command.Command) (command.Command, error) {
	q, cancel := bounded(ctx)
	defer cancel()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return command.Command{}, err
	}
	defer tx.Rollback(q)

	existing, err := scanCommand(tx.QueryRow(q, `SELECT `+commandColumns+` FROM device_commands WHERE device_id=$1 AND command_id=$2 FOR UPDATE`, entry.DeviceID, entry.CommandID))
	if err == nil {
		if !sameCommandContent(existing, entry) {
			return command.Command{}, command.ErrCommandConflict
		}
		if err = tx.Commit(q); err != nil {
			return command.Command{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return command.Command{}, err
	}

	var enabled bool
	var pending *string
	err = tx.QueryRow(q, `SELECT enabled,pending_operation FROM devices WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, entry.DeviceID).Scan(&enabled, &pending)
	if errors.Is(err, pgx.ErrNoRows) {
		return command.Command{}, device.ErrNotFound
	}
	if err != nil {
		return command.Command{}, err
	}
	if !enabled || pending != nil {
		return command.Command{}, command.ErrDeviceUnavailable
	}
	var modifiable *bool
	var metricEnabled *bool
	var minimum, maximum *float64
	err = tx.QueryRow(q, `SELECT l.modifiable,m.enabled,m.min_value,m.max_value
FROM device_metric_latest AS l
JOIN device_metrics AS m USING (device_id,metric_key)
WHERE l.device_id=$1 AND l.metric_key=$2`, entry.DeviceID, entry.MetricKey).Scan(&modifiable, &metricEnabled, &minimum, &maximum)
	if errors.Is(err, pgx.ErrNoRows) {
		return command.Command{}, command.ErrMetricNotReady
	}
	if err != nil {
		return command.Command{}, err
	}
	if modifiable == nil || !*modifiable || metricEnabled == nil || !*metricEnabled {
		return command.Command{}, command.ErrMetricNotMutable
	}
	if entry.Value != nil && (minimum != nil && *entry.Value < *minimum || maximum != nil && *entry.Value > *maximum) {
		return command.Command{}, fmt.Errorf("%w：value 超出已配置的指标范围", command.ErrInvalidCommand)
	}

	created, err := scanCommand(tx.QueryRow(q, `INSERT INTO device_commands
(device_id,command_id,action,metric_key,value,status,created_at,attempts,next_attempt_at,deadline_at)
VALUES($1,$2,$3,$4,$5,$6,$7,0,$8,$9)
ON CONFLICT DO NOTHING
RETURNING `+commandColumns,
		entry.DeviceID, entry.CommandID, entry.Action, entry.MetricKey, entry.Value, entry.Status,
		entry.CreatedAt, entry.NextAttemptAt, entry.DeadlineAt,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		created, err = scanCommand(tx.QueryRow(q, `SELECT `+commandColumns+` FROM device_commands WHERE device_id=$1 AND command_id=$2`, entry.DeviceID, entry.CommandID))
		if err == nil {
			if !sameCommandContent(created, entry) {
				return command.Command{}, command.ErrCommandConflict
			}
		} else if errors.Is(err, pgx.ErrNoRows) {
			return command.Command{}, command.ErrCommandPending
		}
	}
	if err != nil {
		return command.Command{}, err
	}
	if err = tx.Commit(q); err != nil {
		return command.Command{}, err
	}
	return created, nil
}

func sameCommandContent(left, right command.Command) bool {
	if left.Action != right.Action || left.MetricKey != right.MetricKey || left.Value == nil != (right.Value == nil) {
		return false
	}
	return left.Value == nil || *left.Value == *right.Value
}

func (s *PostgresStore) GetCommand(ctx context.Context, deviceID, commandID string) (command.Command, error) {
	q, cancel := bounded(ctx)
	defer cancel()
	entry, err := scanCommand(s.pool.QueryRow(q, `SELECT `+commandColumns+` FROM device_commands WHERE device_id=$1 AND command_id=$2`, deviceID, commandID))
	if errors.Is(err, pgx.ErrNoRows) {
		return command.Command{}, command.ErrCommandNotFound
	}
	return entry, err
}

func (s *PostgresStore) CommandHistory(ctx context.Context, deviceID string, limit int) ([]command.Command, error) {
	if limit <= 0 || limit > command.MaxHistoryLimit {
		return nil, command.ErrCommandLimit
	}
	q, cancel := bounded(ctx)
	defer cancel()
	rows, err := s.pool.Query(q, `SELECT `+commandColumns+` FROM device_commands WHERE device_id=$1 ORDER BY created_at DESC,command_id DESC LIMIT $2`, deviceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]command.Command, 0, limit)
	for rows.Next() {
		entry, scanErr := scanCommand(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (s *PostgresStore) ClaimDue(ctx context.Context, now time.Time, limit, maxAttempts int, retryBase, retryMaximum time.Duration) ([]command.Command, error) {
	q, cancel := bounded(ctx)
	defer cancel()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(q)
	rows, err := tx.Query(q, `WITH due AS (
    SELECT c.device_id,c.command_id
    FROM device_commands AS c
    JOIN devices AS d ON d.id=c.device_id
    WHERE c.status IN ('waiting_to_send','broker_acked')
      AND c.next_attempt_at <= $1 AND c.deadline_at > $1 AND c.attempts < $3
      AND d.enabled AND d.pending_operation IS NULL AND d.deleted_at IS NULL
    ORDER BY c.next_attempt_at,c.created_at,c.command_id
    LIMIT $2
    FOR UPDATE OF c SKIP LOCKED
)
UPDATE device_commands AS c
SET attempts=c.attempts+1,
    last_attempt_at=$1,
    next_attempt_at=$1 + LEAST($4::double precision * interval '1 second' * power(2::double precision,c.attempts),$5::double precision * interval '1 second')
FROM due
WHERE c.device_id=due.device_id AND c.command_id=due.command_id
RETURNING c.device_id,c.command_id,c.action,c.metric_key,c.value,c.status,c.created_at,c.attempts,c.last_attempt_at,c.next_attempt_at,c.deadline_at,c.broker_acked_at,c.result_received_at,c.last_error`,
		now.UTC(), limit, maxAttempts, retryBase/time.Second, retryMaximum/time.Second,
	)
	if err != nil {
		return nil, err
	}
	entries := make([]command.Command, 0, limit)
	for rows.Next() {
		entry, scanErr := scanCommand(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		entries = append(entries, entry)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if err = tx.Commit(q); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].NextAttemptAt.Equal(entries[j].NextAttemptAt) {
			return entries[i].NextAttemptAt.Before(entries[j].NextAttemptAt)
		}
		return entries[i].CommandID < entries[j].CommandID
	})
	return entries, nil
}

// DispatchDue holds a per-device PostgreSQL advisory lock while publishing.
// Lifecycle transitions acquire the same lock, so they either finish first and
// suppress this send, or wait until an already-started bounded publish ends.
func (s *PostgresStore) DispatchDue(ctx context.Context, now time.Time, limit, maxAttempts int, retryBase, retryMaximum time.Duration, publish func(context.Context, command.Command) error) error {
	for count := 0; count < limit; count++ {
		dispatched, err := s.dispatchOne(ctx, now, maxAttempts, retryBase, retryMaximum, publish)
		if err != nil {
			return err
		}
		if !dispatched {
			return nil
		}
	}
	return nil
}

func (s *PostgresStore) dispatchOne(ctx context.Context, now time.Time, maxAttempts int, retryBase, retryMaximum time.Duration, publish func(context.Context, command.Command) error) (bool, error) {
	q, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var deviceID string
	err := s.pool.QueryRow(q, `SELECT c.device_id
FROM device_commands AS c
JOIN devices AS d ON d.id=c.device_id
WHERE c.status IN ('waiting_to_send','broker_acked')
  AND c.next_attempt_at <= $1 AND c.deadline_at > $1 AND c.attempts < $2
  AND d.enabled AND d.pending_operation IS NULL AND d.deleted_at IS NULL
ORDER BY c.next_attempt_at,c.created_at,c.command_id
LIMIT 1`, now.UTC(), maxAttempts).Scan(&deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	connection, err := s.pool.Acquire(q)
	if err != nil {
		return false, err
	}
	defer connection.Release()
	if _, err = connection.Exec(q, `SELECT pg_advisory_lock(hashtextextended($1,0))`, deviceID); err != nil {
		return false, err
	}
	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer unlockCancel()
		if _, unlockErr := connection.Exec(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, deviceID); unlockErr != nil {
			_ = connection.Hijack().Close(context.Background())
		}
	}()

	// Persist the retry reservation before publishing. A crash after PUBLISH but
	// before recording its outcome must still consume one of the bounded tries.
	claimTx, err := connection.Begin(q)
	if err != nil {
		return false, err
	}
	defer claimTx.Rollback(q)
	entry, err := scanCommand(claimTx.QueryRow(q, `SELECT `+strings.ReplaceAll(commandColumns, ",", ",c.")+`
FROM device_commands AS c
JOIN devices AS d ON d.id=c.device_id
WHERE c.device_id=$1
  AND c.status IN ('waiting_to_send','broker_acked')
  AND c.next_attempt_at <= $2 AND c.deadline_at > $2 AND c.attempts < $3
  AND d.enabled AND d.pending_operation IS NULL AND d.deleted_at IS NULL
ORDER BY c.next_attempt_at,c.created_at,c.command_id
LIMIT 1
FOR UPDATE OF c SKIP LOCKED`, deviceID, now.UTC(), maxAttempts))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	firstAttempt := entry.Attempts == 0
	var modifiable, metricEnabled *bool
	var minimum, maximum *float64
	err = claimTx.QueryRow(q, `SELECT l.modifiable,m.enabled,m.min_value,m.max_value
FROM device_metric_latest AS l
JOIN device_metrics AS m USING (device_id,metric_key)
WHERE l.device_id=$1 AND l.metric_key=$2`, entry.DeviceID, entry.MetricKey).Scan(&modifiable, &metricEnabled, &minimum, &maximum)
	mutable := err == nil && modifiable != nil && *modifiable && metricEnabled != nil && *metricEnabled
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if mutable && entry.Value != nil && (minimum != nil && *entry.Value < *minimum || maximum != nil && *entry.Value > *maximum) {
		mutable = false
	}
	if !mutable {
		status := command.StatusResultUnknown
		reason := "重试前 Latest 已声明不可修改或数值超出范围；设备执行情况未知"
		if firstAttempt && entry.Status == command.StatusWaitingToSend {
			status = command.StatusCancelled
			reason = "发送前 Latest 已声明不可修改或数值超出已配置范围"
		}
		if _, err = claimTx.Exec(q, `UPDATE device_commands SET status=$3,last_error=$4 WHERE device_id=$1 AND command_id=$2`, entry.DeviceID, entry.CommandID, status, reason); err != nil {
			return false, err
		}
		if err = claimTx.Commit(q); err != nil {
			return false, err
		}
		return true, nil
	}
	entry.Attempts++
	attemptAt := now.UTC().Truncate(time.Microsecond)
	entry.LastAttemptAt = &attemptAt
	entry.NextAttemptAt = attemptAt.Add(retryDelay(entry.Attempts-1, retryBase, retryMaximum)).UTC()
	if _, err = claimTx.Exec(q, `UPDATE device_commands SET attempts=$3,last_attempt_at=$4,next_attempt_at=$5
WHERE device_id=$1 AND command_id=$2`, entry.DeviceID, entry.CommandID, entry.Attempts, entry.LastAttemptAt, entry.NextAttemptAt); err != nil {
		return false, err
	}
	if err = claimTx.Commit(q); err != nil {
		return false, err
	}

	// A prior attempt's result can win the gap between reservation and send.
	// Re-read the row so a terminal command is not published again.
	sendTx, err := connection.Begin(q)
	if err != nil {
		return false, err
	}
	defer sendTx.Rollback(q)
	entry, err = scanCommand(sendTx.QueryRow(q, `SELECT `+strings.ReplaceAll(commandColumns, ",", ",c.")+`
FROM device_commands AS c
JOIN devices AS d ON d.id=c.device_id
WHERE c.device_id=$1 AND c.command_id=$2
  AND d.enabled AND d.pending_operation IS NULL AND d.deleted_at IS NULL
FOR UPDATE OF c`, entry.DeviceID, entry.CommandID))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if entry.Status != command.StatusWaitingToSend && entry.Status != command.StatusBrokerAcked {
		if err = sendTx.Commit(q); err != nil {
			return false, err
		}
		return true, nil
	}
	var sendModifiable, sendMetricEnabled *bool
	var sendMinimum, sendMaximum *float64
	err = sendTx.QueryRow(q, `SELECT l.modifiable,m.enabled,m.min_value,m.max_value
FROM device_metric_latest AS l
JOIN device_metrics AS m USING (device_id,metric_key)
WHERE l.device_id=$1 AND l.metric_key=$2`, entry.DeviceID, entry.MetricKey).Scan(&sendModifiable, &sendMetricEnabled, &sendMinimum, &sendMaximum)
	sendMutable := err == nil && sendModifiable != nil && *sendModifiable && sendMetricEnabled != nil && *sendMetricEnabled
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if sendMutable && entry.Value != nil && (sendMinimum != nil && *entry.Value < *sendMinimum || sendMaximum != nil && *entry.Value > *sendMaximum) {
		sendMutable = false
	}
	if !sendMutable {
		status := command.StatusResultUnknown
		reason := "Latest 在发送前撤回修改能力或数值超出范围；设备执行情况未知"
		if firstAttempt && entry.Status == command.StatusWaitingToSend {
			status = command.StatusCancelled
			reason = "发送前 Latest 已声明不可修改或数值超出已配置范围"
		}
		if _, err = sendTx.Exec(q, `UPDATE device_commands SET status=$3,last_error=$4 WHERE device_id=$1 AND command_id=$2`, entry.DeviceID, entry.CommandID, status, reason); err != nil {
			return false, err
		}
		if err = sendTx.Commit(q); err != nil {
			return false, err
		}
		return true, nil
	}

	publishErr := publish(q, entry)
	if publishErr != nil {
		reason := publishErr.Error()
		if len(reason) > 512 {
			reason = reason[:512]
		}
		if _, err = sendTx.Exec(q, `UPDATE device_commands SET last_error=$3 WHERE device_id=$1 AND command_id=$2 AND status IN ('waiting_to_send','broker_acked')`, entry.DeviceID, entry.CommandID, reason); err != nil {
			return false, err
		}
	} else {
		ackedAt := time.Now().UTC().Truncate(time.Microsecond)
		if _, err = sendTx.Exec(q, `UPDATE device_commands SET status='broker_acked',broker_acked_at=COALESCE(broker_acked_at,$3),last_error=''
WHERE device_id=$1 AND command_id=$2 AND status IN ('waiting_to_send','broker_acked')`, entry.DeviceID, entry.CommandID, ackedAt); err != nil {
			return false, err
		}
	}
	if err = sendTx.Commit(q); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) MarkBrokerAck(ctx context.Context, deviceID, commandID string, at time.Time) error {
	q, cancel := bounded(ctx)
	defer cancel()
	_, err := s.pool.Exec(q, `UPDATE device_commands SET status='broker_acked',broker_acked_at=COALESCE(broker_acked_at,$3),last_error=''
WHERE device_id=$1 AND command_id=$2 AND status IN ('waiting_to_send','broker_acked')`, deviceID, commandID, at.UTC())
	return err
}

func (s *PostgresStore) MarkPublishFailure(ctx context.Context, deviceID, commandID, reason string, at time.Time) error {
	if len(reason) > 512 {
		reason = reason[:512]
	}
	q, cancel := bounded(ctx)
	defer cancel()
	_, err := s.pool.Exec(q, `UPDATE device_commands SET last_error=$3 WHERE device_id=$1 AND command_id=$2 AND status IN ('waiting_to_send','broker_acked')`, deviceID, commandID, reason)
	return err
}

func (s *PostgresStore) Expire(ctx context.Context, now time.Time, _ int) error {
	q, cancel := bounded(ctx)
	defer cancel()
	_, err := s.pool.Exec(q, `UPDATE device_commands SET
    status=CASE WHEN attempts=0 THEN 'cancelled' ELSE 'result_unknown' END,
    last_error=CASE WHEN attempts=0 THEN '期限内未能发送；设备执行情况确定为未发送'
                    ELSE '命令期限结束仍未收到设备结果；执行情况未知' END
WHERE status IN ('waiting_to_send','broker_acked') AND deadline_at <= $1`, now.UTC())
	return err
}

func (s *PostgresStore) ProcessResult(ctx context.Context, deviceID string, result command.Result, raw []byte, at time.Time) (command.Disposition, error) {
	q, cancel := bounded(ctx)
	defer cancel()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return command.Disposition{}, err
	}
	defer tx.Rollback(q)
	var pending *string
	err = tx.QueryRow(q, `SELECT pending_operation FROM devices WHERE id=$1 FOR UPDATE`, deviceID).Scan(&pending)
	if errors.Is(err, pgx.ErrNoRows) {
		if err = recordCommandAnomaly(q, tx, deviceID, result.CommandID, "结果 Topic 对应的设备不存在", raw, at); err != nil {
			return command.Disposition{}, err
		}
		if err = tx.Commit(q); err != nil {
			return command.Disposition{}, err
		}
		return command.Disposition{Anomaly: "结果 Topic 对应的设备不存在"}, nil
	}
	if err != nil {
		return command.Disposition{}, err
	}
	if pending != nil {
		return command.Disposition{}, command.ErrDeviceTransition
	}
	entry, err := scanCommand(tx.QueryRow(q, `SELECT `+commandColumns+` FROM device_commands WHERE device_id=$1 AND command_id=$2 FOR UPDATE`, deviceID, result.CommandID))
	if errors.Is(err, pgx.ErrNoRows) {
		if err = recordCommandAnomaly(q, tx, deviceID, result.CommandID, "未知 command_id 或结果 Topic 设备不匹配", raw, at); err != nil {
			return command.Disposition{}, err
		}
		if err = tx.Commit(q); err != nil {
			return command.Disposition{}, err
		}
		return command.Disposition{Anomaly: "未知 command_id 或结果 Topic 设备不匹配"}, nil
	}
	if err != nil {
		return command.Disposition{}, err
	}
	problem := validateCommandResult(entry, result)
	if problem == "" && entry.Status == command.StatusCancelled {
		problem = "已取消命令收到迟到结果"
	}
	if problem == "" && (entry.Status == command.StatusApplied || entry.Status == command.StatusRejected) {
		if entry.Status == result.Status {
			if err = tx.Commit(q); err != nil {
				return command.Disposition{}, err
			}
			return command.Disposition{Command: &entry, Duplicate: true}, nil
		}
		problem = "同一命令收到冲突的终态结果"
	}
	if problem != "" {
		if err = recordCommandAnomaly(q, tx, deviceID, result.CommandID, problem, raw, at); err != nil {
			return command.Disposition{}, err
		}
		if err = tx.Commit(q); err != nil {
			return command.Disposition{}, err
		}
		return command.Disposition{Command: &entry, Anomaly: problem}, nil
	}
	entry.Status = result.Status
	entry.ResultReceivedAt = &at
	entry.LastError = ""
	_, err = tx.Exec(q, `UPDATE device_commands SET status=$3,result_received_at=$4,last_error=''
WHERE device_id=$1 AND command_id=$2`, deviceID, result.CommandID, result.Status, at.UTC())
	if err != nil {
		return command.Disposition{}, err
	}
	if err = tx.Commit(q); err != nil {
		return command.Disposition{}, err
	}
	return command.Disposition{Command: &entry}, nil
}

func validateCommandResult(entry command.Command, result command.Result) string {
	if entry.MetricKey != result.MetricKey {
		return "结果 metric_key 与命令不匹配"
	}
	switch entry.Action {
	case command.ActionSetMetric:
		if result.Value == nil || entry.Value == nil || *result.Value != *entry.Value {
			return "set_metric 结果缺少或不匹配请求回显 value"
		}
	case command.ActionClearOverride:
		if result.Value != nil {
			return "clear_override 结果不应包含 value"
		}
	default:
		return "存储命令 action 无效"
	}
	return ""
}

func recordCommandAnomaly(ctx context.Context, tx pgx.Tx, deviceID, commandID, reason string, raw []byte, at time.Time) error {
	if len(raw) > command.MaxResultBytes {
		raw = raw[:command.MaxResultBytes]
	}
	_, err := tx.Exec(ctx, `INSERT INTO command_result_anomalies(topic_device_id,command_id,reason,payload,received_at) VALUES($1,$2,$3,$4,$5)`, deviceID, commandID, reason, string(raw), at.UTC())
	return err
}

func (s *PostgresStore) CancelDeviceCommands(ctx context.Context, deviceID string) error {
	q, cancel := bounded(ctx)
	defer cancel()
	_, err := s.pool.Exec(q, `UPDATE device_commands SET status='cancelled',last_error='设备已删除'
WHERE device_id=$1 AND status IN ('waiting_to_send','broker_acked','result_unknown')`, deviceID)
	return err
}
