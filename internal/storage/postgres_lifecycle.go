package storage

import (
	"context"
	"errors"
	"time"

	"Project/internal/device"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ device.DurableLifecycle = (*PostgresStore)(nil)

func (s *PostgresStore) BeginCreate(ctx context.Context, d device.Device, digest device.SecretDigest) error {
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return err
	}
	defer tx.Rollback(q)
	tag, err := tx.Exec(q, `INSERT INTO devices(id,name,enabled,secret_digest,created_at,updated_at,pending_operation) VALUES($1,$2,false,$3,$4,$5,'create') ON CONFLICT DO NOTHING`, d.ID, d.Name, digest[:], d.CreatedAt, d.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if rollbackErr := tx.Rollback(q); rollbackErr != nil {
			return errors.Join(errors.New("结束重复设备创建事务失败"), rollbackErr)
		}
		return s.CheckCreate(q, d.ID)
	}
	return tx.Commit(q)
}
func (s *PostgresStore) BeginOperation(ctx context.Context, id, operation string, at time.Time) error {
	if operation != "enable" && operation != "disable" && operation != "reset" && operation != "delete" {
		return errors.New("无效的设备操作")
	}
	q, c := bounded(ctx)
	defer c()
	tag, err := s.pool.Exec(q, `UPDATE devices SET enabled=false,pending_operation=$2,updated_at=GREATEST(updated_at,$3) WHERE id=$1 AND deleted_at IS NULL AND pending_operation IS NULL AND ($2 <> 'enable' OR NOT requires_secret_reset)`, id, operation, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if operation == "enable" {
			var resetRequired bool
			var pending *string
			checkErr := s.pool.QueryRow(q, `SELECT requires_secret_reset,pending_operation FROM devices WHERE id=$1 AND deleted_at IS NULL`, id).Scan(&resetRequired, &pending)
			if checkErr == nil && pending == nil && resetRequired {
				return device.ErrSecretResetRequired
			}
			if checkErr != nil && !errors.Is(checkErr, pgx.ErrNoRows) {
				return checkErr
			}
		}
		return device.ErrPendingOrNotFound
	}
	return nil
}
func (s *PostgresStore) FinishOperation(ctx context.Context, id, operation string, digest *device.SecretDigest, enabled bool, at time.Time) error {
	q, c := bounded(ctx)
	defer c()
	tx, err := s.pool.Begin(q)
	if err != nil {
		return err
	}
	defer tx.Rollback(q)
	var tag pgconn.CommandTag
	if operation == "delete" || operation == "create-abort" {
		expected := "delete"
		if operation == "create-abort" {
			expected = "create"
		}
		tag, err = tx.Exec(q, `UPDATE devices SET deleted_at=$3,enabled=false,pending_operation=NULL,secret_digest=decode(repeat('00',32),'hex'),latest_sampled_at=NULL,latest_received_at=NULL,last_valid_received_at=NULL,latest_message_id=NULL,latest_metrics=NULL WHERE id=$1 AND pending_operation=$2 AND deleted_at IS NULL`, id, expected, at)
		if err == nil && tag.RowsAffected() > 0 {
			_, err = tx.Exec(q, `DELETE FROM device_metric_latest WHERE device_id=$1`, id)
		}
	} else {
		var digestBytes []byte
		if digest != nil {
			digestBytes = digest[:]
		}
		tag, err = tx.Exec(q, `UPDATE devices SET pending_operation=NULL,enabled=$3,secret_digest=COALESCE($4,secret_digest),requires_secret_reset=CASE WHEN $2='reset' THEN $4::bytea IS NULL ELSE requires_secret_reset END,updated_at=GREATEST(updated_at,$5) WHERE id=$1 AND pending_operation=$2 AND deleted_at IS NULL`, id, operation, enabled, digestBytes, at)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return device.ErrPendingOrNotFound
	}
	return tx.Commit(q)
}
func (s *PostgresStore) PendingOperations(ctx context.Context) ([]device.PendingOperation, error) {
	q, c := bounded(ctx)
	defer c()
	rows, err := s.pool.Query(q, `SELECT id,pending_operation FROM devices WHERE pending_operation IS NOT NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []device.PendingOperation{}
	for rows.Next() {
		var p device.PendingOperation
		if err = rows.Scan(&p.ID, &p.Operation); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
