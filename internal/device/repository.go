package device

import (
	"context"
	"errors"
	"time"
)

var (
	ErrExists              = errors.New("设备已存在")
	ErrNotFound            = errors.New("设备不存在")
	ErrIDDeleted           = errors.New("设备编号曾被删除，不能复用")
	ErrPendingOrNotFound   = errors.New("设备不存在或有待恢复的 Broker 操作")
	ErrSecretResetRequired = errors.New("设备密钥状态不确定，必须先重新重置密钥")
)

type Repository interface {
	CheckCreate(ctx context.Context, id string) error
	Create(ctx context.Context, d Device, secretDigest SecretDigest) error
	Get(ctx context.Context, id string) (Device, error)
	List(ctx context.Context) ([]Device, error)
	UpdateName(ctx context.Context, id, name string, updatedAt time.Time) error
	SetEnabled(ctx context.Context, id string, enabled bool, updatedAt time.Time) error
	ResetSecret(ctx context.Context, id string, secretDigest SecretDigest, updatedAt time.Time) error
	Delete(ctx context.Context, id string) error
}

// DurableLifecycle 将跨 Broker 操作的意图先提交到数据库。待处理期间设备不能接收遥测。
type DurableLifecycle interface {
	BeginCreate(ctx context.Context, d Device, digest SecretDigest) error
	BeginOperation(ctx context.Context, id, operation string, at time.Time) error
	FinishOperation(ctx context.Context, id, operation string, digest *SecretDigest, enabled bool, at time.Time) error
	PendingOperations(ctx context.Context) ([]PendingOperation, error)
}

type PendingOperation struct {
	ID        string
	Operation string
}
