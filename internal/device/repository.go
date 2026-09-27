package device

import (
	"context"
	"errors"
	"time"
)

var (
	ErrExists   = errors.New("设备已存在")
	ErrNotFound = errors.New("设备不存在")
)

type Repository interface {
	Create(ctx context.Context, d Device) error
	Get(ctx context.Context, id string) (Device, error)
	List(ctx context.Context) ([]Device, error)
	UpdateName(ctx context.Context, id, name string, updatedAt time.Time) error
	SetEnabled(ctx context.Context, id string, enabled bool, updatedAt time.Time) error
	Delete(ctx context.Context, id string) error
}
