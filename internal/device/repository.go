package device

import (
	"context"
	"errors"
)

var (
	ErrExists   = errors.New("设备已存在")
	ErrNotFound = errors.New("设备不存在")
)

type Repository interface {
	Create(ctx context.Context, d Device) error
	Get(ctx context.Context, id string) (Device, error)
	List(ctx context.Context) ([]Device, error)
	Update(ctx context.Context, d Device) error
	Delete(ctx context.Context, id string) error
}
