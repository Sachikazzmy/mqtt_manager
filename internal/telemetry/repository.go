package telemetry

import (
	"context"
	"errors"
)

var (
	ErrDuplicateMessage = errors.New("遥测消息已接收")
)

type Repository interface {
	// Store 必须原子完成去重、历史保存和最新状态更新。
	Store(ctx context.Context, sample Sample) error
}
