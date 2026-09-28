package telemetry

import (
	"context"
	"errors"
)

var (
	ErrDuplicateMessage = errors.New("遥测消息已接收")
	ErrInvalidSecret    = errors.New("设备密钥无效")
)

type Repository interface {
	// Commit 必须在同一存储操作中完成设备状态校验、认证、去重、历史保存和 Latest 更新。
	Commit(ctx context.Context, deviceID, secret string, sample Sample) error
	History(ctx context.Context, query HistoryQuery) ([]Sample, error)
}
