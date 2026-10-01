package telemetry

import (
	"context"
	"errors"
)

var (
	ErrDuplicateMessage = errors.New("遥测消息已接收")
	ErrInvalidSecret    = errors.New("设备密钥无效")
	// ErrDeviceTransition means a recoverable broker lifecycle operation is in
	// progress. MQTT must leave the message unacknowledged for a later redelivery.
	ErrDeviceTransition = errors.New("设备生命周期操作尚未完成")
)

type Repository interface {
	// Commit 必须在同一存储事务中完成设备状态和指标定义校验、认证、去重、原始历史保存及分项 Latest 更新。
	Commit(ctx context.Context, deviceID, secret string, sample Sample) error
	// CommitFromBroker 仅用于已由受信任 MQTT Broker 认证和授权的消息入口；其余校验和原子写入要求与 Commit 相同。
	CommitFromBroker(ctx context.Context, deviceID string, sample Sample) error
	History(ctx context.Context, query HistoryQuery) ([]Sample, error)
}
