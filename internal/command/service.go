package command

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"Project/internal/device"
)

const (
	DispatchBatchSize = 1
	MaxAttempts       = 5
	CommandLifetime   = 10 * time.Minute
	RetryBase         = 5 * time.Second
	RetryMaximum      = time.Minute
	DispatchInterval  = time.Second
	PublishTimeout    = 3 * time.Second // 短于数据库操作超时，为生命周期锁等待保留余量。
)

type deviceLookup interface {
	Get(context.Context, string) (device.Device, error)
}

type Service struct {
	devices deviceLookup
	repo    Repository
	now     func() time.Time
}

func NewService(devices deviceLookup, repo Repository) *Service {
	return &Service{devices: devices, repo: repo, now: time.Now}
}

func (s *Service) SetMetric(ctx context.Context, deviceID, metricKey string, value float64) (Command, error) {
	return s.SetMetricWithID(ctx, "", deviceID, metricKey, value)
}

// SetMetricWithID is also the idempotent retry entry point for callers that
// durably retained a command_id after a request timeout.
func (s *Service) SetMetricWithID(ctx context.Context, commandID, deviceID, metricKey string, value float64) (Command, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Command{}, fmt.Errorf("%w：value 必须是有限数字", ErrInvalidCommand)
	}
	return s.submit(ctx, Request{CommandID: commandID, DeviceID: deviceID, Action: ActionSetMetric, MetricKey: metricKey, Value: &value})
}

func (s *Service) ClearOverride(ctx context.Context, deviceID, metricKey string) (Command, error) {
	return s.ClearOverrideWithID(ctx, "", deviceID, metricKey)
}

func (s *Service) ClearOverrideWithID(ctx context.Context, commandID, deviceID, metricKey string) (Command, error) {
	return s.submit(ctx, Request{CommandID: commandID, DeviceID: deviceID, Action: ActionClearOverride, MetricKey: metricKey})
}

func (s *Service) submit(ctx context.Context, request Request) (Command, error) {
	if request.Action != ActionSetMetric && request.Action != ActionClearOverride {
		return Command{}, fmt.Errorf("%w：只允许 set_metric 和 clear_override", ErrInvalidCommand)
	}
	if !validMetricKey(request.MetricKey) {
		return Command{}, fmt.Errorf("%w：metric_key 无效", ErrInvalidCommand)
	}
	if request.Action == ActionSetMetric && (request.Value == nil || math.IsNaN(*request.Value) || math.IsInf(*request.Value, 0)) {
		return Command{}, fmt.Errorf("%w：set_metric 必须有有限 value", ErrInvalidCommand)
	}
	if request.Action == ActionClearOverride && request.Value != nil {
		return Command{}, fmt.Errorf("%w：clear_override 不携带 value", ErrInvalidCommand)
	}
	if request.CommandID != "" {
		if !validToken(request.CommandID, 128) {
			return Command{}, fmt.Errorf("%w：command_id 无效", ErrInvalidCommand)
		}
		existing, err := s.repo.GetCommand(ctx, request.DeviceID, request.CommandID)
		if err == nil {
			if sameContent(existing, request) {
				return existing, nil
			}
			return Command{}, ErrCommandConflict
		}
		if !errors.Is(err, ErrCommandNotFound) {
			return Command{}, err
		}
	} else {
		id, err := newCommandID()
		if err != nil {
			return Command{}, fmt.Errorf("生成 command_id 失败：%w", err)
		}
		request.CommandID = id
	}

	target, err := s.devices.Get(ctx, request.DeviceID)
	if err != nil {
		return Command{}, err
	}
	if !target.Enabled {
		return Command{}, ErrDeviceUnavailable
	}
	if target.Latest == nil {
		return Command{}, ErrMetricNotReady
	}
	state, exists := target.Latest.Metrics[request.MetricKey]
	if !exists {
		return Command{}, ErrMetricNotReady
	}
	if !state.Modifiable {
		return Command{}, ErrMetricNotMutable
	}
	for _, definition := range target.MetricDefinitions {
		if definition.Key != request.MetricKey {
			continue
		}
		if !definition.Enabled {
			return Command{}, ErrMetricNotMutable
		}
		if request.Value != nil && definition.MinValue != nil && *request.Value < *definition.MinValue {
			return Command{}, fmt.Errorf("%w：value 低于指标已配置的最小值", ErrInvalidCommand)
		}
		if request.Value != nil && definition.MaxValue != nil && *request.Value > *definition.MaxValue {
			return Command{}, fmt.Errorf("%w：value 高于指标已配置的最大值", ErrInvalidCommand)
		}
		break
	}

	now := s.now().UTC().Truncate(time.Microsecond)
	entry := Command{
		DeviceID: request.DeviceID, CommandID: request.CommandID, Action: request.Action,
		MetricKey: request.MetricKey, Value: cloneFloat(request.Value), Status: StatusWaitingToSend,
		CreatedAt: now, NextAttemptAt: now, DeadlineAt: now.Add(CommandLifetime),
	}
	created, err := s.repo.CreateCommand(ctx, entry)
	if err != nil {
		return Command{}, err
	}
	return created, nil
}

func sameContent(existing Command, request Request) bool {
	if existing.Action != request.Action || existing.MetricKey != request.MetricKey {
		return false
	}
	if existing.Value == nil || request.Value == nil {
		return existing.Value == nil && request.Value == nil
	}
	return *existing.Value == *request.Value
}

func newCommandID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func (s *Service) Get(ctx context.Context, deviceID, commandID string) (Command, error) {
	return s.repo.GetCommand(ctx, deviceID, commandID)
}

func (s *Service) History(ctx context.Context, deviceID string, limit int) ([]Command, error) {
	if limit <= 0 || limit > MaxHistoryLimit {
		return nil, fmt.Errorf("%w：数量必须是 1 到 %d", ErrCommandLimit, MaxHistoryLimit)
	}
	return s.repo.CommandHistory(ctx, deviceID, limit)
}

func (s *Service) ProcessResultFromBroker(ctx context.Context, topic string, payload []byte) (Disposition, error) {
	deviceID, err := deviceIDFromResultTopic(topic)
	if err != nil {
		return Disposition{}, err
	}
	result, err := ParseResult(payload)
	if err != nil {
		return Disposition{}, err
	}
	return s.repo.ProcessResult(ctx, deviceID, result, append([]byte(nil), payload...), s.now().UTC().Truncate(time.Microsecond))
}

func deviceIDFromResultTopic(topic string) (string, error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "factory" || parts[2] != ResultTopicSuffix || !validDeviceID(parts[1]) {
		return "", fmt.Errorf("%w：只接受 factory/{device_id}/command_result", ErrInvalidResult)
	}
	return parts[1], nil
}

func validDeviceID(value string) bool {
	if value == "" || len(value) > 64 || value == "admin" {
		return false
	}
	for index, char := range value {
		allowed := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-'
		if !allowed || index == 0 && !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func (s *Service) Run(ctx context.Context, publisher Publisher) {
	ticker := time.NewTicker(DispatchInterval)
	defer ticker.Stop()
	for {
		s.dispatchBatch(ctx, publisher)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) dispatchBatch(ctx context.Context, publisher Publisher) {
	if err := ctx.Err(); err != nil {
		return
	}
	now := s.now().UTC().Truncate(time.Microsecond)
	if err := s.repo.Expire(ctx, now, MaxAttempts); err != nil {
		log.Printf("命令超时状态暂未更新，将稍后重试: %v", err)
		return
	}
	if err := s.repo.DispatchDue(ctx, now, DispatchBatchSize, MaxAttempts, RetryBase, RetryMaximum, func(publishCtx context.Context, entry Command) error {
		payload, marshalErr := json.Marshal(commandPayload(entry))
		if marshalErr != nil {
			return fmt.Errorf("编码命令失败")
		}
		boundedPublishCtx, cancel := context.WithTimeout(publishCtx, PublishTimeout)
		defer cancel()
		return publisher.PublishCommand(boundedPublishCtx, entry.DeviceID, payload)
	}); err != nil {
		log.Printf("命令 outbox 暂时无法处理，将稍后重试: %v", err)
	}
}
