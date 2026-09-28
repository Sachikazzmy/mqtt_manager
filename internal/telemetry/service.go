package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

const (
	MaxPayloadBytes = 64 * 1024

	supportedVersion   = "1"
	maxDeviceIDBytes   = 64
	maxMessageIDBytes  = 128
	maxMetricNameBytes = 64
	maxMetricUnitBytes = 32
	maxMetricCount     = 64

	DefaultMaxFutureSkew = 5 * time.Minute
	MaxHistoryLimit      = 1000
	DefaultHistoryWindow = 24 * time.Hour
	MaxHistoryWindow     = 7 * 24 * time.Hour
)

var (
	ErrPayloadTooLarge = errors.New("遥测消息超过大小限制")
	ErrInvalidMessage  = errors.New("遥测消息格式无效")
	ErrDeviceMismatch  = errors.New("入口设备编号与消息设备编号不一致")
	ErrDeviceDisabled  = errors.New("设备已禁用")
	ErrHistoryLimit    = errors.New("历史数量限制无效")
	ErrHistoryWindow   = errors.New("历史查询时间范围无效")
)

type MetricRule struct {
	Unit string
	Min  *float64
	Max  *float64
}

type Config struct {
	MaxFutureSkew time.Duration
	MetricRules   map[string]MetricRule
}

type Service struct {
	repo          Repository
	now           func() time.Time
	maxFutureSkew time.Duration
	metricRules   map[string]MetricRule
}

func NewService(repo Repository) *Service {
	return NewServiceWithConfig(repo, Config{
		MaxFutureSkew: DefaultMaxFutureSkew,
		MetricRules:   DefaultMetricRules(),
	})
}

func NewServiceWithConfig(repo Repository, config Config) *Service {
	if config.MaxFutureSkew < 0 {
		config.MaxFutureSkew = DefaultMaxFutureSkew
	}
	if config.MetricRules == nil {
		config.MetricRules = DefaultMetricRules()
	}
	rules := make(map[string]MetricRule, len(config.MetricRules))
	for name, rule := range config.MetricRules {
		rules[name] = rule
	}
	return &Service{
		repo:          repo,
		now:           time.Now,
		maxFutureSkew: config.MaxFutureSkew,
		metricRules:   rules,
	}
}

// NewServiceWithClock 为本地模拟和测试注入固定时钟；生产代码通常使用 NewService。
func NewServiceWithClock(repo Repository, now func() time.Time, config Config) *Service {
	service := NewServiceWithConfig(repo, config)
	service.now = now
	return service
}

func DefaultMetricRules() map[string]MetricRule {
	return map[string]MetricRule{
		"temperature": {Unit: "C"},
		"pressure":    {Unit: "kPa"},
		"current":     {Unit: "A"},
	}
}

func parseMessage(payload []byte) (Message, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()

	var msg Message
	if err := decoder.Decode(&msg); err != nil {
		return Message{}, fmt.Errorf("%w：JSON 解析失败：%v", ErrInvalidMessage, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Message{}, fmt.Errorf("%w：JSON 只能包含一个对象", ErrInvalidMessage)
	}
	return msg, nil
}

func normalizeMessage(msg *Message) error {
	msg.Version = strings.TrimSpace(msg.Version)
	msg.DeviceID = strings.TrimSpace(msg.DeviceID)
	msg.MessageID = strings.TrimSpace(msg.MessageID)
	msg.SampledAt = msg.SampledAt.UTC()
	if msg.Metrics == nil {
		return nil
	}

	metrics := make(map[string]MetricValue, len(msg.Metrics))
	for name, metric := range msg.Metrics {
		name = strings.TrimSpace(name)
		if _, exists := metrics[name]; exists {
			return fmt.Errorf("%w：metric 名称规范化后重复", ErrInvalidMessage)
		}
		metric.Unit = strings.TrimSpace(metric.Unit)
		metrics[name] = metric
	}
	msg.Metrics = metrics
	return nil
}

func (s *Service) validateMessage(msg Message, now time.Time) error {
	if msg.Version != supportedVersion {
		return fmt.Errorf("%w：version 必须为 %q", ErrInvalidMessage, supportedVersion)
	}
	if err := validateID("payload.device_id", msg.DeviceID); err != nil {
		return err
	}
	if msg.MessageID == "" {
		return fmt.Errorf("%w：message_id 不能为空", ErrInvalidMessage)
	}
	if len(msg.MessageID) > maxMessageIDBytes {
		return fmt.Errorf("%w：message_id 不能超过 %d 字节", ErrInvalidMessage, maxMessageIDBytes)
	}
	if msg.SampledAt.IsZero() {
		return fmt.Errorf("%w：sampled_at 不能为空或无效", ErrInvalidMessage)
	}
	if msg.SampledAt.After(now.Add(s.maxFutureSkew)) {
		return fmt.Errorf("%w：sampled_at 不能明显晚于当前时间", ErrInvalidMessage)
	}
	if len(msg.Metrics) == 0 {
		return fmt.Errorf("%w：metrics 不能为空", ErrInvalidMessage)
	}
	if len(msg.Metrics) > maxMetricCount {
		return fmt.Errorf("%w：metrics 不能超过 %d 项", ErrInvalidMessage, maxMetricCount)
	}

	for name, metric := range msg.Metrics {
		if name == "" {
			return fmt.Errorf("%w：metric 名称不能为空", ErrInvalidMessage)
		}
		if len(name) > maxMetricNameBytes {
			return fmt.Errorf("%w：metric 名称不能超过 %d 字节", ErrInvalidMessage, maxMetricNameBytes)
		}
		rule, ok := s.metricRules[name]
		if !ok {
			return fmt.Errorf("%w：不支持的 metric %q", ErrInvalidMessage, name)
		}
		if !metric.valuePresent {
			return fmt.Errorf("%w：metric %q 缺少 value", ErrInvalidMessage, name)
		}
		if !metric.unitPresent || metric.Unit == "" {
			return fmt.Errorf("%w：metric %q 的 unit 不能为空", ErrInvalidMessage, name)
		}
		if len(metric.Unit) > maxMetricUnitBytes {
			return fmt.Errorf("%w：metric %q 的单位不能超过 %d 字节", ErrInvalidMessage, name, maxMetricUnitBytes)
		}
		if metric.Unit != rule.Unit {
			return fmt.Errorf("%w：metric %q 的 unit 必须为 %q", ErrInvalidMessage, name, rule.Unit)
		}
		if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
			return fmt.Errorf("%w：metric %q 的数值必须是有限数字", ErrInvalidMessage, name)
		}
		if rule.Min != nil && metric.Value < *rule.Min {
			return fmt.Errorf("%w：metric %q 小于配置的最小值", ErrInvalidMessage, name)
		}
		if rule.Max != nil && metric.Value > *rule.Max {
			return fmt.Errorf("%w：metric %q 大于配置的最大值", ErrInvalidMessage, name)
		}
	}

	return nil
}

func validateID(field, id string) error {
	if id == "" {
		return fmt.Errorf("%w：%s 不能为空", ErrInvalidMessage, field)
	}
	if len(id) > maxDeviceIDBytes {
		return fmt.Errorf("%w：%s 不能超过 %d 字节", ErrInvalidMessage, field, maxDeviceIDBytes)
	}
	return nil
}

func (s *Service) Receive(ctx context.Context, deviceID, secret string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) > MaxPayloadBytes {
		return fmt.Errorf("%w：%d 字节，最大允许 %d 字节", ErrPayloadTooLarge, len(payload), MaxPayloadBytes)
	}

	deviceID = strings.TrimSpace(deviceID)
	if err := validateID("入口设备编号", deviceID); err != nil {
		return err
	}

	msg, err := parseMessage(payload)
	if err != nil {
		return err
	}
	if err := normalizeMessage(&msg); err != nil {
		return err
	}
	if msg.DeviceID != deviceID {
		return fmt.Errorf("%w：入口=%q payload=%q", ErrDeviceMismatch, deviceID, msg.DeviceID)
	}

	receivedAt := s.now().UTC()
	if err := s.validateMessage(msg, receivedAt); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	sample := Sample{
		DeviceID:   msg.DeviceID,
		MessageID:  msg.MessageID,
		SampledAt:  msg.SampledAt,
		ReceivedAt: receivedAt,
		Metrics:    cloneMetrics(msg.Metrics),
	}
	if err := s.repo.Commit(ctx, deviceID, secret, sample); err != nil {
		return fmt.Errorf("保存遥测消息失败：%w", err)
	}

	return nil
}

func (s *Service) History(ctx context.Context, query HistoryQuery) ([]Sample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.DeviceID != "" {
		query.DeviceID = strings.TrimSpace(query.DeviceID)
		if err := validateID("查询设备编号", query.DeviceID); err != nil {
			return nil, err
		}
	}
	if query.Limit <= 0 {
		return nil, fmt.Errorf("%w：必须大于 0", ErrHistoryLimit)
	}
	if query.Limit > MaxHistoryLimit {
		return nil, fmt.Errorf("%w：最大允许 %d", ErrHistoryLimit, MaxHistoryLimit)
	}

	now := s.now().UTC()
	var from, to time.Time
	if query.From != nil {
		from = query.From.UTC()
	} else if query.To != nil {
		to = query.To.UTC()
		from = to.Add(-DefaultHistoryWindow)
	} else {
		to = now
		from = now.Add(-DefaultHistoryWindow)
	}
	if query.To != nil {
		to = query.To.UTC()
	} else if query.From != nil {
		to = now
	}
	if from.After(to) {
		return nil, fmt.Errorf("%w：起始时间不能晚于结束时间", ErrHistoryWindow)
	}
	if to.Sub(from) > MaxHistoryWindow {
		return nil, fmt.Errorf("%w：最大允许 %s", ErrHistoryWindow, MaxHistoryWindow)
	}
	query.From = &from
	query.To = &to
	return s.repo.History(ctx, query)
}

func cloneMetrics(metrics map[string]MetricValue) map[string]MetricValue {
	if metrics == nil {
		return nil
	}
	cloned := make(map[string]MetricValue, len(metrics))
	for name, metric := range metrics {
		cloned[name] = metric
	}
	return cloned
}
