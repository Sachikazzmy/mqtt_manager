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
	"unicode/utf8"

	"Project/internal/device"
)

const (
	MaxPayloadBytes = 64 * 1024

	supportedVersion   = "1"
	maxDeviceIDBytes   = 64
	maxMessageIDBytes  = 128
	maxMetricNameBytes = 64
	maxMetricUnitBytes = 32
	maxMetricCount     = device.HardMaxMetricLimit

	DefaultMaxFutureSkew = 5 * time.Minute
	MaxHistoryLimit      = 1000
	DefaultHistoryWindow = 24 * time.Hour
	MaxHistoryWindow     = 7 * 24 * time.Hour
)

var (
	ErrPayloadTooLarge = errors.New("遥测消息超过大小限制")
	ErrInvalidMessage  = errors.New("遥测消息格式无效")
	ErrInvalidTopic    = errors.New("遥测 Topic 无效")
	ErrDeviceMismatch  = errors.New("入口设备编号与消息设备编号不一致")
	ErrDeviceDisabled  = errors.New("设备已禁用")
	ErrHistoryLimit    = errors.New("历史数量限制无效")
	ErrHistoryWindow   = errors.New("历史查询时间范围无效")
)

type Config struct {
	MaxFutureSkew time.Duration
}

type Service struct {
	repo          Repository
	now           func() time.Time
	maxFutureSkew time.Duration
}

func NewService(repo Repository) *Service {
	return NewServiceWithConfig(repo, Config{
		MaxFutureSkew: DefaultMaxFutureSkew,
	})
}

func NewServiceWithConfig(repo Repository, config Config) *Service {
	if config.MaxFutureSkew < 0 {
		config.MaxFutureSkew = DefaultMaxFutureSkew
	}
	return &Service{
		repo:          repo,
		now:           time.Now,
		maxFutureSkew: config.MaxFutureSkew,
	}
}

// NewServiceWithClock 为本地模拟和测试注入固定时钟；生产代码通常使用 NewService。
func NewServiceWithClock(repo Repository, now func() time.Time, config Config) *Service {
	service := NewServiceWithConfig(repo, config)
	service.now = now
	return service
}

func parseMessage(payload []byte) (Message, error) {
	if !utf8.Valid(payload) {
		return Message{}, fmt.Errorf("%w：JSON 载荷必须是有效 UTF-8", ErrInvalidMessage)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil {
		return Message{}, fmt.Errorf("%w：JSON 解析失败：%v", ErrInvalidMessage, err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return Message{}, fmt.Errorf("%w：顶层必须是 JSON 对象", ErrInvalidMessage)
	}
	var envelope Message
	var rawMetrics json.RawMessage
	seen := make(map[string]struct{}, 5)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return Message{}, fmt.Errorf("%w：JSON 字段解析失败：%v", ErrInvalidMessage, err)
		}
		name, ok := token.(string)
		if !ok {
			return Message{}, fmt.Errorf("%w：顶层字段名必须是字符串", ErrInvalidMessage)
		}
		if _, duplicate := seen[name]; duplicate {
			return Message{}, fmt.Errorf("%w：顶层字段 %q 重复", ErrInvalidMessage, name)
		}
		seen[name] = struct{}{}
		switch name {
		case "version":
			err = decoder.Decode(&envelope.Version)
		case "device_id":
			err = decoder.Decode(&envelope.DeviceID)
		case "message_id":
			err = decoder.Decode(&envelope.MessageID)
		case "sampled_at":
			err = decoder.Decode(&envelope.SampledAt)
		case "metrics":
			err = decoder.Decode(&rawMetrics)
		default:
			return Message{}, fmt.Errorf("%w：不支持顶层字段 %q", ErrInvalidMessage, name)
		}
		if err != nil {
			return Message{}, fmt.Errorf("%w：字段 %q 解析失败：%v", ErrInvalidMessage, name, err)
		}
	}
	if _, err = decoder.Token(); err != nil {
		return Message{}, fmt.Errorf("%w：JSON 对象不完整：%v", ErrInvalidMessage, err)
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Message{}, fmt.Errorf("%w：JSON 只能包含一个对象", ErrInvalidMessage)
	}
	metrics, err := parseMetrics(rawMetrics)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Version: envelope.Version, DeviceID: envelope.DeviceID, MessageID: envelope.MessageID,
		SampledAt: envelope.SampledAt, Metrics: metrics,
	}, nil
}

func parseMetrics(raw json.RawMessage) (map[string]MetricValue, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w：缺少 metrics 对象", ErrInvalidMessage)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("%w：metrics 解析失败：%v", ErrInvalidMessage, err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("%w：metrics 必须是 JSON 对象", ErrInvalidMessage)
	}
	metrics := make(map[string]MetricValue)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("%w：metrics 解析失败：%v", ErrInvalidMessage, err)
		}
		name, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("%w：metric key 必须是字符串", ErrInvalidMessage)
		}
		if _, exists := metrics[name]; exists {
			return nil, fmt.Errorf("%w：metrics 中 metric_key %q 重复", ErrInvalidMessage, name)
		}
		var metric MetricValue
		if err := decoder.Decode(&metric); err != nil {
			return nil, fmt.Errorf("%w：metric %q 解析失败：%v", ErrInvalidMessage, name, err)
		}
		metrics[name] = metric
	}
	if _, err = decoder.Token(); err != nil {
		return nil, fmt.Errorf("%w：metrics 对象不完整：%v", ErrInvalidMessage, err)
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w：metrics 只能包含一个 JSON 对象", ErrInvalidMessage)
	}
	return metrics, nil
}

func normalizeMessage(msg *Message) error {
	if msg.MessageID != strings.TrimSpace(msg.MessageID) {
		return fmt.Errorf("%w：message_id 不得包含首尾空白", ErrInvalidMessage)
	}
	msg.SampledAt = msg.SampledAt.UTC()
	if msg.Metrics == nil {
		return nil
	}

	metrics := make(map[string]MetricValue, len(msg.Metrics))
	for name, metric := range msg.Metrics {
		if _, exists := metrics[name]; exists {
			return fmt.Errorf("%w：metric 名称规范化后重复", ErrInvalidMessage)
		}
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
	if strings.ContainsRune(msg.MessageID, '\x00') {
		return fmt.Errorf("%w：message_id 不能包含 NUL 字符", ErrInvalidMessage)
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
		if err := validateMetricKey(name); err != nil {
			return err
		}
		if !metric.valuePresent {
			return fmt.Errorf("%w：metric %q 缺少 value", ErrInvalidMessage, name)
		}
		if !metric.unitPresent || strings.TrimSpace(metric.Unit) == "" {
			return fmt.Errorf("%w：metric %q 的 unit 不能为空", ErrInvalidMessage, name)
		}
		if metric.Unit != strings.TrimSpace(metric.Unit) || strings.ContainsRune(metric.Unit, '\x00') {
			return fmt.Errorf("%w：metric %q 的 unit 不得含首尾空白或 NUL", ErrInvalidMessage, name)
		}
		if len(metric.Unit) > maxMetricUnitBytes {
			return fmt.Errorf("%w：metric %q 的单位不能超过 %d 字节", ErrInvalidMessage, name, maxMetricUnitBytes)
		}
		if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
			return fmt.Errorf("%w：metric %q 的数值必须是有限数字", ErrInvalidMessage, name)
		}
	}

	return nil
}

func validateMetricKey(key string) error {
	if key == "" || len(key) > maxMetricNameBytes {
		return fmt.Errorf("%w：metric_key 必须为 1 到 %d 字节", ErrInvalidMessage, maxMetricNameBytes)
	}
	for index, value := range key {
		allowed := value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '-'
		if !allowed || index == 0 && !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9') {
			return fmt.Errorf("%w：metric_key 只允许以字母或数字开头的 ASCII 字母、数字、下划线和连字符", ErrInvalidMessage)
		}
	}
	return nil
}

// ResolveMetricsAgainstDefinitions 应在仓储锁定设备配置后调用。segment-N 首次合法上报时
// 自动建立定义并绑定单位；返回值只比输入多出本次新建的 segment 定义。
func ResolveMetricsAgainstDefinitions(metrics map[string]MetricValue, definitions []device.MetricDefinition, definitionLimit int) ([]device.MetricDefinition, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("%w：metrics 不能为空", ErrInvalidMessage)
	}
	if err := device.ValidateMetricLimit(definitionLimit); err != nil {
		return nil, fmt.Errorf("%w：设备指标总数上限无效", ErrInvalidMessage)
	}
	if len(metrics) > maxMetricCount {
		return nil, fmt.Errorf("%w：metrics 不能超过 %d 项", ErrInvalidMessage, maxMetricCount)
	}
	byKey := make(map[string]device.MetricDefinition, len(definitions))
	definitionOrder := append([]device.MetricDefinition(nil), definitions...)
	for _, definition := range definitions {
		byKey[definition.Key] = definition
	}
	newDefinitions := make([]device.MetricDefinition, 0)
	for key, metric := range metrics {
		definition, ok := byKey[key]
		if !ok {
			slot, isSegment := device.SegmentSlotIndex(key)
			if !isSegment || slot > device.HardMaxMetricLimit {
				return nil, fmt.Errorf("%w：metric_key %q 不属于已登记指标或可用 segment 槽位", ErrInvalidMessage, key)
			}
			if len(definitionOrder)+len(newDefinitions) >= definitionLimit {
				return nil, fmt.Errorf("%w：设备指标数量达到配置上限 %d", ErrInvalidMessage, definitionLimit)
			}
			definition = device.MetricDefinition{
				Key: key, DisplayName: key, Unit: metric.Unit, Enabled: true,
				DisplayOrder: slot,
			}
			newDefinitions = append(newDefinitions, definition)
			byKey[key] = definition
		}
		if !definition.Enabled {
			return nil, fmt.Errorf("%w：metric_key %q 已停用", ErrInvalidMessage, key)
		}
		if metric.Unit != definition.Unit {
			return nil, fmt.Errorf("%w：metric %q 的 unit 必须为 %q", ErrInvalidMessage, key, definition.Unit)
		}
		if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
			return nil, fmt.Errorf("%w：metric %q 的数值必须是有限数字", ErrInvalidMessage, key)
		}
		if definition.MinValue != nil && metric.Value < *definition.MinValue {
			return nil, fmt.Errorf("%w：metric %q 小于配置的最小值", ErrInvalidMessage, key)
		}
		if definition.MaxValue != nil && metric.Value > *definition.MaxValue {
			return nil, fmt.Errorf("%w：metric %q 大于配置的最大值", ErrInvalidMessage, key)
		}
	}
	// 已登记且启用的定义，加上总定义上限内尚未创建的 segment，可组成可上报子集。
	capacity := 0
	for _, definition := range definitionOrder {
		if definition.Enabled {
			capacity++
		}
	}
	capacity += len(newDefinitions)
	capacity += max(0, definitionLimit-len(definitionOrder)-len(newDefinitions))
	capacity = min(capacity, definitionLimit)
	if len(metrics) > capacity {
		return nil, fmt.Errorf("%w：本条指标数量超过该设备已启用定义数", ErrInvalidMessage)
	}
	return append(definitionOrder, newDefinitions...), nil
}

func validateID(field, id string) error {
	if id == "" {
		return fmt.Errorf("%w：%s 不能为空", ErrInvalidMessage, field)
	}
	if id == "admin" {
		return fmt.Errorf("%w：%s 不能使用 Broker 管理账户编号", ErrInvalidMessage, field)
	}
	if len(id) > maxDeviceIDBytes {
		return fmt.Errorf("%w：%s 不能超过 %d 字节", ErrInvalidMessage, field, maxDeviceIDBytes)
	}
	for index, value := range id {
		allowed := value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '-'
		if !allowed || index == 0 && !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9') {
			return fmt.Errorf("%w：%s 包含不允许的字符", ErrInvalidMessage, field)
		}
	}
	return nil
}

func (s *Service) Receive(ctx context.Context, deviceID, secret string, payload []byte) error {
	_, err := s.receive(ctx, deviceID, secret, payload, false)
	return err
}

type ReceiveResult struct {
	DeviceID  string                 `json:"device_id"`
	MessageID string                 `json:"message_id"`
	Metrics   map[string]MetricValue `json:"metrics"`
}

func (s *Service) ReceiveFromBroker(ctx context.Context, topic string, payload []byte) (ReceiveResult, error) {
	deviceID, err := deviceIDFromTopic(topic)
	if err != nil {
		return ReceiveResult{}, err
	}
	return s.receive(ctx, deviceID, "", payload, true)
}

func deviceIDFromTopic(topic string) (string, error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "factory" || parts[2] != "telemetry" {
		return "", fmt.Errorf("%w：只接受 factory/{device_id}/telemetry", ErrInvalidTopic)
	}
	if err := validateID("Topic 设备编号", parts[1]); err != nil {
		return "", fmt.Errorf("%w：%v", ErrInvalidTopic, err)
	}
	return parts[1], nil
}

func (s *Service) receive(ctx context.Context, deviceID, secret string, payload []byte, brokerAuthenticated bool) (ReceiveResult, error) {
	var result ReceiveResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(payload) > MaxPayloadBytes {
		return result, fmt.Errorf("%w：%d 字节，最大允许 %d 字节", ErrPayloadTooLarge, len(payload), MaxPayloadBytes)
	}

	if err := validateID("入口设备编号", deviceID); err != nil {
		return result, err
	}

	msg, err := parseMessage(payload)
	if err != nil {
		return result, err
	}
	if err := normalizeMessage(&msg); err != nil {
		return result, err
	}
	result = ReceiveResult{DeviceID: msg.DeviceID, MessageID: msg.MessageID, Metrics: cloneMetrics(msg.Metrics)}
	if msg.DeviceID != deviceID {
		return result, fmt.Errorf("%w：入口=%q payload=%q", ErrDeviceMismatch, deviceID, msg.DeviceID)
	}

	receivedAt := s.now().UTC().Truncate(time.Microsecond)
	if err := s.validateMessage(msg, receivedAt); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	sample := Sample{
		DeviceID:   msg.DeviceID,
		MessageID:  msg.MessageID,
		SampledAt:  msg.SampledAt.Truncate(time.Microsecond),
		ReceivedAt: receivedAt,
		Metrics:    cloneMetrics(msg.Metrics),
	}
	if brokerAuthenticated {
		err = s.repo.CommitFromBroker(ctx, deviceID, sample)
	} else {
		err = s.repo.Commit(ctx, deviceID, secret, sample)
	}
	if err != nil {
		return result, fmt.Errorf("保存遥测消息失败：%w", err)
	}

	return result, nil
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
