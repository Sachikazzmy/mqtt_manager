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

	"Project/internal/device"
)

const (
	MaxPayloadBytes = 64 * 1024

	supportedVersion   = "1"
	maxDeviceIDBytes   = 64
	maxMessageIDBytes  = 128
	maxMetricNameBytes = 64
	maxMetricUnitBytes = 32
	maxMetricCount     = 64
)

var (
	ErrPayloadTooLarge = errors.New("遥测消息超过大小限制")
	ErrInvalidMessage  = errors.New("遥测消息格式无效")
	ErrDeviceMismatch  = errors.New("Topic 设备编号与消息设备编号不一致")
	ErrDeviceDisabled  = errors.New("设备已禁用")
)

type Service struct {
	devices *device.Service
	repo    Repository
	now     func() time.Time
}

func NewService(devices *device.Service, repo Repository) *Service {
	return newServiceWithClock(devices, repo, time.Now)
}

func newServiceWithClock(devices *device.Service, repo Repository, now func() time.Time) *Service {
	return &Service{
		devices: devices,
		repo:    repo,
		now:     now,
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

func normalizeMessage(msg *Message) {
	msg.Version = strings.TrimSpace(msg.Version)
	msg.DeviceID = strings.TrimSpace(msg.DeviceID)
	msg.MessageID = strings.TrimSpace(msg.MessageID)
	msg.SampledAt = msg.SampledAt.UTC()
	for name, metric := range msg.Metrics {
		metric.Unit = strings.TrimSpace(metric.Unit)
		msg.Metrics[name] = metric
	}
}

func validateMessage(msg Message) error {
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
	if len(msg.Metrics) == 0 {
		return fmt.Errorf("%w：metrics 不能为空", ErrInvalidMessage)
	}
	if len(msg.Metrics) > maxMetricCount {
		return fmt.Errorf("%w：metrics 不能超过 %d 项", ErrInvalidMessage, maxMetricCount)
	}

	for name, metric := range msg.Metrics {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("%w：metric 名称不能为空", ErrInvalidMessage)
		}
		if len(name) > maxMetricNameBytes {
			return fmt.Errorf("%w：metric 名称不能超过 %d 字节", ErrInvalidMessage, maxMetricNameBytes)
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
		if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
			return fmt.Errorf("%w：metric %q 的数值必须是有限数字", ErrInvalidMessage, name)
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

func (s *Service) Receive(ctx context.Context, topicDeviceID string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) > MaxPayloadBytes {
		return fmt.Errorf("%w：%d 字节，最大允许 %d 字节", ErrPayloadTooLarge, len(payload), MaxPayloadBytes)
	}

	topicDeviceID = strings.TrimSpace(topicDeviceID)
	if err := validateID("Topic 设备编号", topicDeviceID); err != nil {
		return err
	}

	msg, err := parseMessage(payload)
	if err != nil {
		return err
	}
	normalizeMessage(&msg)

	if msg.DeviceID != topicDeviceID {
		return fmt.Errorf("%w：topic=%q payload=%q", ErrDeviceMismatch, topicDeviceID, msg.DeviceID)
	}
	if err := validateMessage(msg); err != nil {
		return err
	}

	d, err := s.devices.Get(ctx, msg.DeviceID)
	if err != nil {
		return err
	}
	if !d.Enabled {
		return fmt.Errorf("%w：%q", ErrDeviceDisabled, msg.DeviceID)
	}

	receivedAt := s.now().UTC()
	sample := Sample{
		DeviceID:   msg.DeviceID,
		MessageID:  msg.MessageID,
		SampledAt:  msg.SampledAt,
		ReceivedAt: receivedAt,
		Metrics:    cloneMetrics(msg.Metrics),
	}

	if err := s.repo.Store(ctx, sample); err != nil {
		return fmt.Errorf("保存遥测消息失败：%w", err)
	}

	return nil
}
