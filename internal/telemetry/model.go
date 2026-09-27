package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type MetricValue struct {
	Value        float64 `json:"value"`
	Unit         string  `json:"unit"`
	valuePresent bool
	unitPresent  bool
}

func (m *MetricValue) UnmarshalJSON(data []byte) error {
	var value struct {
		Value *float64 `json:"value"`
		Unit  *string  `json:"unit"`
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("解析指标失败：%w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("解析指标失败：只能包含一个 JSON 对象")
	}

	m.valuePresent = value.Value != nil
	m.unitPresent = value.Unit != nil
	if value.Value != nil {
		m.Value = *value.Value
	}
	if value.Unit != nil {
		m.Unit = *value.Unit
	}
	return nil
}

type Message struct {
	Version   string                 `json:"version"`
	DeviceID  string                 `json:"device_id"`
	MessageID string                 `json:"message_id"`
	SampledAt time.Time              `json:"sampled_at"`
	Metrics   map[string]MetricValue `json:"metrics"`
}

type Sample struct {
	DeviceID   string                 `json:"device_id"`
	MessageID  string                 `json:"message_id"`
	SampledAt  time.Time              `json:"sampled_at"`
	ReceivedAt time.Time              `json:"received_at"`
	Metrics    map[string]MetricValue `json:"metrics"`
}

type LatestStatus struct {
	DeviceID       string                 `json:"device_id"`
	MessageID      string                 `json:"message_id"`
	SampledAt      time.Time              `json:"sampled_at"`
	ReceivedAt     time.Time              `json:"received_at"`
	LastReceivedAt time.Time              `json:"last_received_at"`
	Metrics        map[string]MetricValue `json:"metrics"`
}
