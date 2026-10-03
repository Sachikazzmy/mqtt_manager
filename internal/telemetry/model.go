package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type MetricValue struct {
	Value             float64 `json:"value"`
	Unit              string  `json:"unit"`
	Modifiable        bool    `json:"modifiable,omitempty"`
	valuePresent      bool
	unitPresent       bool
	modifiablePresent bool
}

// SetModifiable marks the protocol field as present when a producer builds a
// message in memory. Parsed legacy messages keep the field absent on marshal.
func (m *MetricValue) SetModifiable(value bool) {
	m.Modifiable = value
	m.modifiablePresent = true
}

func (m *MetricValue) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("解析指标失败：%w", err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return fmt.Errorf("解析指标失败：必须是 JSON 对象")
	}
	*m = MetricValue{}
	seen := make(map[string]struct{}, 3)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return fmt.Errorf("解析指标失败：%w", err)
		}
		name, ok := token.(string)
		if !ok {
			return fmt.Errorf("解析指标失败：字段名必须是字符串")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("解析指标失败：字段 %q 重复", name)
		}
		seen[name] = struct{}{}
		switch name {
		case "value":
			var value *float64
			if err = decoder.Decode(&value); err == nil && value != nil {
				m.Value = *value
				m.valuePresent = true
			}
		case "unit":
			var unit *string
			if err = decoder.Decode(&unit); err == nil && unit != nil {
				m.Unit = *unit
				m.unitPresent = true
			}
		case "modifiable":
			var modifiable *bool
			if err = decoder.Decode(&modifiable); err == nil && modifiable != nil {
				m.Modifiable = *modifiable
				m.modifiablePresent = true
			} else if err == nil {
				err = fmt.Errorf("必须是 JSON boolean，不能为 null")
			}
		default:
			return fmt.Errorf("解析指标失败：不支持字段 %q", name)
		}
		if err != nil {
			return fmt.Errorf("解析指标失败：字段 %q：%w", name, err)
		}
	}
	if _, err = decoder.Token(); err != nil {
		return fmt.Errorf("解析指标失败：对象不完整：%w", err)
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("解析指标失败：只能包含一个 JSON 对象")
	}
	return nil
}

// MarshalJSON preserves whether a legacy telemetry metric omitted modifiable.
// That distinction is part of the raw historical message and must not be
// filled in with a value invented by the server.
func (m MetricValue) MarshalJSON() ([]byte, error) {
	if m.modifiablePresent {
		return json.Marshal(struct {
			Value      float64 `json:"value"`
			Unit       string  `json:"unit"`
			Modifiable bool    `json:"modifiable"`
		}{Value: m.Value, Unit: m.Unit, Modifiable: m.Modifiable})
	}
	return json.Marshal(struct {
		Value float64 `json:"value"`
		Unit  string  `json:"unit"`
	}{Value: m.Value, Unit: m.Unit})
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

type HistoryQuery struct {
	DeviceID string
	From     *time.Time
	To       *time.Time
	Limit    int
}
