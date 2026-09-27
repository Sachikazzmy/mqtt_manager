package device

import "time"

type Device struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type MetricValue struct {
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type TelemetryMessage struct {
	Version   string                 `json:"version"`
	DeviceID  string                 `json:"device_id"`
	MessageID string                 `json:"message_id"`
	SampledAt time.Time              `json:"sampled_at"`
	Metrics   map[string]MetricValue `json:"metrics"`
}
