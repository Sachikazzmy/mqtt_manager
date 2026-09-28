package device

import "time"

type MetricState struct {
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type LatestState struct {
	Metrics             map[string]MetricState `json:"metrics"`
	SampledAt           time.Time              `json:"sampled_at"`
	ReceivedAt          time.Time              `json:"received_at"`
	LastValidReceivedAt time.Time              `json:"last_valid_received_at"`
	MessageID           string                 `json:"message_id"`
}

type Device struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Enabled   bool         `json:"enabled"`
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
	Latest    *LatestState `json:"latest"`
}
