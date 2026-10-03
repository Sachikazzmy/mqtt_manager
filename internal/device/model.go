package device

import (
	"strconv"
	"strings"
	"time"
)

type MetricState struct {
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	Modifiable bool      `json:"modifiable"`
	SampledAt  time.Time `json:"sampled_at"`
	ReceivedAt time.Time `json:"received_at"`
	MessageID  string    `json:"message_id"`
}

type LatestState struct {
	Metrics map[string]MetricState `json:"metrics"`
}

// MetricDefinition 是设备上报某个 metric_key 时必须遵循的持久化约定。
// Unit 和 Key 创建后不可修改；要改变物理含义或单位，必须停用旧 key 并新增 key。
type MetricDefinition struct {
	Key          string   `json:"metric_key"`
	DisplayName  string   `json:"display_name"`
	Unit         string   `json:"unit"`
	MinValue     *float64 `json:"min_value,omitempty"`
	MaxValue     *float64 `json:"max_value,omitempty"`
	Enabled      bool     `json:"enabled"`
	DisplayOrder int      `json:"display_order"`
}

type Device struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// LastValidReceivedAt 是设备最后一条有效、非重复消息的服务端接收时间，不是在线状态。
	LastValidReceivedAt *time.Time         `json:"last_valid_received_at"`
	MetricDefinitions   []MetricDefinition `json:"-"`
	Latest              *LatestState       `json:"latest"`
}

const (
	MinMetricLimit     = 1
	DefaultMetricLimit = 10
	HardMaxMetricLimit = 100
)

// SegmentSlotIndex recognizes only canonical, one-based segment-N keys.
// The key namespace is independently bounded; device capacity counts bound keys.
func SegmentSlotIndex(key string) (int, bool) {
	if !strings.HasPrefix(key, "segment-") {
		return 0, false
	}
	text := strings.TrimPrefix(key, "segment-")
	index, err := strconv.Atoi(text)
	if err != nil || index < 1 || index > HardMaxMetricLimit || strconv.Itoa(index) != text {
		return 0, false
	}
	return index, true
}
