package storage

import (
	"container/heap"
	"context"
	"crypto/hmac"
	"sort"
	"sync"
	"time"

	"Project/internal/device"
	"Project/internal/telemetry"
)

type deviceRecord struct {
	config       device.Device
	secretDigest device.SecretDigest
}

type messageKey struct {
	deviceID  string
	messageID string
}

type MemoryStore struct {
	mu         sync.RWMutex
	devices    map[string]deviceRecord
	deletedIDs map[string]struct{}
	seen       map[messageKey]struct{}
	history    []telemetry.Sample
}

var _ device.Repository = (*MemoryStore)(nil)
var _ telemetry.Repository = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		devices:    make(map[string]deviceRecord),
		deletedIDs: make(map[string]struct{}),
		seen:       make(map[messageKey]struct{}),
	}
}

func (s *MemoryStore) Create(ctx context.Context, config device.Device, secretDigest device.SecretDigest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, deleted := s.deletedIDs[config.ID]; deleted {
		return device.ErrIDDeleted
	}
	if _, exists := s.devices[config.ID]; exists {
		return device.ErrExists
	}
	config.Latest = nil
	s.devices[config.ID] = deviceRecord{
		config:       config,
		secretDigest: secretDigest,
	}
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, id string) (device.Device, error) {
	if err := ctx.Err(); err != nil {
		return device.Device{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	record, exists := s.devices[id]
	if !exists {
		return device.Device{}, device.ErrNotFound
	}
	return cloneDevice(record.config), nil
}

func (s *MemoryStore) List(ctx context.Context) ([]device.Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	devices := make([]device.Device, 0, len(s.devices))
	for _, record := range s.devices {
		devices = append(devices, cloneDevice(record.config))
	}
	sort.Slice(devices, func(i, j int) bool {
		return devices[i].ID < devices[j].ID
	})
	return devices, nil
}

func (s *MemoryStore) UpdateName(ctx context.Context, id, name string, updatedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	record, exists := s.devices[id]
	if !exists {
		return device.ErrNotFound
	}
	record.config.Name = name
	record.config.UpdatedAt = newerTime(record.config.UpdatedAt, updatedAt)
	s.devices[id] = record
	return nil
}

func (s *MemoryStore) SetEnabled(ctx context.Context, id string, enabled bool, updatedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	record, exists := s.devices[id]
	if !exists {
		return device.ErrNotFound
	}
	record.config.Enabled = enabled
	record.config.UpdatedAt = newerTime(record.config.UpdatedAt, updatedAt)
	s.devices[id] = record
	return nil
}

func (s *MemoryStore) ResetSecret(ctx context.Context, id string, secretDigest device.SecretDigest, updatedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	record, exists := s.devices[id]
	if !exists {
		return device.ErrNotFound
	}
	record.secretDigest = secretDigest
	record.config.UpdatedAt = newerTime(record.config.UpdatedAt, updatedAt)
	s.devices[id] = record
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, exists := s.devices[id]; !exists {
		return device.ErrNotFound
	}
	delete(s.devices, id)
	s.deletedIDs[id] = struct{}{}
	return nil
}

func (s *MemoryStore) Commit(ctx context.Context, deviceID, secret string, sample telemetry.Sample) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	record, exists := s.devices[deviceID]
	if !exists {
		return device.ErrNotFound
	}
	if sample.DeviceID != deviceID {
		return telemetry.ErrDeviceMismatch
	}
	provided := device.HashSecret(secret)
	if !hmac.Equal(record.secretDigest[:], provided[:]) {
		return telemetry.ErrInvalidSecret
	}
	if !record.config.Enabled {
		return telemetry.ErrDeviceDisabled
	}

	key := messageKey{deviceID: deviceID, messageID: sample.MessageID}
	if _, duplicate := s.seen[key]; duplicate {
		return telemetry.ErrDuplicateMessage
	}

	sample = cloneSample(sample)
	s.seen[key] = struct{}{}
	s.history = append(s.history, sample)
	updateLatest(&record.config, sample)
	s.devices[deviceID] = record
	return nil
}

func (s *MemoryStore) History(ctx context.Context, query telemetry.HistoryQuery) ([]telemetry.Sample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.Limit <= 0 {
		return nil, telemetry.ErrHistoryLimit
	}
	if query.Limit > telemetry.MaxHistoryLimit {
		return nil, telemetry.ErrHistoryLimit
	}
	if query.From == nil || query.To == nil {
		return nil, telemetry.ErrHistoryWindow
	}
	from := query.From.UTC()
	to := query.To.UTC()
	if from.After(to) || to.Sub(from) > telemetry.MaxHistoryWindow {
		return nil, telemetry.ErrHistoryWindow
	}

	// Commit 只追加 history，已追加的记录和指标 map 不再修改；复制 slice 头后即可在快照上查询。
	s.mu.RLock()
	history := s.history
	s.mu.RUnlock()

	candidates := sampleIndexHeap{samples: history}
	heap.Init(&candidates)
	for index, sample := range history {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if query.DeviceID != "" && sample.DeviceID != query.DeviceID {
			continue
		}
		if sample.SampledAt.Before(from) {
			continue
		}
		if sample.SampledAt.After(to) {
			continue
		}
		heap.Push(&candidates, index)
		if candidates.Len() > query.Limit {
			heap.Pop(&candidates)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	indexes := append([]int(nil), candidates.indexes...)
	sort.Slice(indexes, func(i, j int) bool {
		return sampleBefore(history[indexes[i]], history[indexes[j]])
	})
	result := make([]telemetry.Sample, len(indexes))
	for i, index := range indexes {
		result[i] = cloneSample(history[index])
	}
	return result, nil
}

type sampleIndexHeap struct {
	samples []telemetry.Sample
	indexes []int
}

func (h sampleIndexHeap) Len() int {
	return len(h.indexes)
}

func (h sampleIndexHeap) Less(i, j int) bool {
	return sampleBefore(h.samples[h.indexes[i]], h.samples[h.indexes[j]])
}

func (h sampleIndexHeap) Swap(i, j int) {
	h.indexes[i], h.indexes[j] = h.indexes[j], h.indexes[i]
}

func (h *sampleIndexHeap) Push(value any) {
	h.indexes = append(h.indexes, value.(int))
}

func (h *sampleIndexHeap) Pop() any {
	last := len(h.indexes) - 1
	value := h.indexes[last]
	h.indexes = h.indexes[:last]
	return value
}

func sampleBefore(left, right telemetry.Sample) bool {
	if !left.SampledAt.Equal(right.SampledAt) {
		return left.SampledAt.Before(right.SampledAt)
	}
	if !left.ReceivedAt.Equal(right.ReceivedAt) {
		return left.ReceivedAt.Before(right.ReceivedAt)
	}
	if left.MessageID != right.MessageID {
		return left.MessageID < right.MessageID
	}
	return left.DeviceID < right.DeviceID
}

func updateLatest(config *device.Device, sample telemetry.Sample) {
	lastValidReceivedAt := sample.ReceivedAt
	if config.Latest != nil && config.Latest.LastValidReceivedAt.After(lastValidReceivedAt) {
		lastValidReceivedAt = config.Latest.LastValidReceivedAt
	}

	if config.Latest != nil && !isNewerSample(sample, config.Latest) {
		config.Latest.LastValidReceivedAt = lastValidReceivedAt
		return
	}

	config.Latest = &device.LatestState{
		Metrics:             toMetricStates(sample.Metrics),
		SampledAt:           sample.SampledAt,
		ReceivedAt:          sample.ReceivedAt,
		LastValidReceivedAt: lastValidReceivedAt,
		MessageID:           sample.MessageID,
	}
}

// 决胜顺序固定为采样时间、接收时间、消息 ID，保证同一输入得到稳定结果。
func isNewerSample(sample telemetry.Sample, current *device.LatestState) bool {
	if !sample.SampledAt.Equal(current.SampledAt) {
		return sample.SampledAt.After(current.SampledAt)
	}
	if !sample.ReceivedAt.Equal(current.ReceivedAt) {
		return sample.ReceivedAt.After(current.ReceivedAt)
	}
	return sample.MessageID > current.MessageID
}

func newerTime(current, candidate time.Time) time.Time {
	if candidate.After(current) {
		return candidate
	}
	return current
}

func toMetricStates(metrics map[string]telemetry.MetricValue) map[string]device.MetricState {
	if metrics == nil {
		return nil
	}
	cloned := make(map[string]device.MetricState, len(metrics))
	for name, metric := range metrics {
		cloned[name] = device.MetricState{Value: metric.Value, Unit: metric.Unit}
	}
	return cloned
}

func cloneDevice(config device.Device) device.Device {
	config.Latest = cloneLatest(config.Latest)
	return config
}

func cloneLatest(latest *device.LatestState) *device.LatestState {
	if latest == nil {
		return nil
	}
	cloned := *latest
	if latest.Metrics != nil {
		cloned.Metrics = make(map[string]device.MetricState, len(latest.Metrics))
		for name, metric := range latest.Metrics {
			cloned.Metrics[name] = metric
		}
	}
	return &cloned
}

func cloneSample(sample telemetry.Sample) telemetry.Sample {
	if sample.Metrics != nil {
		metrics := make(map[string]telemetry.MetricValue, len(sample.Metrics))
		for name, metric := range sample.Metrics {
			metrics[name] = metric
		}
		sample.Metrics = metrics
	}
	return sample
}
