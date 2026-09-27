package telemetry

import (
	"context"
	"sync"
)

type MemoryRepository struct {
	mu      sync.RWMutex
	seen    map[messageKey]struct{}
	samples []Sample
	latest  map[string]LatestStatus
}

type messageKey struct {
	deviceID  string
	messageID string
}

var _ Repository = (*MemoryRepository)(nil)

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		seen:   make(map[messageKey]struct{}),
		latest: make(map[string]LatestStatus),
	}
}

func (r *MemoryRepository) Store(ctx context.Context, sample Sample) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	key := messageKey{deviceID: sample.DeviceID, messageID: sample.MessageID}
	if _, exists := r.seen[key]; exists {
		return ErrDuplicateMessage
	}

	sample.Metrics = cloneMetrics(sample.Metrics)
	r.seen[key] = struct{}{}
	r.samples = append(r.samples, sample)
	r.updateLatest(sample)
	return nil
}

func (r *MemoryRepository) updateLatest(sample Sample) {
	current, exists := r.latest[sample.DeviceID]
	lastReceivedAt := sample.ReceivedAt
	if exists && current.LastReceivedAt.After(lastReceivedAt) {
		lastReceivedAt = current.LastReceivedAt
	}

	if exists && !isNewerSample(sample, current) {
		current.LastReceivedAt = lastReceivedAt
		r.latest[sample.DeviceID] = current
		return
	}

	r.latest[sample.DeviceID] = LatestStatus{
		DeviceID:       sample.DeviceID,
		MessageID:      sample.MessageID,
		SampledAt:      sample.SampledAt,
		ReceivedAt:     sample.ReceivedAt,
		LastReceivedAt: lastReceivedAt,
		Metrics:        cloneMetrics(sample.Metrics),
	}
}

// 采样时间相同时，后接收的消息优先；两个时间都相同时用消息编号稳定决胜。
func isNewerSample(sample Sample, current LatestStatus) bool {
	if !sample.SampledAt.Equal(current.SampledAt) {
		return sample.SampledAt.After(current.SampledAt)
	}
	if !sample.ReceivedAt.Equal(current.ReceivedAt) {
		return sample.ReceivedAt.After(current.ReceivedAt)
	}
	return sample.MessageID > current.MessageID
}

func (r *MemoryRepository) Samples(ctx context.Context) ([]Sample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	samples := make([]Sample, len(r.samples))
	for i, sample := range r.samples {
		sample.Metrics = cloneMetrics(sample.Metrics)
		samples[i] = sample
	}
	return samples, nil
}

func (r *MemoryRepository) Latest(ctx context.Context, deviceID string) (LatestStatus, bool, error) {
	if err := ctx.Err(); err != nil {
		return LatestStatus{}, false, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	latest, ok := r.latest[deviceID]
	if !ok {
		return LatestStatus{}, false, nil
	}
	latest.Metrics = cloneMetrics(latest.Metrics)
	return latest, true, nil
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
