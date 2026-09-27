package device

import (
	"context"
	"sort"
	"sync"
	"time"
)

type MemoryRepository struct {
	mu      sync.RWMutex
	devices map[string]Device
}

var _ Repository = (*MemoryRepository)(nil)

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		devices: make(map[string]Device),
	}
}

func (r *MemoryRepository) Create(ctx context.Context, d Device) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.devices[d.ID]; exists {
		return ErrExists
	}

	r.devices[d.ID] = d
	return nil
}

func (r *MemoryRepository) Get(
	ctx context.Context,
	id string,
) (Device, error) {
	if err := ctx.Err(); err != nil {
		return Device{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	d, exists := r.devices[id]
	if !exists {
		return Device{}, ErrNotFound
	}
	return d, nil
}

func (r *MemoryRepository) List(
	ctx context.Context,
) ([]Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	devices := make([]Device, 0, len(r.devices))
	for _, d := range r.devices {
		devices = append(devices, d)
	}

	sort.Slice(devices, func(i, j int) bool {
		return devices[i].ID < devices[j].ID
	})

	return devices, nil
}

func (r *MemoryRepository) UpdateName(ctx context.Context, id, name string, updatedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	d, exists := r.devices[id]
	if !exists {
		return ErrNotFound
	}

	d.Name = name
	if updatedAt.After(d.UpdatedAt) {
		d.UpdatedAt = updatedAt
	}
	r.devices[id] = d
	return nil
}

func (r *MemoryRepository) SetEnabled(ctx context.Context, id string, enabled bool, updatedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	d, exists := r.devices[id]
	if !exists {
		return ErrNotFound
	}

	d.Enabled = enabled
	if updatedAt.After(d.UpdatedAt) {
		d.UpdatedAt = updatedAt
	}
	r.devices[id] = d
	return nil
}

func (r *MemoryRepository) Delete(
	ctx context.Context,
	id string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.devices[id]; !exists {
		return ErrNotFound
	}

	delete(r.devices, id)
	return nil
}
