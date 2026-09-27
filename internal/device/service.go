package device

import (
	"context"
	"fmt"
	"strings"
)

type Service struct {
	repo Repository
}

// 目前的一切操作都在对虚拟的接口进行操作，上层只需要接管接口，而不去在意接口是由什么功能实现
func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Create(ctx context.Context, id, name string) error {
	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)

	if id == "" {
		return fmt.Errorf("设备编号不能为空")
	}
	if len(id) > 64 {
		return fmt.Errorf("设备编号不能超出64字节")
	}
	if name == "" {
		return fmt.Errorf("设备名称不能为空")
	}
	if len(name) > 128 {
		return fmt.Errorf("设备名称不能超过128字节")
	}

	d := Device{
		ID:   id,
		Name: name,
	}

	if err := s.repo.Create(ctx, d); err != nil {
		return fmt.Errorf("新增设备 %q 失败： %w", id, err)
	}

	return nil
}

func (s *Service) Get(ctx context.Context, id string) (Device, error) {
	id = strings.TrimSpace(id)

	if id == "" {
		return Device{}, fmt.Errorf("设备编号不能为空")
	}

	d, err := s.repo.Get(ctx, id)
	if err != nil {
		return Device{}, fmt.Errorf("查询设备 %q 失败：%w", id, err)
	}

	return d, nil
}

func (s *Service) List(ctx context.Context) ([]Device, error) {
	devices, err := s.repo.List(ctx)

	if err != nil {
		return nil, fmt.Errorf("查询设备列表失败： %w", err)
	}
	return devices, nil
}

func (s *Service) Update(ctx context.Context, id, name string) error {
	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)

	if id == "" {
		return fmt.Errorf("设备编号不能为空")
	}
	if len(id) > 64 {
		return fmt.Errorf("设备编号不能超过 64 字节")
	}
	if name == "" {
		return fmt.Errorf("设备名称不能为空")
	}
	if len(name) > 128 {
		return fmt.Errorf("设备名称不能超过 128 字节")
	}

	d := Device{
		ID:   id,
		Name: name,
	}

	if err := s.repo.Update(ctx, d); err != nil {
		return fmt.Errorf("修改设备 %q 失败: %w", id, err)
	}

	return nil
}

func (s *Service) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)

	if id == "" {
		return fmt.Errorf("设备编号不能为空")
	}

	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("删除设备 %q 失败: %w", id, err)
	}

	return nil
}
