package device

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Service struct {
	repo Repository
	now  func() time.Time // Service层实现对传入信息的时间校验
}

// 目前的一切操作都在对虚拟的接口进行操作，上层只需要接管接口，而不去在意接口是由什么功能实现
func NewService(repo Repository) *Service {
	return newServiceWithClock(repo, time.Now)
}

func newServiceWithClock(repo Repository, now func() time.Time) *Service {
	return &Service{
		repo: repo,
		now:  now,
	}
}

func (s *Service) currentTime() time.Time {
	return s.now().UTC()
}

// 复用id与name的校验
func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("设备编号不能为空")
	}
	if len(id) > 64 {
		return fmt.Errorf("设备编号不能超过 64 字节")
	}
	return nil
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("设备名称不能为空")
	}
	if len(name) > 128 {
		return fmt.Errorf("设备名称不能超过 128 字节")
	}
	return nil
}

func (s *Service) Create(ctx context.Context, id, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)

	if err := validateID(id); err != nil {
		return "", err
	}
	if err := validateName(name); err != nil {
		return "", err
	}
	secret, err := GenerateSecret()
	if err != nil {
		return "", fmt.Errorf("生成设备密钥失败: %w", err)
	}

	now := s.currentTime()
	d := Device{
		ID:        id,
		Name:      name,
		Enabled:   true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.repo.Create(ctx, d, HashSecret(secret)); err != nil {
		return "", fmt.Errorf("新增设备 %q 失败： %w", id, err)
	}

	return secret, nil
}

func (s *Service) ResetSecret(ctx context.Context, id string) (string, error) {
	id = strings.TrimSpace(id)
	if err := validateID(id); err != nil {
		return "", err
	}
	secret, err := GenerateSecret()
	if err != nil {
		return "", fmt.Errorf("生成设备密钥失败: %w", err)
	}
	if err := s.repo.ResetSecret(ctx, id, HashSecret(secret), s.currentTime()); err != nil {
		return "", fmt.Errorf("重置设备 %q 密钥失败: %w", id, err)
	}
	return secret, nil
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

	if err := validateID(id); err != nil {
		return err
	}
	if err := validateName(name); err != nil {
		return err
	}

	if err := s.repo.UpdateName(ctx, id, name, s.currentTime()); err != nil {
		return fmt.Errorf("修改设备 %q 失败: %w", id, err)
	}
	return nil
}

// 增加设备启用与关闭选项
func (s *Service) setEnabled(ctx context.Context, id string, enabled bool) error {
	id = strings.TrimSpace(id)

	if err := validateID(id); err != nil {
		return err
	}

	if err := s.repo.SetEnabled(ctx, id, enabled, s.currentTime()); err != nil {
		return fmt.Errorf("修改设备启用状态 %q 失败: %w", id, err)
	}
	return nil
}

func (s *Service) Enable(ctx context.Context, id string) error {
	return s.setEnabled(ctx, id, true)
}

func (s *Service) Disable(ctx context.Context, id string) error {
	return s.setEnabled(ctx, id, false)
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
