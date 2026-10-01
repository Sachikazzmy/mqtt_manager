package device

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidID            = errors.New("设备编号无效")
	ErrBrokerStateUncertain = errors.New("Broker 状态不确定")
)

type BrokerLifecycle interface {
	CreateDevice(ctx context.Context, id, secret string) error
	SetDeviceEnabled(ctx context.Context, id string, enabled bool) error
	ResetDeviceSecret(ctx context.Context, id, secret string, enabled bool) error
	DeleteDevice(ctx context.Context, id string) error
}

type Service struct {
	repo        Repository
	broker      BrokerLifecycle
	operationMu sync.Mutex
	now         func() time.Time
}

// 目前的一切操作都在对虚拟的接口进行操作，上层只需要接管接口，而不去在意接口是由什么功能实现
func NewService(repo Repository) *Service {
	return newServiceWithClock(repo, time.Now, nil)
}

func NewManagedService(repo Repository, broker BrokerLifecycle) *Service {
	return newServiceWithClock(repo, time.Now, broker)
}

func newServiceWithClock(repo Repository, now func() time.Time, broker BrokerLifecycle) *Service {
	return &Service{
		repo:   repo,
		now:    now,
		broker: broker,
	}
}

func ValidateMetricLimit(limit int) error {
	if limit < MinMetricLimit || limit > HardMaxMetricLimit {
		return fmt.Errorf("每设备指标总数上限必须在 %d 到 %d 之间", MinMetricLimit, HardMaxMetricLimit)
	}
	return nil
}

func (s *Service) currentTime() time.Time {
	return s.now().UTC()
}

// 复用id与name的校验
func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("%w：设备编号不能为空", ErrInvalidID)
	}
	if id == "admin" {
		return fmt.Errorf("%w：设备编号不能占用 Broker 管理账户", ErrInvalidID)
	}
	if len(id) > 64 {
		return fmt.Errorf("%w：设备编号不能超过 64 字节", ErrInvalidID)
	}
	for index, value := range id {
		allowed := value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '-'
		if !allowed || index == 0 && !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9') {
			return fmt.Errorf("%w：只允许字母、数字、下划线和连字符，且必须以字母或数字开头", ErrInvalidID)
		}
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

	name = strings.TrimSpace(name)

	if err := validateID(id); err != nil {
		return "", err
	}
	if err := validateName(name); err != nil {
		return "", err
	}

	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if err := s.repo.CheckCreate(ctx, id); err != nil {
		return "", fmt.Errorf("新增设备 %q 失败：%w", id, err)
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
	if durable, ok := s.repo.(DurableLifecycle); ok && s.broker != nil {
		return s.durableCreate(ctx, durable, d, secret)
	}

	if s.broker != nil {
		if err := s.broker.CreateDevice(ctx, id, secret); err != nil {
			return "", fmt.Errorf("同步设备 %q 到 Broker 失败：%w", id, err)
		}
	}

	if err := s.repo.Create(context.WithoutCancel(ctx), d, HashSecret(secret)); err != nil {
		if s.broker != nil {
			compensationCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if compensationErr := s.broker.DeleteDevice(compensationCtx, id); compensationErr != nil {
				return "", fmt.Errorf("本地注册失败且 Broker 补偿失败，设备状态不确定：%w", errors.Join(err, compensationErr, ErrBrokerStateUncertain))
			}
		}
		return "", fmt.Errorf("新增设备 %q 失败：%w", id, err)
	}

	return secret, nil
}

func (s *Service) ResetSecret(ctx context.Context, id string) (string, error) {
	if err := validateID(id); err != nil {
		return "", err
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if durable, ok := s.repo.(DurableLifecycle); ok && s.broker != nil {
		return s.durableReset(ctx, durable, id)
	}
	existing, err := s.repo.Get(ctx, id)
	if err != nil {
		return "", fmt.Errorf("重置设备 %q 密钥失败：%w", id, err)
	}
	secret, err := GenerateSecret()
	if err != nil {
		return "", fmt.Errorf("生成设备密钥失败: %w", err)
	}
	if existing.Enabled {
		if err := s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime()); err != nil {
			return "", fmt.Errorf("重置设备 %q 密钥失败：%w", id, err)
		}
	}
	if s.broker != nil {
		if err := s.broker.ResetDeviceSecret(ctx, id, secret, existing.Enabled); err != nil {
			if !errors.Is(err, ErrBrokerStateUncertain) && existing.Enabled {
				_ = s.repo.SetEnabled(context.WithoutCancel(ctx), id, true, s.currentTime())
			}
			return "", fmt.Errorf("重置设备 %q 密钥失败：%w", id, err)
		}
	}
	if err := s.repo.ResetSecret(context.WithoutCancel(ctx), id, HashSecret(secret), s.currentTime()); err != nil {
		_ = s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime())
		if s.broker != nil {
			return "", fmt.Errorf("Broker 密钥已更新但本地提交失败，设备已禁用：%w", errors.Join(err, ErrBrokerStateUncertain))
		}
		return "", fmt.Errorf("重置设备 %q 密钥失败: %w", id, err)
	}
	if existing.Enabled {
		if err := s.repo.SetEnabled(context.WithoutCancel(ctx), id, true, s.currentTime()); err != nil {
			_ = s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime())
			if s.broker != nil {
				_ = s.broker.SetDeviceEnabled(context.Background(), id, false)
				return "", fmt.Errorf("本地恢复设备启用状态失败，Broker 账户已禁用：%w", errors.Join(err, ErrBrokerStateUncertain))
			}
			return "", fmt.Errorf("恢复设备启用状态失败: %w", err)
		}
	}
	return secret, nil
}

func (s *Service) Get(ctx context.Context, id string) (Device, error) {
	if err := validateID(id); err != nil {
		return Device{}, err
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
	if err := validateID(id); err != nil {
		return err
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if durable, ok := s.repo.(DurableLifecycle); ok && s.broker != nil {
		return s.durableEnabled(ctx, durable, id, enabled)
	}
	existing, err := s.repo.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("修改设备启用状态 %q 失败：%w", id, err)
	}

	if enabled {
		if s.broker == nil {
			if existing.Enabled {
				return nil
			}
			if err := s.repo.SetEnabled(ctx, id, true, s.currentTime()); err != nil {
				return fmt.Errorf("修改设备启用状态 %q 失败: %w", id, err)
			}
			return nil
		}

		if existing.Enabled {
			if err := s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime()); err != nil {
				return fmt.Errorf("同步设备 %q 启用状态失败：%w", id, err)
			}
		}
		if err := s.broker.SetDeviceEnabled(ctx, id, true); err != nil {
			return fmt.Errorf("启用设备 %q 的 Broker 账户失败，本地设备保持禁用：%w", id, err)
		}
		if err := s.repo.SetEnabled(ctx, id, true, s.currentTime()); err != nil {
			_ = s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime())
			compensationCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if compensationErr := s.broker.SetDeviceEnabled(compensationCtx, id, false); compensationErr != nil {
				return fmt.Errorf("Broker 已启用但本地提交失败，设备状态不确定：%w", errors.Join(err, compensationErr, ErrBrokerStateUncertain))
			}
			return fmt.Errorf("修改设备启用状态 %q 失败: %w", id, err)
		}
		return nil
	}

	if s.broker != nil {
		if err := s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime()); err != nil {
			return fmt.Errorf("关闭设备 %q 的本地接收失败：%w", id, err)
		}
		if err := s.broker.SetDeviceEnabled(ctx, id, false); err != nil {
			return fmt.Errorf("本地设备已禁用，但禁用设备 %q 的 Broker 账户失败：%w", id, err)
		}
		return nil
	}

	if err := s.repo.SetEnabled(ctx, id, false, s.currentTime()); err != nil {
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
	if err := validateID(id); err != nil {
		return err
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if durable, ok := s.repo.(DurableLifecycle); ok && s.broker != nil {
		return s.durableDelete(ctx, durable, id)
	}
	if _, err := s.repo.Get(ctx, id); err != nil {
		return fmt.Errorf("删除设备 %q 失败：%w", id, err)
	}
	if s.broker != nil {
		if err := s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime()); err != nil {
			return fmt.Errorf("关闭待删除设备 %q 的本地接收失败：%w", id, err)
		}
		if err := s.broker.DeleteDevice(ctx, id); err != nil {
			return fmt.Errorf("设备已在本地禁用，但删除设备 %q 的 Broker 账户失败：%w", id, err)
		}
	}

	if err := s.repo.Delete(context.WithoutCancel(ctx), id); err != nil {
		_ = s.repo.SetEnabled(context.WithoutCancel(ctx), id, false, s.currentTime())
		if s.broker != nil {
			return fmt.Errorf("Broker 账户已删除但本地删除失败，设备已禁用：%w", errors.Join(err, ErrBrokerStateUncertain))
		}
		return fmt.Errorf("删除设备 %q 失败: %w", id, err)
	}

	return nil
}
