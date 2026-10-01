package device

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const lifecycleTimeout = 8 * time.Second

func lifecycleStep(ctx context.Context, action func(context.Context) error) error {
	step, cancel := context.WithTimeout(ctx, lifecycleTimeout)
	defer cancel()
	return action(step)
}

func (s *Service) durableCreate(ctx context.Context, repo DurableLifecycle, d Device, secret string) (string, error) {
	digest := HashSecret(secret)
	if err := lifecycleStep(ctx, func(q context.Context) error { return repo.BeginCreate(q, d, digest) }); err != nil {
		return "", err
	}
	if err := lifecycleStep(ctx, func(q context.Context) error { return s.broker.CreateDevice(q, d.ID, secret) }); err != nil {
		return "", fmt.Errorf("Broker 创建状态待恢复，密钥不会交付: %w", err)
	}
	if err := lifecycleStep(ctx, func(q context.Context) error {
		return repo.FinishOperation(q, d.ID, "create", nil, true, s.currentTime())
	}); err != nil {
		return "", fmt.Errorf("设备创建提交状态待核实，密钥不会交付: %w", err)
	}
	return secret, nil
}
func (s *Service) durableEnabled(ctx context.Context, repo DurableLifecycle, id string, enabled bool) error {
	operation := "disable"
	if enabled {
		operation = "enable"
	}
	if err := lifecycleStep(ctx, func(q context.Context) error { return repo.BeginOperation(q, id, operation, s.currentTime()) }); err != nil {
		return err
	}
	if err := lifecycleStep(ctx, func(q context.Context) error { return s.broker.SetDeviceEnabled(q, id, enabled) }); err != nil {
		return fmt.Errorf("Broker 状态待恢复: %w", err)
	}
	if err := lifecycleStep(ctx, func(q context.Context) error {
		return repo.FinishOperation(q, id, operation, nil, enabled, s.currentTime())
	}); err != nil {
		return fmt.Errorf("设备状态提交待恢复: %w", err)
	}
	return nil
}
func (s *Service) durableReset(ctx context.Context, repo DurableLifecycle, id string) (string, error) {
	existing, err := s.repo.Get(ctx, id)
	if err != nil {
		return "", err
	}
	secret, err := GenerateSecret()
	if err != nil {
		return "", err
	}
	if err = lifecycleStep(ctx, func(q context.Context) error { return repo.BeginOperation(q, id, "reset", s.currentTime()) }); err != nil {
		return "", err
	}
	if err = lifecycleStep(ctx, func(q context.Context) error { return s.broker.ResetDeviceSecret(q, id, secret, existing.Enabled) }); err != nil {
		return "", fmt.Errorf("Broker 密钥状态待恢复，密钥不会交付: %w", err)
	}
	digest := HashSecret(secret)
	if err = lifecycleStep(ctx, func(q context.Context) error {
		return repo.FinishOperation(q, id, "reset", &digest, existing.Enabled, s.currentTime())
	}); err != nil {
		return "", fmt.Errorf("密钥提交状态待核实，密钥不会交付: %w", err)
	}
	return secret, nil
}
func (s *Service) durableDelete(ctx context.Context, repo DurableLifecycle, id string) error {
	if err := lifecycleStep(ctx, func(q context.Context) error { return repo.BeginOperation(q, id, "delete", s.currentTime()) }); err != nil {
		return err
	}
	if err := lifecycleStep(ctx, func(q context.Context) error { return s.broker.DeleteDevice(q, id) }); err != nil {
		return fmt.Errorf("Broker 删除待恢复: %w", err)
	}
	if err := lifecycleStep(ctx, func(q context.Context) error {
		return repo.FinishOperation(q, id, "delete", nil, false, s.currentTime())
	}); err != nil {
		return fmt.Errorf("删除提交待恢复: %w", err)
	}
	return nil
}

// RecoverPending 重试持久化意图。无法确认的创建会撤销并保留 ID 墓碑；密钥重置中断后保持禁用，由操作员重新重置。
func (s *Service) RecoverPending(ctx context.Context) error {
	repo, ok := s.repo.(DurableLifecycle)
	if !ok || s.broker == nil {
		return nil
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	var pending []PendingOperation
	if err := lifecycleStep(ctx, func(q context.Context) error { var e error; pending, e = repo.PendingOperations(q); return e }); err != nil {
		return err
	}
	var failures []error
	for _, p := range pending {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		var err error
		switch p.Operation {
		case "create", "delete":
			err = lifecycleStep(ctx, func(q context.Context) error { return s.broker.DeleteDevice(q, p.ID) })
		case "enable", "disable", "reset":
			err = lifecycleStep(ctx, func(q context.Context) error { return s.broker.SetDeviceEnabled(q, p.ID, false) })
		default:
			err = fmt.Errorf("未知待恢复操作 %q", p.Operation)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("恢复设备 %q 的 %s 操作: %w", p.ID, p.Operation, err))
			continue
		}
		finish := p.Operation
		if finish == "create" {
			finish = "create-abort"
		}
		err = lifecycleStep(ctx, func(q context.Context) error {
			return repo.FinishOperation(q, p.ID, finish, nil, false, s.currentTime())
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("完成设备 %q 的恢复: %w", p.ID, err))
		}
	}
	return errors.Join(failures...)
}
