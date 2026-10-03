package storage

import (
	"context"
	"sort"
	"time"

	"Project/internal/command"
	"Project/internal/device"
)

var _ command.Repository = (*MemoryStore)(nil)

func (s *MemoryStore) CreateCommand(ctx context.Context, entry command.Command) (command.Command, error) {
	if err := ctx.Err(); err != nil {
		return command.Command{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return command.Command{}, err
	}
	key := commandKey{deviceID: entry.DeviceID, commandID: entry.CommandID}
	if existing, ok := s.commands[key]; ok {
		if !sameCommandContent(existing, entry) {
			return command.Command{}, command.ErrCommandConflict
		}
		return cloneCommand(existing), nil
	}
	record, ok := s.devices[entry.DeviceID]
	if !ok {
		return command.Command{}, device.ErrNotFound
	}
	if !record.config.Enabled {
		return command.Command{}, command.ErrDeviceUnavailable
	}
	if record.config.Latest == nil {
		return command.Command{}, command.ErrMetricNotReady
	}
	state, ok := record.config.Latest.Metrics[entry.MetricKey]
	if !ok {
		return command.Command{}, command.ErrMetricNotReady
	}
	if !state.Modifiable {
		return command.Command{}, command.ErrMetricNotMutable
	}
	for _, definition := range record.config.MetricDefinitions {
		if definition.Key != entry.MetricKey {
			continue
		}
		if !definition.Enabled {
			return command.Command{}, command.ErrMetricNotMutable
		}
		if entry.Value != nil && (definition.MinValue != nil && *entry.Value < *definition.MinValue || definition.MaxValue != nil && *entry.Value > *definition.MaxValue) {
			return command.Command{}, command.ErrInvalidCommand
		}
		break
	}
	for _, pending := range s.commands {
		if pending.DeviceID == entry.DeviceID && pending.MetricKey == entry.MetricKey &&
			(pending.Status == command.StatusWaitingToSend || pending.Status == command.StatusBrokerAcked || pending.Status == command.StatusResultUnknown) {
			return command.Command{}, command.ErrCommandPending
		}
	}
	entry = cloneCommand(entry)
	s.commands[key] = entry
	return cloneCommand(entry), nil
}

func (s *MemoryStore) GetCommand(ctx context.Context, deviceID, commandID string) (command.Command, error) {
	if err := ctx.Err(); err != nil {
		return command.Command{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.commands[commandKey{deviceID: deviceID, commandID: commandID}]
	if !ok {
		return command.Command{}, command.ErrCommandNotFound
	}
	return cloneCommand(entry), nil
}

func (s *MemoryStore) CommandHistory(ctx context.Context, deviceID string, limit int) ([]command.Command, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > command.MaxHistoryLimit {
		return nil, command.ErrCommandLimit
	}
	s.mu.RLock()
	entries := make([]command.Command, 0)
	for _, entry := range s.commands {
		if entry.DeviceID == deviceID {
			entries = append(entries, cloneCommand(entry))
		}
	}
	s.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].CreatedAt.After(entries[j].CreatedAt)
		}
		return entries[i].CommandID > entries[j].CommandID
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func (s *MemoryStore) ClaimDue(ctx context.Context, now time.Time, limit, maxAttempts int, retryBase, retryMaximum time.Duration) ([]command.Command, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	eligible := make([]command.Command, 0)
	for key, entry := range s.commands {
		record, active := s.devices[entry.DeviceID]
		if !active || !record.config.Enabled || entry.Attempts >= maxAttempts || entry.NextAttemptAt.After(now) || !entry.DeadlineAt.After(now) ||
			(entry.Status != command.StatusWaitingToSend && entry.Status != command.StatusBrokerAcked) {
			continue
		}
		if !commandStillMutable(record.config, entry) {
			if entry.Status == command.StatusWaitingToSend && entry.Attempts == 0 {
				entry.Status = command.StatusCancelled
				entry.LastError = "发送前 Latest 已声明不可修改或数值超出已配置范围"
			} else {
				entry.Status = command.StatusResultUnknown
				entry.LastError = "重试前 Latest 已声明不可修改或数值超出范围；设备执行情况未知"
			}
			s.commands[key] = entry
			continue
		}
		eligible = append(eligible, cloneCommand(entry))
	}
	sort.Slice(eligible, func(i, j int) bool {
		if !eligible[i].NextAttemptAt.Equal(eligible[j].NextAttemptAt) {
			return eligible[i].NextAttemptAt.Before(eligible[j].NextAttemptAt)
		}
		return eligible[i].CommandID < eligible[j].CommandID
	})
	if len(eligible) > limit {
		eligible = eligible[:limit]
	}
	for index := range eligible {
		entry := eligible[index]
		key := commandKey{deviceID: entry.DeviceID, commandID: entry.CommandID}
		entry.Attempts++
		entry.LastAttemptAt = timePtr(now.UTC())
		entry.NextAttemptAt = now.Add(retryDelay(entry.Attempts-1, retryBase, retryMaximum)).UTC()
		s.commands[key] = entry
		eligible[index] = cloneCommand(entry)
	}
	return eligible, nil
}

func commandStillMutable(config device.Device, entry command.Command) bool {
	if config.Latest == nil {
		return false
	}
	state, ok := config.Latest.Metrics[entry.MetricKey]
	if !ok || !state.Modifiable {
		return false
	}
	for _, definition := range config.MetricDefinitions {
		if definition.Key != entry.MetricKey {
			continue
		}
		if !definition.Enabled {
			return false
		}
		if entry.Value != nil && (definition.MinValue != nil && *entry.Value < *definition.MinValue || definition.MaxValue != nil && *entry.Value > *definition.MaxValue) {
			return false
		}
		break
	}
	return true
}

func (s *MemoryStore) DispatchDue(ctx context.Context, now time.Time, limit, maxAttempts int, retryBase, retryMaximum time.Duration, publish func(context.Context, command.Command) error) error {
	for count := 0; count < limit; count++ {
		entries, err := s.ClaimDue(ctx, now, 1, maxAttempts, retryBase, retryMaximum)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		entry := entries[0]
		publishErr := publish(ctx, entry)
		key := commandKey{deviceID: entry.DeviceID, commandID: entry.CommandID}
		s.mu.Lock()
		current, ok := s.commands[key]
		if ok && (current.Status == command.StatusWaitingToSend || current.Status == command.StatusBrokerAcked) {
			if publishErr != nil {
				current.LastError = publishErr.Error()
				if len(current.LastError) > 512 {
					current.LastError = current.LastError[:512]
				}
			} else {
				current.Status = command.StatusBrokerAcked
				if current.BrokerAckedAt == nil {
					current.BrokerAckedAt = timePtr(time.Now().UTC())
				}
				current.LastError = ""
			}
			s.commands[key] = current
		}
		s.mu.Unlock()
	}
	return nil
}

func retryDelay(attempt int, base, maximum time.Duration) time.Duration {
	delay := base
	for index := 0; index < attempt && delay < maximum; index++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func (s *MemoryStore) MarkBrokerAck(ctx context.Context, deviceID, commandID string, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := commandKey{deviceID: deviceID, commandID: commandID}
	entry, ok := s.commands[key]
	if !ok {
		return command.ErrCommandNotFound
	}
	if entry.Status == command.StatusWaitingToSend || entry.Status == command.StatusBrokerAcked {
		entry.Status = command.StatusBrokerAcked
		if entry.BrokerAckedAt == nil {
			entry.BrokerAckedAt = timePtr(at.UTC())
		}
		entry.LastError = ""
		s.commands[key] = entry
	}
	return nil
}

func (s *MemoryStore) MarkPublishFailure(ctx context.Context, deviceID, commandID, reason string, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := commandKey{deviceID: deviceID, commandID: commandID}
	entry, ok := s.commands[key]
	if !ok {
		return command.ErrCommandNotFound
	}
	if len(reason) > 512 {
		reason = reason[:512]
	}
	if entry.Status == command.StatusWaitingToSend || entry.Status == command.StatusBrokerAcked {
		entry.LastError = reason
		s.commands[key] = entry
	}
	return nil
}

func (s *MemoryStore) Expire(ctx context.Context, now time.Time, _ int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.commands {
		if entry.Status != command.StatusWaitingToSend && entry.Status != command.StatusBrokerAcked {
			continue
		}
		if !entry.DeadlineAt.After(now) {
			if entry.Attempts == 0 {
				entry.Status = command.StatusCancelled
				entry.LastError = "期限内未能发送；设备执行情况确定为未发送"
			} else {
				entry.Status = command.StatusResultUnknown
				entry.LastError = "命令期限结束仍未收到设备结果；执行情况未知"
			}
			s.commands[key] = entry
		}
	}
	return nil
}

func (s *MemoryStore) ProcessResult(ctx context.Context, deviceID string, result command.Result, raw []byte, at time.Time) (command.Disposition, error) {
	if err := ctx.Err(); err != nil {
		return command.Disposition{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := commandKey{deviceID: deviceID, commandID: result.CommandID}
	entry, ok := s.commands[key]
	if !ok {
		s.anomalies = append(s.anomalies, "未知 command_id 或结果 Topic 设备不匹配")
		return command.Disposition{Anomaly: s.anomalies[len(s.anomalies)-1]}, nil
	}
	problem := validateCommandResult(entry, result)
	if problem == "" && entry.Status == command.StatusCancelled {
		problem = "已取消命令收到迟到结果"
	}
	if problem == "" && (entry.Status == command.StatusApplied || entry.Status == command.StatusRejected) {
		if entry.Status == result.Status {
			copy := cloneCommand(entry)
			return command.Disposition{Command: &copy, Duplicate: true}, nil
		}
		problem = "同一命令收到冲突的终态结果"
	}
	if problem != "" {
		s.anomalies = append(s.anomalies, problem)
		copy := cloneCommand(entry)
		return command.Disposition{Command: &copy, Anomaly: problem}, nil
	}
	entry.Status = result.Status
	entry.ResultReceivedAt = timePtr(at.UTC())
	entry.LastError = ""
	s.commands[key] = entry
	copy := cloneCommand(entry)
	return command.Disposition{Command: &copy}, nil
}

func (s *MemoryStore) CommandResultAnomalies() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.anomalies...)
}

func cloneCommand(entry command.Command) command.Command {
	entry.Value = commandValueCopy(entry.Value)
	entry.LastAttemptAt = timeCopy(entry.LastAttemptAt)
	entry.BrokerAckedAt = timeCopy(entry.BrokerAckedAt)
	entry.ResultReceivedAt = timeCopy(entry.ResultReceivedAt)
	return entry
}

func commandValueCopy(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func timeCopy(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func timePtr(value time.Time) *time.Time { return &value }
