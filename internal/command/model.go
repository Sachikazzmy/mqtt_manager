package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ActionSetMetric     = "set_metric"
	ActionClearOverride = "clear_override"

	StatusWaitingToSend = "waiting_to_send"
	StatusBrokerAcked   = "broker_acked"
	StatusApplied       = "applied"
	StatusRejected      = "rejected"
	StatusResultUnknown = "result_unknown"
	StatusCancelled     = "cancelled"

	ResultTopicSuffix  = "command_result"
	CommandTopicSuffix = "command"
	MaxResultBytes     = 64 * 1024
	MaxHistoryLimit    = 100
)

var (
	ErrInvalidCommand    = errors.New("命令无效")
	ErrInvalidResult     = errors.New("命令结果无效")
	ErrDeviceUnavailable = errors.New("设备不存在、已停用或正处于生命周期过渡")
	ErrDeviceTransition  = errors.New("设备生命周期操作正在进行，命令结果等待重投")
	ErrMetricNotReady    = errors.New("指标没有可用 Latest")
	ErrMetricNotMutable  = errors.New("指标当前不可修改")
	ErrCommandPending    = errors.New("该设备指标已有未解决命令")
	ErrCommandConflict   = errors.New("command_id 已用于不同命令内容")
	ErrCommandNotFound   = errors.New("命令不存在")
	ErrCommandLimit      = errors.New("命令历史数量无效")
)

type Command struct {
	DeviceID         string     `json:"device_id"`
	CommandID        string     `json:"command_id"`
	Action           string     `json:"action"`
	MetricKey        string     `json:"metric_key"`
	Value            *float64   `json:"value,omitempty"`
	Status           string     `json:"status"`
	CreatedAt        time.Time  `json:"created_at"`
	Attempts         int        `json:"attempts"`
	LastAttemptAt    *time.Time `json:"last_attempt_at,omitempty"`
	NextAttemptAt    time.Time  `json:"next_attempt_at"`
	DeadlineAt       time.Time  `json:"deadline_at"`
	BrokerAckedAt    *time.Time `json:"broker_acked_at,omitempty"`
	ResultReceivedAt *time.Time `json:"result_received_at,omitempty"`
	LastError        string     `json:"last_error,omitempty"`
}

type Request struct {
	CommandID string
	DeviceID  string
	Action    string
	MetricKey string
	Value     *float64
}

type Payload struct {
	Version   string   `json:"version"`
	CommandID string   `json:"command_id"`
	Action    string   `json:"action"`
	MetricKey string   `json:"metric_key"`
	Value     *float64 `json:"value,omitempty"`
}

type Result struct {
	Version   string   `json:"version"`
	CommandID string   `json:"command_id"`
	Status    string   `json:"status"`
	MetricKey string   `json:"metric_key"`
	Value     *float64 `json:"value,omitempty"`
}

type Disposition struct {
	Command   *Command
	Duplicate bool
	Anomaly   string
}

type Repository interface {
	CreateCommand(context.Context, Command) (Command, error)
	GetCommand(context.Context, string, string) (Command, error)
	CommandHistory(context.Context, string, int) ([]Command, error)
	DispatchDue(context.Context, time.Time, int, int, time.Duration, time.Duration, func(context.Context, Command) error) error
	Expire(context.Context, time.Time, int) error
	ProcessResult(context.Context, string, Result, []byte, time.Time) (Disposition, error)
}

type Publisher interface {
	PublishCommand(context.Context, string, []byte) error
}

func commandPayload(cmd Command) Payload {
	return Payload{
		Version: "1", CommandID: cmd.CommandID, Action: cmd.Action,
		MetricKey: cmd.MetricKey, Value: cloneFloat(cmd.Value),
	}
}

func ParseResult(payload []byte) (Result, error) {
	if len(payload) > MaxResultBytes {
		return Result{}, fmt.Errorf("%w：结果超过 %d 字节", ErrInvalidResult, MaxResultBytes)
	}
	if !utf8.Valid(payload) {
		return Result{}, fmt.Errorf("%w：JSON 必须是有效 UTF-8", ErrInvalidResult)
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	token, err := decoder.Token()
	if err != nil {
		return Result{}, fmt.Errorf("%w：%v", ErrInvalidResult, err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return Result{}, fmt.Errorf("%w：顶层必须是 JSON 对象", ErrInvalidResult)
	}
	var result Result
	seen := make(map[string]bool, 5)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return Result{}, fmt.Errorf("%w：%v", ErrInvalidResult, err)
		}
		name, ok := token.(string)
		if !ok || seen[name] {
			return Result{}, fmt.Errorf("%w：字段名无效或重复", ErrInvalidResult)
		}
		seen[name] = true
		switch name {
		case "version":
			err = decoder.Decode(&result.Version)
		case "command_id":
			err = decoder.Decode(&result.CommandID)
		case "status":
			err = decoder.Decode(&result.Status)
		case "metric_key":
			err = decoder.Decode(&result.MetricKey)
		case "value":
			var value *float64
			err = decoder.Decode(&value)
			if err == nil && value == nil {
				err = fmt.Errorf("value 不能是 null")
			}
			result.Value = value
		default:
			return Result{}, fmt.Errorf("%w：不支持字段 %q", ErrInvalidResult, name)
		}
		if err != nil {
			return Result{}, fmt.Errorf("%w：字段 %q：%v", ErrInvalidResult, name, err)
		}
	}
	if _, err = decoder.Token(); err != nil {
		return Result{}, fmt.Errorf("%w：对象不完整", ErrInvalidResult)
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		return Result{}, fmt.Errorf("%w：只能包含一个 JSON 对象", ErrInvalidResult)
	}
	if result.Version != "1" || !validToken(result.CommandID, 128) || !validMetricKey(result.MetricKey) ||
		(result.Status != StatusApplied && result.Status != StatusRejected) {
		return Result{}, fmt.Errorf("%w：version、command_id、status 或 metric_key 无效", ErrInvalidResult)
	}
	if result.Value != nil && (math.IsNaN(*result.Value) || math.IsInf(*result.Value, 0)) {
		return Result{}, fmt.Errorf("%w：value 必须是有限数字", ErrInvalidResult)
	}
	for _, required := range []string{"version", "command_id", "status", "metric_key"} {
		if !seen[required] {
			return Result{}, fmt.Errorf("%w：缺少字段 %q", ErrInvalidResult, required)
		}
	}
	return result, nil
}

func validToken(value string, limit int) bool {
	return value != "" && len(value) <= limit && value == strings.TrimSpace(value) && !strings.ContainsRune(value, '\x00')
}

func validMetricKey(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for index, char := range value {
		valid := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-'
		if !valid || index == 0 && !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
