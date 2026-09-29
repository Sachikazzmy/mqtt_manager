package receive

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"Project/internal/device"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

const (
	dynsecTopic         = "$CONTROL/dynamic-security/v1"
	dynsecResponseTopic = "$CONTROL/dynamic-security/v1/response"
)

type dynsecACL struct {
	Type     string `json:"acltype"`
	Topic    string `json:"topic,omitempty"`
	Priority int    `json:"priority,omitempty"`
	Allow    bool   `json:"allow"`
}

type dynsecRoleRef struct {
	Name     string `json:"rolename"`
	Priority int    `json:"priority"`
}

type dynsecCommand struct {
	Command  string          `json:"command"`
	Username string          `json:"username,omitempty"`
	Password string          `json:"password,omitempty"`
	RoleName string          `json:"rolename,omitempty"`
	Roles    []dynsecRoleRef `json:"roles,omitempty"`
	ACLs     []dynsecACL     `json:"acls,omitempty"`
}

type dynsecResponse struct {
	Command string          `json:"command"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

type dynsecResponseEnvelope struct {
	Responses []dynsecResponse `json:"responses"`
}

type dynsecRequestEnvelope struct {
	Commands []dynsecCommand `json:"commands"`
}

type dynsecTransport interface {
	Execute(context.Context, []dynsecCommand) ([]dynsecResponse, error)
}

type Manager struct {
	mu               sync.Mutex
	transport        dynsecTransport
	receiverUsername string
	receiverPassword []byte
}

var _ device.BrokerLifecycle = (*Manager)(nil)

func NewManager(config Config) *Manager {
	username := config.ReceiverUsername
	if username == "" {
		username = "__project01_receiver__"
	}
	return &Manager{
		transport:        &mqttDynsecTransport{config: config},
		receiverUsername: username,
		receiverPassword: append([]byte(nil), config.ReceiverPassword...),
	}
}

func NewUnavailableManager(cause error) *Manager {
	return &Manager{transport: unavailableTransport{cause: cause}}
}

func (m *Manager) CreateDevice(ctx context.Context, id, secret string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	roleName := deviceRoleName(id)
	role, err := m.execute(ctx, dynsecCommand{Command: "getRole", RoleName: roleName})
	if err != nil {
		return err
	}
	if role.Error != "" {
		if !isNotFound(role.Error) {
			return commandFailure("读取设备发布角色", role)
		}
		role, err = m.execute(ctx, dynsecCommand{
			Command:  "createRole",
			RoleName: roleName,
			ACLs: []dynsecACL{{
				Type: "publishClientSend", Topic: telemetryTopic(id), Priority: 10, Allow: true,
			}},
		})
		if err != nil {
			return err
		}
		if role.Error != "" {
			return commandFailure("创建设备发布角色", role)
		}
	} else if err := validateDeviceRole(role.Data, roleName, telemetryTopic(id)); err != nil {
		return fmt.Errorf("Broker 中设备 %q 的账户角色冲突：%w", id, err)
	}

	client, err := m.execute(ctx, dynsecCommand{Command: "getClient", Username: id})
	if err != nil {
		return err
	}
	if client.Error != "" {
		if !isNotFound(client.Error) {
			return commandFailure("读取设备账户", client)
		}
		client, err = m.execute(ctx, dynsecCommand{
			Command:  "createClient",
			Username: id,
			Password: secret,
			Roles:    []dynsecRoleRef{{Name: roleName, Priority: 10}},
		})
		if err != nil {
			return err
		}
		if client.Error != "" {
			return commandFailure("创建设备账户", client)
		}
		return nil
	}
	if err := validateDeviceClient(client.Data, id, roleName); err != nil {
		return fmt.Errorf("Broker 中设备 %q 的账户冲突：%w", id, err)
	}

	// 进程重启后 Broker 可能还留有本应用创建的账户；角色校验通过后可安全轮换其密钥。
	return m.resetSecretLocked(ctx, id, secret, true)
}

func (m *Manager) BootstrapReceiver(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.receiverPassword) == 0 || m.receiverUsername == "" {
		return fmt.Errorf("%w：订阅账户密钥未配置", ErrBrokerUnavailable)
	}

	defaults, err := m.execute(ctx, dynsecCommand{
		Command: "setDefaultACLAccess",
		ACLs: []dynsecACL{
			{Type: "publishClientSend", Allow: false},
			{Type: "publishClientReceive", Allow: false},
			{Type: "subscribe", Allow: false},
			{Type: "unsubscribe", Allow: false},
		},
	})
	if err != nil {
		return err
	}
	if defaults.Error != "" {
		return commandFailure("设置 Broker 默认 ACL", defaults)
	}

	roleName := receiverRoleName()
	role, err := m.execute(ctx, dynsecCommand{Command: "getRole", RoleName: roleName})
	if err != nil {
		return err
	}
	if role.Error != "" {
		if !isNotFound(role.Error) {
			return commandFailure("读取订阅角色", role)
		}
		role, err = m.execute(ctx, dynsecCommand{
			Command:  "createRole",
			RoleName: roleName,
			ACLs:     receiverACLs(),
		})
		if err != nil {
			return err
		}
		if role.Error != "" {
			return commandFailure("创建订阅角色", role)
		}
	} else if err := validateReceiverRole(role.Data, roleName); err != nil {
		return fmt.Errorf("Broker 中订阅角色冲突：%w", err)
	}

	username := m.receiverUsername
	client, err := m.execute(ctx, dynsecCommand{Command: "getClient", Username: username})
	if err != nil {
		return err
	}
	if client.Error != "" {
		if !isNotFound(client.Error) {
			return commandFailure("读取订阅账户", client)
		}
		client, err = m.execute(ctx, dynsecCommand{
			Command:  "createClient",
			Username: username,
			Password: string(m.receiverPassword),
			Roles:    []dynsecRoleRef{{Name: roleName, Priority: 10}},
		})
		if err != nil {
			return err
		}
		if client.Error != "" {
			return commandFailure("创建订阅账户", client)
		}
		return nil
	}
	if err := validateDeviceClient(client.Data, username, roleName); err != nil {
		return fmt.Errorf("Broker 中订阅账户冲突：%w", err)
	}
	return m.resetManagedSecretLocked(ctx, username, string(m.receiverPassword), true, roleName)
}

func (m *Manager) SetDeviceEnabled(ctx context.Context, id string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	client, err := m.getManagedClient(ctx, id)
	if err != nil {
		return err
	}
	disabled, err := clientDisabled(client.Data)
	if err != nil {
		return err
	}
	if disabled == !enabled {
		return nil
	}
	command := "disableClient"
	if enabled {
		command = "enableClient"
	}
	response, err := m.execute(ctx, dynsecCommand{Command: command, Username: id})
	if err != nil {
		return err
	}
	if response.Error != "" {
		return commandFailure("修改设备 Broker 账户状态", response)
	}
	return nil
}

func (m *Manager) ResetDeviceSecret(ctx context.Context, id, secret string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resetSecretLocked(ctx, id, secret, enabled)
}

func (m *Manager) resetSecretLocked(ctx context.Context, id, secret string, enabled bool) error {
	return m.resetManagedSecretLocked(ctx, id, secret, enabled, deviceRoleName(id))
}

func (m *Manager) resetManagedSecretLocked(ctx context.Context, id, secret string, enabled bool, expectedRole string) error {
	client, err := m.getManagedClientWithRole(ctx, id, expectedRole)
	if err != nil {
		return err
	}
	disabled, err := clientDisabled(client.Data)
	if err != nil {
		return err
	}
	commands := make([]dynsecCommand, 0, 3)
	if !disabled {
		commands = append(commands, dynsecCommand{Command: "disableClient", Username: id})
	}
	commands = append(commands, dynsecCommand{Command: "setClientPassword", Username: id, Password: secret})
	if enabled {
		commands = append(commands, dynsecCommand{Command: "enableClient", Username: id})
	}
	responses, err := m.executeBatch(ctx, commands)
	if err != nil {
		return err
	}
	for _, response := range responses {
		if response.Error != "" {
			return fmt.Errorf("重置设备密钥时 Broker 可能已部分执行，设备应保持禁用：%w", errors.Join(device.ErrBrokerStateUncertain, commandFailure("重置设备密钥", response)))
		}
	}
	return nil
}

func (m *Manager) DeleteDevice(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	client, err := m.execute(ctx, dynsecCommand{Command: "getClient", Username: id})
	if err != nil {
		return err
	}
	if client.Error != "" {
		if isNotFound(client.Error) {
			return nil
		}
		return commandFailure("读取待删除账户", client)
	}
	if err := validateDeviceClient(client.Data, id, deviceRoleName(id)); err != nil {
		return fmt.Errorf("拒绝删除不属于本设备的 Broker 账户：%w", err)
	}
	if err := m.validateManagedRole(ctx, id, deviceRoleName(id)); err != nil {
		return fmt.Errorf("拒绝删除权限不符合预期的 Broker 账户：%w", err)
	}
	disabled, err := clientDisabled(client.Data)
	if err != nil {
		return err
	}
	commands := make([]dynsecCommand, 0, 2)
	if !disabled {
		commands = append(commands, dynsecCommand{Command: "disableClient", Username: id})
	}
	commands = append(commands, dynsecCommand{Command: "deleteClient", Username: id})
	responses, err := m.executeBatch(ctx, commands)
	if err != nil {
		return err
	}
	deleteResponse := responses[len(responses)-1]
	if deleteResponse.Error != "" && !isNotFound(deleteResponse.Error) {
		return errors.Join(device.ErrBrokerStateUncertain, commandFailure("删除设备 Broker 账户", deleteResponse))
	}
	return nil
}

func (m *Manager) getManagedClient(ctx context.Context, id string) (dynsecResponse, error) {
	return m.getManagedClientWithRole(ctx, id, deviceRoleName(id))
}

func (m *Manager) getManagedClientWithRole(ctx context.Context, id, expectedRole string) (dynsecResponse, error) {
	client, err := m.execute(ctx, dynsecCommand{Command: "getClient", Username: id})
	if err != nil {
		return dynsecResponse{}, err
	}
	if client.Error != "" {
		if isNotFound(client.Error) {
			return dynsecResponse{}, device.ErrNotFound
		}
		return dynsecResponse{}, commandFailure("读取设备 Broker 账户", client)
	}
	if err := validateDeviceClient(client.Data, id, expectedRole); err != nil {
		return dynsecResponse{}, fmt.Errorf("Broker 中设备 %q 的账户冲突：%w", id, err)
	}
	if err := m.validateManagedRole(ctx, id, expectedRole); err != nil {
		return dynsecResponse{}, fmt.Errorf("Broker 中设备 %q 的有效权限冲突：%w", id, err)
	}
	return client, nil
}

func (m *Manager) validateManagedRole(ctx context.Context, id, expectedRole string) error {
	role, err := m.execute(ctx, dynsecCommand{Command: "getRole", RoleName: expectedRole})
	if err != nil {
		return err
	}
	if role.Error != "" {
		return commandFailure("读取设备权限角色", role)
	}
	if expectedRole == receiverRoleName() {
		return validateReceiverRole(role.Data, expectedRole)
	}
	return validateDeviceRole(role.Data, expectedRole, telemetryTopic(id))
}

func (m *Manager) executeBatch(ctx context.Context, commands []dynsecCommand) ([]dynsecResponse, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	responses, err := m.transport.Execute(ctx, commands)
	if err != nil {
		return nil, err
	}
	if len(responses) != len(commands) {
		return nil, fmt.Errorf("%w：Broker 管理响应数量不匹配", device.ErrBrokerStateUncertain)
	}
	return responses, nil
}

func (m *Manager) execute(ctx context.Context, command dynsecCommand) (dynsecResponse, error) {
	responses, err := m.executeBatch(ctx, []dynsecCommand{command})
	if err != nil {
		return dynsecResponse{}, err
	}
	if len(responses) != 1 {
		return dynsecResponse{}, fmt.Errorf("%w：Broker 管理响应数量不匹配", device.ErrBrokerStateUncertain)
	}
	return responses[0], nil
}

func deviceRoleName(id string) string {
	digest := sha256.Sum256([]byte(id))
	return "_project01_device_" + hex.EncodeToString(digest[:24])
}

func telemetryTopic(id string) string {
	return "factory/" + id + "/telemetry"
}

func validateDeviceRole(data json.RawMessage, expectedRole, expectedTopic string) error {
	var envelope struct {
		Role struct {
			Name string      `json:"rolename"`
			ACLs []dynsecACL `json:"acls"`
		} `json:"role"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("解析角色失败")
	}
	if envelope.Role.Name != expectedRole || len(envelope.Role.ACLs) != 1 {
		return errors.New("角色权限与应用预期不一致")
	}
	acl := envelope.Role.ACLs[0]
	if acl.Type != "publishClientSend" || acl.Topic != expectedTopic || !acl.Allow {
		return errors.New("角色没有严格限定为本设备遥测 Topic")
	}
	return nil
}

func validateDeviceClient(data json.RawMessage, expectedID, expectedRole string) error {
	var envelope struct {
		Client struct {
			Username string            `json:"username"`
			Disabled bool              `json:"disabled"`
			Roles    []dynsecRoleRef   `json:"roles"`
			Groups   []json.RawMessage `json:"groups"`
		} `json:"client"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return errors.New("无法确认账户归属")
	}
	if envelope.Client.Username != expectedID {
		return errors.New("账户用户名与预期不一致")
	}
	if len(envelope.Client.Groups) != 0 {
		return errors.New("账户绑定了非预期组权限")
	}
	if len(envelope.Client.Roles) != 1 || envelope.Client.Roles[0].Name != expectedRole {
		return errors.New("账户不是仅绑定本设备专属角色")
	}
	return nil
}

func clientDisabled(data json.RawMessage) (bool, error) {
	var envelope struct {
		Client struct {
			Disabled bool `json:"disabled"`
		} `json:"client"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("%w：无法确认 Broker 账户启用状态", device.ErrBrokerStateUncertain)
	}
	// Dynamic Security omits disabled=false for enabled clients.
	return envelope.Client.Disabled, nil
}

func receiverRoleName() string {
	return "__project01_receiver_role__"
}

func receiverACLs() []dynsecACL {
	return []dynsecACL{
		{Type: "subscribePattern", Topic: telemetrySubscription, Priority: 10, Allow: true},
		{Type: "publishClientReceive", Topic: telemetrySubscription, Priority: 10, Allow: true},
	}
}

func validateReceiverRole(data json.RawMessage, expectedRole string) error {
	var envelope struct {
		Role struct {
			Name string      `json:"rolename"`
			ACLs []dynsecACL `json:"acls"`
		} `json:"role"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return errors.New("解析订阅角色失败")
	}
	if envelope.Role.Name != expectedRole || len(envelope.Role.ACLs) != 2 {
		return errors.New("订阅角色权限与应用预期不一致")
	}
	seen := make(map[string]bool, len(envelope.Role.ACLs))
	for _, acl := range envelope.Role.ACLs {
		if acl.Topic != telemetrySubscription || acl.Priority != 10 || !acl.Allow {
			return errors.New("订阅角色权限范围不符合预期")
		}
		seen[acl.Type] = true
	}
	if !seen["subscribePattern"] || !seen["publishClientReceive"] {
		return errors.New("订阅角色缺少必要 ACL")
	}
	return nil
}

func isNotFound(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "not found") || strings.Contains(message, "does not exist")
}

func commandFailure(action string, response dynsecResponse) error {
	// Broker 控制响应不直接返回，避免把可能含有敏感字段的 JSON 写入终端或日志。
	return fmt.Errorf("%s失败（Broker 返回 %s）", action, response.Command)
}

type unavailableTransport struct{ cause error }

func (t unavailableTransport) Execute(context.Context, []dynsecCommand) ([]dynsecResponse, error) {
	return nil, fmt.Errorf("%w：%v", ErrBrokerUnavailable, t.cause)
}

type mqttDynsecTransport struct{ config Config }

func (t *mqttDynsecTransport) Execute(ctx context.Context, commands []dynsecCommand) ([]dynsecResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.config.TLSConfig == nil || len(t.config.AdminPassword) == 0 {
		return nil, fmt.Errorf("%w：管理连接尚未配置", ErrBrokerUnavailable)
	}
	connectCtx, cancel := context.WithTimeout(ctx, t.config.DialTimeout)
	defer cancel()
	conn, err := tlsDial(connectCtx, t.config.Address, t.config.TLSConfig, t.config.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("%w：TLS 管理连接失败: %v", ErrBrokerUnavailable, err)
	}
	defer conn.Close()

	responseCh := make(chan []byte, 2)
	client := paho.NewClient(paho.ClientConfig{
		Conn:          packets.NewThreadSafeConn(conn),
		PacketTimeout: t.config.PacketTimeout,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(received paho.PublishReceived) (bool, error) {
				if received.Packet.Topic != dynsecResponseTopic {
					return false, nil
				}
				payload := append([]byte(nil), received.Packet.Payload...)
				select {
				case responseCh <- payload:
				default:
				}
				return true, nil
			},
		},
	})

	connectCtx, cancel = context.WithTimeout(ctx, t.config.PacketTimeout)
	defer cancel()
	clientID, err := randomClientID("project01-control-")
	if err != nil {
		return nil, fmt.Errorf("生成 Broker 管理连接编号失败: %w", err)
	}
	if _, err := client.Connect(connectCtx, &paho.Connect{
		ClientID:     clientID,
		Username:     t.config.AdminUsername,
		Password:     t.config.AdminPassword,
		UsernameFlag: true,
		PasswordFlag: true,
		CleanStart:   true,
		KeepAlive:    30,
	}); err != nil {
		return nil, fmt.Errorf("%w：MQTT 管理认证失败: %v", ErrBrokerUnavailable, err)
	}
	defer client.Disconnect(&paho.Disconnect{})

	subscribeCtx, cancel := context.WithTimeout(ctx, t.config.PacketTimeout)
	defer cancel()
	if _, err := client.Subscribe(subscribeCtx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{
		Topic: dynsecResponseTopic,
		QoS:   1,
	}}}); err != nil {
		return nil, fmt.Errorf("%w：订阅 Broker 管理响应失败: %v", ErrBrokerUnavailable, err)
	}

	requestPayload, err := json.Marshal(dynsecRequestEnvelope{Commands: commands})
	if err != nil {
		return nil, err
	}
	publishCtx, cancel := context.WithTimeout(ctx, t.config.PacketTimeout)
	defer cancel()
	if _, err := client.Publish(publishCtx, &paho.Publish{
		QoS:     1,
		Topic:   dynsecTopic,
		Payload: requestPayload,
		Retain:  false,
	}); err != nil {
		return nil, fmt.Errorf("%w：Broker 管理命令发送结果不确定: %v", device.ErrBrokerStateUncertain, err)
	}

	responseCtx, cancel := context.WithTimeout(ctx, t.config.PacketTimeout)
	defer cancel()
	select {
	case payload := <-responseCh:
		var envelope dynsecResponseEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			return nil, fmt.Errorf("%w：Broker 管理响应无法解析", device.ErrBrokerStateUncertain)
		}
		if len(envelope.Responses) != len(commands) {
			return nil, fmt.Errorf("%w：Broker 管理响应数量不匹配", device.ErrBrokerStateUncertain)
		}
		return envelope.Responses, nil
	case <-client.Done():
		return nil, fmt.Errorf("%w：等待 Broker 管理响应时连接断开", device.ErrBrokerStateUncertain)
	case <-responseCtx.Done():
		return nil, fmt.Errorf("%w：等待 Broker 管理响应超时", device.ErrBrokerStateUncertain)
	}
}

func tlsDial(ctx context.Context, address string, config *tls.Config, timeout time.Duration) (net.Conn, error) {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: timeout},
		Config:    config.Clone(),
	}
	return dialer.DialContext(ctx, "tcp", address)
}

func randomClientID(prefix string) (string, error) {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value), nil
}
