package receive

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"Project/internal/command"
	"Project/internal/device"
	"Project/internal/telemetry"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

const (
	telemetrySubscription = "factory/+/telemetry"
	resultSubscription    = "factory/+/command_result"
)

type Event struct {
	Type          string
	Status        string
	DeviceID      string
	MessageID     string
	CommandID     string
	CommandStatus string
	Metrics       map[string]telemetry.MetricValue
	Reason        string
}

type delivery struct {
	packet     *paho.Publish
	ack        func() error
	disconnect func()
}

type Receiver struct {
	config    Config
	telemetry *telemetry.Service
	commands  *command.Service
	queue     chan delivery
	events    chan Event
	reconnect chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	clientMu  sync.RWMutex
	client    *paho.Client
	cancel    context.CancelFunc
	done      chan struct{}
	dropped   atomic.Uint64
}

func NewReceiver(config Config, telemetryService *telemetry.Service, commandServices ...*command.Service) (*Receiver, error) {
	if config.QueueSize < 1 || config.QueueSize > 4096 {
		return nil, fmt.Errorf("MQTT queue size 必须为 1 到 4096")
	}
	if telemetryService == nil {
		return nil, fmt.Errorf("telemetry service 不能为空")
	}
	var commandService *command.Service
	if len(commandServices) > 0 {
		commandService = commandServices[0]
	}
	return &Receiver{
		config:    config,
		telemetry: telemetryService,
		commands:  commandService,
		queue:     make(chan delivery, config.QueueSize),
		events:    make(chan Event, config.QueueSize),
		reconnect: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}, nil
}

func (r *Receiver) Events() <-chan Event {
	return r.events
}

func (r *Receiver) Start(parent context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	go r.run(ctx)
}

func (r *Receiver) Close() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel == nil {
		return
	}
	r.closeOnce.Do(cancel)
	<-r.done
}

func (r *Receiver) DroppedEvents() uint64 {
	return r.dropped.Swap(0)
}

func (r *Receiver) run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		r.process(ctx)
	}()
	go func() {
		defer workers.Done()
		r.supervise(ctx)
	}()
	workers.Wait()
	r.setReadiness(false)
	close(r.events)
	close(r.done)
}

func (r *Receiver) supervise(ctx context.Context) {
	defer func() {
		if ctx.Err() == nil {
			r.emit(Event{Type: "broker", Status: "offline", Reason: "MQTT 接收器已停止"})
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		// 旧连接的 Done 与重连请求可能同时就绪；清除旧请求，避免新连接刚建立就被关闭。
		select {
		case <-r.reconnect:
		default:
		}
		r.emit(Event{Type: "broker", Status: "connecting"})
		client, err := r.connect(ctx)
		if err != nil {
			r.emit(Event{Type: "broker", Status: "offline", Reason: "连接失败，稍后重试"})
			if !waitContext(ctx, r.config.ReconnectDelay) {
				return
			}
			continue
		}

		subscribeCtx, cancel := context.WithTimeout(ctx, r.config.PacketTimeout)
		suback, err := client.Subscribe(subscribeCtx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{
			Topic:             telemetrySubscription,
			QoS:               1,
			RetainAsPublished: true,
			RetainHandling:    0,
		}, {
			Topic:             resultSubscription,
			QoS:               1,
			RetainAsPublished: true,
			RetainHandling:    0,
		}}})
		cancel()
		if err == nil && (suback == nil || len(suback.Reasons) != 2 || suback.Reasons[0] >= 0x80 || suback.Reasons[1] >= 0x80) {
			err = fmt.Errorf("Broker 未接受遥测和命令结果订阅")
		}
		if err != nil {
			r.emit(Event{Type: "broker", Status: "offline", Reason: "订阅失败，稍后重试"})
			_ = client.Disconnect(&paho.Disconnect{})
			if !waitContext(ctx, r.config.ReconnectDelay) {
				return
			}
			continue
		}

		r.setClient(client)
		r.emit(Event{Type: "broker", Status: "online", Reason: "遥测与命令结果订阅均已确认"})
		select {
		case <-ctx.Done():
			r.clearClient(client)
			_ = client.Disconnect(&paho.Disconnect{})
			return
		case <-client.Done():
		case <-r.reconnect:
			r.clearClient(client)
			_ = client.Disconnect(&paho.Disconnect{})
		}
		r.clearClient(client)
		r.emit(Event{Type: "broker", Status: "offline", Reason: "连接中断，稍后恢复订阅"})
		if !waitContext(ctx, r.config.ReconnectDelay) {
			return
		}
	}
}

func (r *Receiver) connect(ctx context.Context) (*paho.Client, error) {
	if r.config.TLSConfig == nil || len(r.config.ReceiverPassword) == 0 {
		return nil, fmt.Errorf("MQTT TLS 或订阅凭据未配置")
	}
	connectCtx, cancel := context.WithTimeout(ctx, r.config.DialTimeout)
	defer cancel()
	dialer := &net.Dialer{Timeout: r.config.DialTimeout}
	rawConn, err := dialer.DialContext(connectCtx, "tcp", r.config.Address)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(rawConn, r.config.TLSConfig.Clone())
	if err := tlsConn.HandshakeContext(connectCtx); err != nil {
		_ = rawConn.Close()
		return nil, err
	}

	client := paho.NewClient(paho.ClientConfig{
		Conn:                       packets.NewThreadSafeConn(tlsConn),
		PacketTimeout:              r.config.PacketTimeout,
		EnableManualAcknowledgment: true,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(received paho.PublishReceived) (bool, error) {
				if received.Packet.QoS != 1 {
					r.emit(Event{Type: "reject", DeviceID: displayDeviceID(received.Packet.Topic), Reason: "只接受 QoS 1 上报"})
					return true, nil
				}
				item := delivery{
					packet: received.Packet,
					ack:    func() error { return received.Client.Ack(received.Packet) },
					disconnect: func() {
						_ = received.Client.Disconnect(&paho.Disconnect{})
					},
				}
				r.enqueue(item)
				return true, nil
			},
		},
	})

	sessionExpiry := uint32(r.config.SessionExpiry / time.Second)
	receiveMaximum := uint16(r.config.QueueSize)
	if r.config.QueueSize > 65535 {
		receiveMaximum = 65535
	}
	properties := &paho.ConnectProperties{
		SessionExpiryInterval: &sessionExpiry,
		ReceiveMaximum:        &receiveMaximum,
	}
	connectPacketCtx, cancel := context.WithTimeout(ctx, r.config.PacketTimeout)
	defer cancel()
	if _, err := client.Connect(connectPacketCtx, &paho.Connect{
		ClientID:     "project01-receiver",
		Username:     r.config.ReceiverUsername,
		Password:     r.config.ReceiverPassword,
		UsernameFlag: true,
		PasswordFlag: true,
		CleanStart:   false,
		KeepAlive:    30,
		Properties:   properties,
	}); err != nil {
		_ = tlsConn.Close()
		return nil, err
	}
	return client, nil
}

func (r *Receiver) process(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-r.queue:
			r.processDelivery(ctx, item)
		}
	}
}

func (r *Receiver) processDelivery(ctx context.Context, item delivery) {
	packet := item.packet
	deviceID := displayDeviceID(packet.Topic)
	if ctx.Err() != nil {
		return
	}
	if packet.Retain {
		r.emit(Event{Type: "reject", DeviceID: deviceID, Reason: "遗留 retained 消息已拒收"})
		_ = item.ack()
		return
	}

	if !strings.HasPrefix(packet.Topic, "factory/") {
		r.rejectAndAck(item, deviceID, "Topic 不属于 factory/{device_id}/telemetry 或 command_result")
		return
	}
	parts := strings.Split(packet.Topic, "/")
	if len(parts) != 3 {
		r.rejectAndAck(item, deviceID, "Topic 格式无效")
		return
	}
	if parts[2] == command.ResultTopicSuffix {
		r.processCommandResult(ctx, item, deviceID)
		return
	}
	if parts[2] != "telemetry" {
		r.rejectAndAck(item, deviceID, "Topic 不属于订阅的业务消息")
		return
	}
	result, err := r.telemetry.ReceiveFromBroker(ctx, packet.Topic, packet.Payload)
	switch {
	case err == nil:
		r.emit(Event{Type: "accepted", DeviceID: result.DeviceID, MessageID: result.MessageID, Metrics: result.Metrics})
		if ackErr := item.ack(); ackErr != nil {
			r.emit(Event{Type: "notice", DeviceID: result.DeviceID, MessageID: result.MessageID, Reason: "数据已提交但 MQTT 确认未排队；断线后若重投会按 message_id 去重"})
			r.requestReconnect()
		}
	case errors.Is(err, telemetry.ErrDuplicateMessage):
		r.emit(Event{Type: "duplicate", DeviceID: result.DeviceID, MessageID: result.MessageID, Metrics: result.Metrics})
		if ackErr := item.ack(); ackErr != nil {
			r.requestReconnect()
		}
	case isPermanentRejection(err):
		r.emit(Event{Type: "reject", DeviceID: firstNonEmpty(result.DeviceID, deviceID), MessageID: result.MessageID, Reason: err.Error()})
		if ackErr := item.ack(); ackErr != nil {
			r.requestReconnect()
		}
	default:
		reason := "存储处理暂时失败；消息未确认，正在断开连接以触发重投"
		if err != nil {
			reason += ": " + err.Error()
		}
		r.emit(Event{Type: "retry", DeviceID: firstNonEmpty(result.DeviceID, deviceID), MessageID: result.MessageID, Reason: reason})
		item.disconnect()
		r.requestReconnect()
	}
}

func (r *Receiver) processCommandResult(ctx context.Context, item delivery, topicDeviceID string) {
	if r.commands == nil {
		r.rejectAndAck(item, topicDeviceID, "命令服务未配置")
		return
	}
	disposition, err := r.commands.ProcessResultFromBroker(ctx, item.packet.Topic, item.packet.Payload)
	if err != nil {
		if errors.Is(err, command.ErrInvalidResult) {
			r.rejectAndAck(item, topicDeviceID, err.Error())
			return
		}
		r.emit(Event{Type: "retry", DeviceID: topicDeviceID, Reason: "命令结果提交失败；消息未确认并将重投: " + err.Error()})
		item.disconnect()
		r.requestReconnect()
		return
	}
	if disposition.Anomaly != "" {
		r.emit(Event{Type: "reject", DeviceID: topicDeviceID, Reason: disposition.Anomaly})
	} else if disposition.Command != nil {
		r.emit(Event{Type: "command_result", DeviceID: topicDeviceID, CommandID: disposition.Command.CommandID, CommandStatus: disposition.Command.Status})
	}
	if ackErr := item.ack(); ackErr != nil {
		r.requestReconnect()
	}
}

func (r *Receiver) rejectAndAck(item delivery, deviceID, reason string) {
	r.emit(Event{Type: "reject", DeviceID: deviceID, Reason: reason})
	if err := item.ack(); err != nil {
		r.requestReconnect()
	}
}

// PublishCommand intentionally accepts a device identity, not an arbitrary
// topic, so the outbox can only publish under that device's command namespace.
func (r *Receiver) PublishCommand(ctx context.Context, deviceID string, payload []byte) error {
	if !validDeviceTopicID(deviceID) {
		return fmt.Errorf("命令设备编号无效")
	}
	r.clientMu.RLock()
	client := r.client
	r.clientMu.RUnlock()
	if client == nil {
		return fmt.Errorf("MQTT 命令发布连接尚未就绪")
	}
	select {
	case <-client.Done():
		return fmt.Errorf("MQTT 命令发布连接已断开")
	default:
	}
	response, err := client.Publish(ctx, &paho.Publish{
		Topic: "factory/" + deviceID + "/" + command.CommandTopicSuffix,
		QoS:   1, Retain: false, Payload: payload,
	})
	if err != nil {
		return err
	}
	if response == nil || response.ReasonCode >= 0x80 {
		return fmt.Errorf("Broker 未确认命令发布")
	}
	return nil
}

func validDeviceTopicID(id string) bool {
	if id == "" || len(id) > 64 || id == "admin" {
		return false
	}
	for index, char := range id {
		allowed := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-'
		if !allowed || index == 0 && !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func (r *Receiver) setClient(client *paho.Client) {
	r.clientMu.Lock()
	r.client = client
	r.clientMu.Unlock()
}

func (r *Receiver) clearClient(client *paho.Client) {
	r.clientMu.Lock()
	if r.client == client {
		r.client = nil
	}
	r.clientMu.Unlock()
}

func isPermanentRejection(err error) bool {
	// ErrDeviceTransition is intentionally omitted: pending lifecycle operations
	// must leave QoS 1 deliveries unacknowledged until the database state settles.
	return errors.Is(err, telemetry.ErrPayloadTooLarge) ||
		errors.Is(err, telemetry.ErrInvalidMessage) ||
		errors.Is(err, telemetry.ErrInvalidTopic) ||
		errors.Is(err, telemetry.ErrDeviceMismatch) ||
		errors.Is(err, telemetry.ErrDeviceDisabled) ||
		errors.Is(err, telemetry.ErrInvalidSecret) ||
		errors.Is(err, device.ErrNotFound) ||
		errors.Is(err, command.ErrInvalidResult)
}

func (r *Receiver) requestReconnect() {
	select {
	case r.reconnect <- struct{}{}:
	default:
	}
}

func (r *Receiver) enqueue(item delivery) bool {
	select {
	case r.queue <- item:
		return true
	default:
		r.emit(Event{
			Type:     "queue_full",
			DeviceID: displayDeviceID(item.packet.Topic),
			Reason:   "处理队列已满；消息未确认，正在断开连接以触发重投",
		})
		r.requestReconnect()
		return false
	}
}

func (r *Receiver) emit(event Event) {
	if event.Type == "broker" {
		r.setReadiness(event.Status == "online")
	}
	select {
	case r.events <- event:
	default:
		r.dropped.Add(1)
	}
}

// The marker reflects a successful subscription, not just a reachable TCP port.
func (r *Receiver) setReadiness(online bool) {
	if r.config.ReadinessFile == "" {
		return
	}
	var err error
	if online {
		err = os.WriteFile(r.config.ReadinessFile, []byte("ready\n"), 0600)
	} else {
		err = os.Remove(r.config.ReadinessFile)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	}
	if err != nil {
		r.emit(Event{Type: "notice", Reason: "无法更新 MQTT 健康状态文件"})
	}
}

func displayDeviceID(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) == 3 && parts[0] == "factory" && (parts[2] == "telemetry" || parts[2] == command.ResultTopicSuffix) {
		return parts[1]
	}
	return "-"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return "-"
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
