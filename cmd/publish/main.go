package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"Project/internal/cli"
	"Project/internal/device"
	"Project/internal/receive"
	"Project/internal/telemetry"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "发布失败:", err)
		os.Exit(1)
	}
}

func run() error {
	address := flag.String("address", "localhost:8883", "MQTT Broker host:port")
	serverName := flag.String("server-name", "localhost", "TLS 证书中的 Broker 名称")
	caFile := flag.String("ca", ".secrets/mosquitto/ca.crt", "Broker CA PEM 文件")
	deviceID := flag.String("device", "", "设备编号")
	metricsArg := flag.String("metrics", "segment-1=23.6:V", "逗号分隔的部分指标，例如 segment-1=23.6:V,segment-2=0:kPa")
	modifiableArg := flag.String("modifiable", "", "逗号分隔的可修改指标 key，例如 segment-1；默认所有指标不可修改")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("不接受位置参数")
	}
	if err := validateDeviceID(*deviceID); err != nil {
		return err
	}
	metrics, err := parseMetrics(*metricsArg)
	if err != nil {
		return err
	}
	if err := applyModifiable(metrics, *modifiableArg); err != nil {
		return err
	}
	tlsConfig, err := receive.LoadTLSConfig(*caFile, *serverName)
	if err != nil {
		return err
	}
	secret, err := cli.NewTerminalSecretReader(os.Stderr).ReadSecret()
	if err != nil {
		return err
	}

	messageID, err := newUUID()
	if err != nil {
		return fmt.Errorf("生成 message_id 失败: %w", err)
	}
	payload, err := json.Marshal(telemetry.Message{
		Version:   "1",
		DeviceID:  *deviceID,
		MessageID: messageID,
		SampledAt: time.Now().UTC(),
		Metrics:   metrics,
	})
	if err != nil {
		return fmt.Errorf("编码遥测消息失败: %w", err)
	}
	if len(payload) > telemetry.MaxPayloadBytes {
		return fmt.Errorf("遥测载荷超过 %d 字节", telemetry.MaxPayloadBytes)
	}

	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: tlsConfig}
	dialCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	connection, err := dialer.DialContext(dialCtx, "tcp", *address)
	if err != nil {
		return fmt.Errorf("连接 MQTT Broker 失败: %w", err)
	}
	defer connection.Close()
	client := paho.NewClient(paho.ClientConfig{
		Conn:          packets.NewThreadSafeConn(connection),
		PacketTimeout: 5 * time.Second,
	})
	clientID, err := newUUID()
	if err != nil {
		return fmt.Errorf("生成 MQTT 客户端编号失败: %w", err)
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := client.Connect(connectCtx, &paho.Connect{
		ClientID:     "project01-sim-" + clientID,
		Username:     *deviceID,
		Password:     []byte(secret),
		UsernameFlag: true,
		PasswordFlag: true,
		CleanStart:   true,
		KeepAlive:    30,
	}); err != nil {
		return fmt.Errorf("MQTT 设备认证失败: %w", err)
	}
	defer client.Disconnect(&paho.Disconnect{})

	publishCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := client.Publish(publishCtx, &paho.Publish{
		QoS:     1,
		Topic:   "factory/" + *deviceID + "/telemetry",
		Payload: payload,
		Retain:  false,
	}); err != nil {
		return fmt.Errorf("MQTT QoS 1 发布失败: %w", err)
	}
	fmt.Printf("PUBACK 已收到 device_id=%s message_id=%s。该确认仅表示 Broker 确认了发布，不代表业务已入库。\n", *deviceID, messageID)
	return nil
}

func validateDeviceID(value string) error {
	if value == "" || len(value) > 64 || value == "admin" {
		return fmt.Errorf("-device 必须是 1 到 64 字节的安全设备编号，且不能为 admin")
	}
	for index, current := range value {
		allowed := current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' || current >= '0' && current <= '9' || current == '_' || current == '-'
		if !allowed || index == 0 && !(current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' || current >= '0' && current <= '9') {
			return fmt.Errorf("-device 只允许字母、数字、下划线和连字符，且必须以字母或数字开头")
		}
	}
	return nil
}

func parseMetrics(input string) (map[string]telemetry.MetricValue, error) {
	metrics := make(map[string]telemetry.MetricValue)
	for _, part := range strings.Split(input, ",") {
		name, valueAndUnit, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found || !validMetricKey(name) || valueAndUnit == "" {
			return nil, fmt.Errorf("-metrics 格式必须为 metric_key=value[:unit]")
		}
		if _, exists := metrics[name]; exists {
			return nil, fmt.Errorf("-metrics 中指标 %q 重复", name)
		}
		value, unit, hasUnit := strings.Cut(valueAndUnit, ":")
		if !hasUnit {
			return nil, fmt.Errorf("metric_key %q 必须显式提供 unit，格式为 %s=value:unit", name, name)
		}
		if unit == "" || len(unit) > 32 {
			return nil, fmt.Errorf("指标 %q 的 unit 必须为 1 到 32 字节", name)
		}
		numeric, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(numeric) || math.IsInf(numeric, 0) {
			return nil, fmt.Errorf("指标 %q 必须是有限数字", name)
		}
		metrics[name] = telemetry.MetricValue{Value: numeric, Unit: unit}
	}
	if len(metrics) == 0 {
		return nil, fmt.Errorf("至少提供一个指标")
	}
	return metrics, nil
}

func applyModifiable(metrics map[string]telemetry.MetricValue, input string) error {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	seen := make(map[string]bool)
	for _, rawKey := range strings.Split(input, ",") {
		key := strings.TrimSpace(rawKey)
		if !validMetricKey(key) {
			return fmt.Errorf("-modifiable 含无效指标 key %q", key)
		}
		if seen[key] {
			return fmt.Errorf("-modifiable 中指标 %q 重复", key)
		}
		metric, exists := metrics[key]
		if !exists {
			return fmt.Errorf("-modifiable 指标 %q 必须同时出现在 -metrics 中", key)
		}
		metric.SetModifiable(true)
		metrics[key] = metric
		seen[key] = true
	}
	return nil
}

func validMetricKey(key string) bool {
	_, ok := device.SegmentSlotIndex(key)
	return ok
}

func newUUID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
