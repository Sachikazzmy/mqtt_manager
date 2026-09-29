package receive

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const DefaultQueueSize = 128

var ErrBrokerUnavailable = errors.New("MQTT Broker 当前不可用")

type Config struct {
	Address          string
	ServerName       string
	CAFile           string
	AdminUsername    string
	AdminPassword    []byte
	ReceiverUsername string
	ReceiverPassword []byte
	QueueSize        int
	DialTimeout      time.Duration
	PacketTimeout    time.Duration
	ReconnectDelay   time.Duration
	SessionExpiry    time.Duration
	TLSConfig        *tls.Config
	ReadinessFile    string
}

func LoadConfigFromEnv() (Config, error) {
	config := Config{
		Address:          envOr("MQTT_ADDRESS", "localhost:8883"),
		ServerName:       envOr("MQTT_SERVER_NAME", "localhost"),
		CAFile:           envOr("MQTT_CA_FILE", ".secrets/mosquitto/ca.crt"),
		AdminUsername:    envOr("MQTT_ADMIN_USERNAME", "admin"),
		ReceiverUsername: envOr("MQTT_RECEIVER_USERNAME", "__project01_receiver__"),
		QueueSize:        DefaultQueueSize,
		DialTimeout:      5 * time.Second,
		PacketTimeout:    5 * time.Second,
		ReconnectDelay:   2 * time.Second,
		SessionExpiry:    24 * time.Hour,
		ReadinessFile:    os.Getenv("MQTT_READINESS_FILE"),
	}
	if value := os.Getenv("MQTT_QUEUE_SIZE"); value != "" {
		queueSize, err := strconv.Atoi(value)
		if err != nil || queueSize < 1 || queueSize > 4096 {
			return Config{}, fmt.Errorf("MQTT_QUEUE_SIZE 必须为 1 到 4096 之间的整数")
		}
		config.QueueSize = queueSize
	}

	adminPasswordFile := envOr("MQTT_ADMIN_PASSWORD_FILE", ".secrets/dynsec_admin_password")
	var err error
	config.AdminPassword, err = readSecretFile(adminPasswordFile)
	if err != nil {
		return Config{}, fmt.Errorf("读取 Broker 管理凭据失败: %w", err)
	}
	receiverPasswordFile := envOr("MQTT_RECEIVER_PASSWORD_FILE", ".secrets/receiver_password")
	config.ReceiverPassword, err = readSecretFile(receiverPasswordFile)
	if err != nil {
		return Config{}, fmt.Errorf("读取 Broker 订阅凭据失败: %w", err)
	}
	config.TLSConfig, err = loadTLSConfig(config.CAFile, config.ServerName)
	if err != nil {
		return Config{}, err
	}
	if _, _, err := net.SplitHostPort(config.Address); err != nil {
		return Config{}, fmt.Errorf("MQTT_ADDRESS 必须使用 host:port 格式: %w", err)
	}
	return config, nil
}

func loadTLSConfig(caFile, serverName string) (*tls.Config, error) {
	if strings.TrimSpace(serverName) == "" {
		return nil, fmt.Errorf("Broker TLS server name 不能为空")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("读取 Broker CA 证书失败: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("Broker CA 文件不含可用的 PEM 证书")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: serverName,
	}, nil
}

func LoadTLSConfig(caFile, serverName string) (*tls.Config, error) {
	return loadTLSConfig(caFile, serverName)
}

func readSecretFile(path string) ([]byte, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	value = []byte(strings.TrimRight(string(value), "\r\n"))
	if len(value) == 0 {
		return nil, fmt.Errorf("凭据文件为空")
	}
	return value, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
