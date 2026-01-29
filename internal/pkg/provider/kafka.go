package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsume"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type kafkaFxResult struct {
	fx.Out

	Writer           *kafka.Writer
	ReaderCreateFunc xconsume.KafkaReaderCreateFunc
}

type kafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`

	Producer kafkaProducerConfig `mapstructure:"producer"`
	Consumer kafkaConsumerConfig `mapstructure:"consumer"`

	TLS  kafkaTLSConfig  `mapstructure:"tls"`
	SASL kafkaSaslConfig `mapstructure:"sasl"`
}

type kafkaProducerConfig struct {
	// Producer 配置
	RequiredAcks      int           `mapstructure:"required_acks"`      // -1=all, 0=none, 1=leader
	Compression       string        `mapstructure:"compression"`        // none, gzip, snappy, lz4, zstd
	MaxMessageBytes   int           `mapstructure:"max_message_bytes"`  // 最大消息大小，默认 1MB
	RetryMax          int           `mapstructure:"retry_max"`          // 最大重试次数
	BatchSize         int           `mapstructure:"batch_size"`         // 批量大小
	BatchTimeout      time.Duration `mapstructure:"batch_timeout"`      // 批量超时时间
	WriteTimeout      time.Duration `mapstructure:"write_timeout"`      // 写入超时
	IdempotentEnabled bool          `mapstructure:"idempotent_enabled"` // 是否启用幂等性
}

type kafkaConsumerConfig struct {
	// Consumer 配置
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`    // 读取超时
	CommitInterval time.Duration `mapstructure:"commit_interval"` // 提交间隔
	StartOffset    int64         `mapstructure:"start_offset"`    // 起始 offset，-1=newest, -2=oldest
	MinBytes       int           `mapstructure:"min_bytes"`       // 最小字节数
	MaxBytes       int           `mapstructure:"max_bytes"`       // 最大字节数
	MaxWait        time.Duration `mapstructure:"max_wait"`        // 最大等待时间
}

type kafkaTLSConfig struct {
	CaCert  string `mapstructure:"ca_cert"`
	CertKey string `mapstructure:"cert_key"`
	CertPem string `mapstructure:"cert_pem"`
}

type kafkaSaslConfig struct {
	Mechanism string `mapstructure:"mechanism"`

	// SCRAM 认证配置
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`

	// OAuth 认证配置
	OAuth kafkaOAuthConfig `mapstructure:"oauth"`
}

type kafkaOAuthConfig struct {
	Endpoint     string `mapstructure:"endpoint"`
	ClientID     string `mapstructure:"client_id"`
	ClientSecret string `mapstructure:"client_secret"`
}

func newKafkaClient(zapLogger *zap.Logger, lifecycle fx.Lifecycle) (kafkaFxResult, error) {
	cfg, err := loadKafkaConfig()
	if err != nil {
		return kafkaFxResult{}, err
	}

	// 配置 TLS ( 强制要求启用 TLS )。
	tlsConfig, err := configureKafkaTLS(cfg.TLS, zapLogger)
	if err != nil {
		return kafkaFxResult{}, err
	}

	// 配置 Kafka Dialer。
	const defaultDialTimeout = 10 * time.Second
	dialer := &kafka.Dialer{
		Timeout:   defaultDialTimeout,
		DualStack: true,
		TLS:       tlsConfig,
	}

	// 配置 Kafka Transport。
	transport := &kafka.Transport{
		TLS: tlsConfig,
	}

	if cfg.SASL.Mechanism != "" {
		// 启用 SASL 认证。
		saslMechanism, err := configureKafkaSasl(cfg.SASL, zapLogger)
		if err != nil {
			return kafkaFxResult{}, err
		}
		dialer.SASLMechanism = saslMechanism
		transport.SASL = saslMechanism
	}

	// 创建 Writer ( Producer )。
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.Hash{}, // 使用 Hash 负载均衡
		Compression:  getKafkaCompression(cfg.Producer.Compression),
		MaxAttempts:  cfg.Producer.RetryMax,
		BatchSize:    cfg.Producer.BatchSize,
		BatchTimeout: cfg.Producer.BatchTimeout,
		ReadTimeout:  cfg.Consumer.ReadTimeout, // 从 broker 读取响应的超时
		WriteTimeout: cfg.Producer.WriteTimeout,
		RequiredAcks: getKafkaRequiredAcks(cfg.Producer.RequiredAcks),
		Async:        false, // 同步模式
		Transport:    transport,
	}

	// 如果启用幂等性，设置为精确一次语义。
	if cfg.Producer.IdempotentEnabled {
		writer.RequiredAcks = kafka.RequireAll
	}
	zapLogger.Info(
		"[ioc-kafka] successfully created kafka writer",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("compression", cfg.Producer.Compression),
		zap.Int("required_acks", cfg.Producer.RequiredAcks),
		zap.Bool("idempotent", cfg.Producer.IdempotentEnabled),
	)

	// 创建 ReaderFactory，用于按需创建 Reader ( Consumer )。
	const defaultReadBackoffMin = 100 * time.Millisecond
	const defaultReadBackoffMax = 1 * time.Second
	readerFactory := func(topic string, groupId string) *kafka.Reader {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			Topic:          topic,
			GroupID:        groupId,
			MinBytes:       cfg.Consumer.MinBytes,
			MaxBytes:       cfg.Consumer.MaxBytes,
			MaxWait:        cfg.Consumer.MaxWait,
			ReadBackoffMin: defaultReadBackoffMin,
			ReadBackoffMax: defaultReadBackoffMax,
			CommitInterval: cfg.Consumer.CommitInterval,
			StartOffset:    cfg.Consumer.StartOffset,
			Dialer:         dialer,
		})

		zapLogger.Info(
			"[ioc-kafka] created kafka reader",
			zap.Strings("brokers", cfg.Brokers),
			zap.String("topic", topic),
			zap.String("group_id", groupId),
		)

		return reader
	}

	// 注册生命周期 Hook。
	lifecycle.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			if err := writer.Close(); err != nil {
				zapLogger.Error("[ioc-kafka] failed to close kafka writer", zap.Error(err))
				return fmt.Errorf("failed to close kafka writer: %w", err)
			}
			zapLogger.Info("[ioc-kafka] kafka writer closed")
			return nil
		},
	})

	return kafkaFxResult{
		Writer:           writer,
		ReaderCreateFunc: readerFactory,
	}, nil
}

// loadKafkaConfig 加载 Kafka 配置。
func loadKafkaConfig() (*kafkaConfig, error) {
	cfg := &kafkaConfig{}
	if err := viper.UnmarshalKey("kafka", cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal kafka config: %w", err)
	}
	return cfg, nil
}

// configureKafkaTLS 配置 TLS。
func configureKafkaTLS(tlsCfg kafkaTLSConfig, logger *zap.Logger) (*tls.Config, error) {
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM([]byte(tlsCfg.CaCert)); !ok {
		logger.Error("[ioc-kafka] failed to append CA certificate to pool for kafka")
		return nil, fmt.Errorf("failed to append CA certificate to pool for kafka")
	}

	clientCert, err := tls.X509KeyPair([]byte(tlsCfg.CertPem), []byte(tlsCfg.CertKey))
	if err != nil {
		logger.Error("[ioc-kafka] failed to create certificate key pair for kafka", zap.Error(err))
		return nil, fmt.Errorf("failed to create certificate key pair for kafka: %w", err)
	}

	logger.Info("[ioc-kafka] successfully configured TLS for kafka")
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: false, // 强制要求 TLS 认证
		RootCAs:            caCertPool,
		Certificates:       []tls.Certificate{clientCert},
	}, nil
}

// configureKafkaSasl 配置 SASL 认证机制。
func configureKafkaSasl(saslCfg kafkaSaslConfig, logger *zap.Logger) (sasl.Mechanism, error) {
	mechanism := saslCfg.Mechanism
	switch mechanism {
	case "scram":
		return configureKafkaSaslScram(saslCfg, logger)
	case "oauth":
		return configureKafkaSaslOAuth(saslCfg.OAuth, logger)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

// configureKafkaSaslScram 配置 SASL/SCRAM-SHA-256 认证。
func configureKafkaSaslScram(saslCfg kafkaSaslConfig, logger *zap.Logger) (sasl.Mechanism, error) {
	if saslCfg.Username == "" || saslCfg.Password == "" {
		return nil, errors.New("username and password are required for SCRAM authentication")
	}

	mechanism, err := scram.Mechanism(scram.SHA256, saslCfg.Username, saslCfg.Password)
	if err != nil {
		logger.Error(
			"[ioc-kafka] failed to create SASL/SCRAM mechanism",
			zap.String("username", saslCfg.Username),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to create SASL/SCRAM mechanism: %w", err)
	}

	logger.Info(
		"[ioc-kafka] successfully configured SASL/SCRAM-SHA-256 for kafka",
		zap.String("username", saslCfg.Username),
	)

	return mechanism, nil
}

// configureKafkaSaslOAuth 配置 SASL/OAUTHBEARER 认证。
func configureKafkaSaslOAuth(oauthCfg kafkaOAuthConfig, logger *zap.Logger) (sasl.Mechanism, error) {
	if oauthCfg.Endpoint == "" || oauthCfg.ClientID == "" || oauthCfg.ClientSecret == "" {
		return nil, errors.New("token_endpoint, client_id and client_secret are required for OAuth authentication")
	}

	mechanism := NewKafkaOAuthMechanism(
		oauthCfg.Endpoint,
		oauthCfg.ClientID,
		oauthCfg.ClientSecret,
	)

	logger.Info(
		"[ioc-kafka] successfully configured SASL/OAUTHBEARER for kafka",
		zap.String("token_endpoint", oauthCfg.Endpoint),
		zap.String("client_id", oauthCfg.ClientID),
	)

	return mechanism, nil
}

// getKafkaCompression 获取压缩算法。
func getKafkaCompression(compression string) kafka.Compression {
	switch compression {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return kafka.Compression(0) // none
	}
}

// getKafkaRequiredAcks 获取 RequiredAcks 配置。
func getKafkaRequiredAcks(acks int) kafka.RequiredAcks {
	switch acks {
	case -1:
		return kafka.RequireAll
	case 0:
		return kafka.RequireNone
	case 1:
		return kafka.RequireOne
	default:
		return kafka.RequireAll
	}
}
