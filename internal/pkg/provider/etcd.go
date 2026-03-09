package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func newEtcdClient(logger *zap.Logger, lifecycle fx.Lifecycle) (*clientv3.Client, error) {
	type tlsConfig struct {
		Enabled bool `mapstructure:"enabled"`

		ServerName         string `mapstructure:"server_name"`
		InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`

		CertPem string `mapstructure:"cert_pem"`
		CertKey string `mapstructure:"cert_key"`
		CaCert  string `mapstructure:"ca_cert"`
	}

	type config struct {
		Endpoints []string `mapstructure:"endpoints"`

		Username    string        `mapstructure:"username"`
		Password    string        `mapstructure:"password"`
		DialTimeout time.Duration `mapstructure:"dial_timeout"`

		TLS tlsConfig `mapstructure:"tls"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("etcd", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal etcd config: %w", err)
	}

	etcdConfig := clientv3.Config{
		Endpoints:   cfg.Endpoints,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DialTimeout: cfg.DialTimeout,
	}

	// 配置 TLS。
	if cfg.TLS.Enabled {
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM([]byte(cfg.TLS.CaCert)) {
			return nil, fmt.Errorf("failed to append CA certificate to pool")
		}

		clientCert, err := tls.X509KeyPair([]byte(cfg.TLS.CertPem), []byte(cfg.TLS.CertKey))
		if err != nil {
			return nil, fmt.Errorf("failed to create certificate key pair: %w", err)
		}

		etcdTLSConfig := &tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
			RootCAs:            caCertPool,
			Certificates:       []tls.Certificate{clientCert},
		}

		if cfg.TLS.ServerName != "" {
			etcdTLSConfig.ServerName = cfg.TLS.ServerName
		}

		etcdConfig.TLS = etcdTLSConfig
	}

	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	// 测试 etcd 连接。
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if _, err := etcdClient.Get(ctx, "/synp/test"); err != nil {
		defer func() {
			_ = etcdClient.Close()
		}()
		return nil, fmt.Errorf("failed to ping etcd: %w", err)
	}

	// 注册 etcd 客户端 Close Hook。
	lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return etcdClient.Close()
		},
	})

	return etcdClient, nil
}
