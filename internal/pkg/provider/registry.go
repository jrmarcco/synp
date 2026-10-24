package provider

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/xgrpc/register"
	"github.com/jrmarcco/xgrpc/register/etcd"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// GrpcRegistryFxModule 提供共享的 gRPC 服务注册器。
// 同时用于网关自身的服务注册与 SyncService 的客户端服务发现。
var GrpcRegistryFxModule = fx.Module(
	"grpc-registry",
	fx.Provide(newGrpcRegistry),
)

func newGrpcRegistry(etcdClient *clientv3.Client, lifecycle fx.Lifecycle, logger *zap.Logger) (register.Registry, error) {
	type config struct {
		KeyPrefix string        `mapstructure:"key_prefix"`
		LeaseTTL  int           `mapstructure:"lease_ttl"`
		Timeout   time.Duration `mapstructure:"timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.grpc.registry", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal grpc registry config: %w", err)
	}

	registry, err := etcd.NewBuilder(etcdClient).
		KeyPrefix(cfg.KeyPrefix).
		LeaseTTL(cfg.LeaseTTL).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc registry: %w", err)
	}

	lifecycle.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			return registry.Close()
		},
	})

	logger.Info("[grpc-registry] grpc registry created")
	return registry, nil
}
