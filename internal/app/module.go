package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/xgrpc/register"
	"github.com/jrmarcco/xgrpc/register/etcd"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var AppFxModule = fx.Module("app", fx.Invoke(startApp))

type appFxParams struct {
	fx.In

	EtcdClient *clientv3.Client
	GrpcServer *grpc.Server

	WebSocketServer synp.Server

	Logger    *zap.Logger
	Lifecycle fx.Lifecycle
}

func startApp(params appFxParams) (*app, error) {
	type registryConfig struct {
		KeyPrefix string        `mapstructure:"key_prefix"`
		LeaseTTL  int           `mapstructure:"lease_ttl"`
		Timeout   time.Duration `mapstructure:"timeout"`
	}
	type config struct {
		Name  string `mapstructure:"name"`
		Addr  string `mapstructure:"addr"`
		Group string `mapstructure:"group"`

		Registry registryConfig `mapstructure:"registry"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.grpc", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal grpc registry config: %w", err)
	}

	grpcRegistry, err := etcd.NewBuilder(params.EtcdClient).
		KeyPrefix(cfg.Registry.KeyPrefix).
		LeaseTTL(cfg.Registry.LeaseTTL).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc registry: %w", err)
	}

	serviceInstance := register.ServiceInstance{
		Name:  cfg.Name,
		Addr:  cfg.Addr,
		Group: cfg.Group,
	}

	app := &app{
		grpcServer:          params.GrpcServer,
		grpcServiceInstance: serviceInstance,

		grpcRegistry:        grpcRegistry,
		grpcRegisterTimeout: cfg.Registry.Timeout,

		webSocketServer: params.WebSocketServer,

		logger: params.Logger,
	}

	params.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return app.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return app.Stop(ctx)
		},
	})

	return app, nil
}
