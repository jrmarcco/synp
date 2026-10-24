package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/xgrpc/register"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var AppFxModule = fx.Module(
	"app",
	fx.Provide(newServiceInstance),
	fx.Invoke(startApp),
)

func newServiceInstance() (register.ServiceInstance, error) {
	type config struct {
		Name  string `mapstructure:"name"`
		Addr  string `mapstructure:"addr"`
		Group string `mapstructure:"group"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.grpc", &cfg); err != nil {
		return register.ServiceInstance{}, fmt.Errorf("failed to unmarshal grpc service instance config: %w", err)
	}

	return register.ServiceInstance{
		Name:  cfg.Name,
		Addr:  cfg.Addr,
		Group: cfg.Group,
	}, nil
}

type appFxParams struct {
	fx.In

	GrpcRegistry        register.Registry
	GrpcServer          *grpc.Server
	GrpcServiceInstance register.ServiceInstance

	WebSocketServer synp.Server

	Logger    *zap.Logger
	Lifecycle fx.Lifecycle
}

func startApp(params appFxParams) error {
	type config struct {
		Timeout time.Duration `mapstructure:"timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.grpc.registry", &cfg); err != nil {
		return fmt.Errorf("failed to unmarshal grpc registry config: %w", err)
	}

	app := &app{
		grpcServer:          params.GrpcServer,
		grpcServiceInstance: params.GrpcServiceInstance,

		grpcRegistry:        params.GrpcRegistry,
		grpcRegisterTimeout: cfg.Timeout,

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
	return nil
}
