package app

import (
	"context"
	"fmt"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/ws"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"github.com/jrmarcco/xgrpc/register/etcd"
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

	Upgrader    synp.Upgrader
	ConnManager synp.ConnManager
	ConnHandler synp.Handler

	Consumers map[string]*gateway.Consumer

	Logger    *zap.Logger
	Lifecycle fx.Lifecycle
}

func startApp(params appFxParams) (*app, error) {
	// 配置 grpc server。
	grpcRegistry, err := etcd.NewBuilder(params.EtcdClient).
		// KeyPrefix("").
		// LeaseTTL().
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc registry: %w", err)
	}

	// 配置 web socket server。
	webSocketConfig := ws.DefaultConfig()
	webSocketServer := ws.NewServer(
		webSocketConfig, params.Upgrader, params.ConnHandler, params.ConnManager, params.Consumers, params.Logger,
	)

	app := &app{
		webSocketServer: webSocketServer,

		grpcServer:   params.GrpcServer,
		grpcRegistry: grpcRegistry,

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
