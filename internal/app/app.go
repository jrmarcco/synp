package app

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/ws"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"github.com/jrmarcco/xgrpc/register"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var AppFxModule = fx.Module("app", fx.Invoke(initApp))

type app struct {
	*grpc.Server

	grpcRegistry        register.Registry
	grpcServiceInstance register.ServiceInstance
	grpcRegisterTimeout time.Duration

	wsServer synp.Server

	logger *zap.Logger
}

func (app *app) Start(ctx context.Context) error {
	// 启动 grpc 服务。
	lc := net.ListenConfig{}
	// FIXME: 这里不应该使用 fx OnStart 方法的 context。
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:5051")
	if err != nil {
		return err
	}

	go func() {
		if serveErr := app.Serve(ln); serveErr != nil {
			app.logger.Fatal("[app] failed to serve grpc", zap.Error(serveErr))
		}
	}()

	if app.grpcRegistry != nil {
		// 注册服务到注册中心。
		registerCtx, cancel := context.WithTimeout(ctx, app.grpcRegisterTimeout)
		registerErr := app.grpcRegistry.Register(registerCtx, app.grpcServiceInstance)
		cancel()

		if registerErr != nil {
			return fmt.Errorf("[app] failed to register grpc service: %w", registerErr)
		}
		app.logger.Info("[app] grpc service registered", zap.Any("service_instance", app.grpcServiceInstance))
	}

	wsStartErr := app.wsServer.Start()
	if wsStartErr != nil {
		return fmt.Errorf("[app] failed to start ws server: %w", wsStartErr)
	}

	app.logger.Info("[app] ws server started")
	return nil
}

func (app *app) Stop() error {
	return app.wsServer.GracefulShutdown()
}

type appFxParams struct {
	fx.In

	Upgrader    synp.Upgrader
	ConnManager synp.ConnManager
	ConnHandler synp.Handler

	Consumers map[string]*gateway.Consumer

	Logger    *zap.Logger
	Lifecycle fx.Lifecycle
}

func initApp(params appFxParams) *app {
	wsConfig := ws.DefaultConfig()
	wsServer := ws.NewServer(
		wsConfig, params.Upgrader, params.ConnHandler, params.ConnManager, params.Consumers, params.Logger,
	)

	app := &app{
		wsServer: wsServer,

		logger: params.Logger,
	}

	params.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return app.Start(ctx)
		},
		OnStop: func(_ context.Context) error {
			return app.Stop()
		},
	})

	return app
}
