package app

import (
	"context"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/ws"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var AppFxModule = fx.Module("app", fx.Invoke(initApp))

type app struct {
	*grpc.Server
	wsServer synp.Server
}

func (app *app) Start() error {
	return app.wsServer.Start()
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
	}

	params.Lifecycle.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			return app.Start()
		},
		OnStop: func(_ context.Context) error {
			return app.Stop()
		},
	})

	return app
}
