package app

import (
	"context"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/ws"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var AppFxModule = fx.Module("app", fx.Invoke(startApp))

type appFxParams struct {
	fx.In

	Upgrader    synp.Upgrader
	ConnManager synp.ConnManager
	ConnHandler synp.Handler

	Consumers map[string]*gateway.Consumer

	Logger    *zap.Logger
	Lifecycle fx.Lifecycle
}

func startApp(params appFxParams) (*app, error) {
	webSocketConfig := ws.DefaultConfig()
	webSocketServer := ws.NewServer(
		webSocketConfig, params.Upgrader, params.ConnHandler, params.ConnManager, params.Consumers, params.Logger,
	)

	app := &app{
		webSocketServer: webSocketServer,

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
