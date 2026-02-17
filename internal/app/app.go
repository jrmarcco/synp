package app

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/ws"
	"github.com/jrmarcco/synp/internal/ws/gateway"
)

var AppFxModule = fx.Module("app", fx.Invoke(initApp))

type app struct {
	wsSvr synp.Server
}

func (app *app) Start() error {
	return app.wsSvr.Start()
}

func (app *app) Stop() error {
	return app.wsSvr.GracefulShutdown()
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
	wsCfg := ws.DefaultConfig()
	wsSvr := ws.NewServer(
		wsCfg, params.Upgrader, params.ConnHandler, params.ConnManager, params.Consumers, params.Logger,
	)

	app := &app{
		wsSvr: wsSvr,
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
