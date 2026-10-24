package service

import (
	"go.uber.org/fx"

	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
)

var ServiceFxModule = fx.Module(
	"service",
	fx.Provide(
		fx.Annotate(
			NewDefaultNotifyService,
			fx.As(new(NotifyService)),
		),

		fx.Annotate(
			NewGrpcSyncCaller,
			fx.As(new(upstream.SyncCaller)),
		),
	),
)
