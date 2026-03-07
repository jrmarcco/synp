package service

import "go.uber.org/fx"

var ServiceFxModule = fx.Module(
	"service",
	fx.Provide(
		fx.Annotate(
			NewDefaultPushService,
			fx.As(new(PushService)),
		),
	),
)
