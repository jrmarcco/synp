package provider

import (
	"context"
	"log/slog"

	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
)

func newLogger(lifecycle fx.Lifecycle) (*zap.Logger, error) {
	type config struct {
		Env string `mapstructure:"env"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("project", &cfg); err != nil {
		return nil, err
	}

	var err error
	var logger *zap.Logger

	switch cfg.Env {
	case "prod":
		logger, err = zap.NewProduction()
	default:
		logger, err = zap.NewDevelopment()
	}

	if err != nil {
		return nil, err
	}

	// 初始化 slog。
	slog.SetDefault(slog.New(zapslog.NewHandler(logger.Core())))

	lifecycle.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			_ = logger.Sync()
			return nil
		},
	})

	return logger, nil
}
