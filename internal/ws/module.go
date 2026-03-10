package ws

import (
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/auth"
	"github.com/jrmarcco/synp/internal/pkg/compression"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var WsFxModule = fx.Module(
	"ws",
	fx.Provide(
		fx.Annotate(
			newWebSocketUpgrader,
			fx.As(new(synp.Upgrader)),
		),
		fx.Annotate(
			newWebSocketServer,
			fx.As(new(synp.Server)),
		),
	),
)

func newWebSocketUpgrader(rdb redis.Cmdable, validator auth.Validator, logger *zap.Logger) (*Upgrader, error) {
	type config struct {
		Enabled                 bool `mapstructure:"enabled"`
		ServerMaxWindowBits     int  `mapstructure:"server_max_window_bits"`
		ServerNoContextTakeover bool `mapstructure:"server_no_context_takeover"`
		ClientMaxWindowBits     int  `mapstructure:"client_max_window_bits"`
		ClientNoContextTakeover bool `mapstructure:"client_no_context_takeover"`
		Level                   int  `mapstructure:"level"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.websocket.compression", &cfg); err != nil {
		return nil, err
	}

	return NewUpgrader(rdb, validator, compression.Config{
		Enabled:                 cfg.Enabled,
		ServerMaxWindowBits:     cfg.ServerMaxWindowBits,
		ServerNoContextTakeover: cfg.ServerNoContextTakeover,
		ClientMaxWindowBits:     cfg.ClientMaxWindowBits,
		ClientNoContextTakeover: cfg.ClientNoContextTakeover,
		Level:                   cfg.Level,
	}, logger), nil
}

type serverFxParams struct {
	fx.In

	Upgrader    synp.Upgrader
	ConnManager synp.ConnManager
	ConnHandler synp.Handler

	Consumers map[string]*gateway.Consumer

	Logger *zap.Logger
}

func newWebSocketServer(params serverFxParams) (*Server, error) {
	type config struct {
		Host string `mapstructure:"host"`
		Port int    `mapstructure:"port"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.websocket", &cfg); err != nil {
		return nil, err
	}

	webSocketConfig := DefaultConfig()

	if cfg.Host != "" {
		webSocketConfig.Host = cfg.Host
	}
	if cfg.Port != 0 {
		webSocketConfig.Port = cfg.Port
	}

	webSocketServer := NewServer(
		webSocketConfig, params.Upgrader, params.ConnHandler, params.ConnManager, params.Consumers, params.Logger,
	)

	return webSocketServer, nil
}
