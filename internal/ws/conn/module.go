package conn

import (
	"time"

	"github.com/jrmarcco/synp"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var ConnFxModule = fx.Module(
	"ws-conn",
	fx.Provide(
		fx.Annotate(
			newConnRegistry,
			fx.As(new(synp.ConnRegistry)),
		),
	),
	fx.Provide(
		fx.Annotate(
			newConnManager,
			fx.As(new(synp.ConnManager)),
		),
	),
)

func newConnRegistry(rdb redis.Cmdable, logger *zap.Logger) (*ConnRegistry, error) {
	type config struct {
		Node      string        `mapstructure:"node"`
		KeyPrefix string        `mapstructure:"key_prefix"`
		TTL       time.Duration `mapstructure:"ttl"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.conn.registry", &cfg); err != nil {
		return nil, err
	}

	return NewConnRegistry(
		rdb, cfg.Node, logger,
		ConnRegistryWithTTL(cfg.TTL),
		ConnRegistryWithKeyPrefix(cfg.KeyPrefix),
	), nil
}

func newConnManager(zapLogger *zap.Logger) (*ConnManager, error) {
	type config = struct {
		ReadTimeout  time.Duration `mapstructure:"read_timeout"`
		WriteTimeout time.Duration `mapstructure:"write_timeout"`

		InitRetryInterval time.Duration `mapstructure:"init_retry_interval"`
		MaxRetryInterval  time.Duration `mapstructure:"max_retry_interval"`
		MaxRetryCount     int32         `mapstructure:"max_retry_count"`

		SendBufferSize    int `mapstructure:"send_buffer_size"`
		ReceiveBufferSize int `mapstructure:"receive_buffer_size"`

		CloseTimeout time.Duration `mapstructure:"close_timeout"`
		RateLimit    int           `mapstructure:"rate_limit"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.conn.manager", &cfg); err != nil {
		return nil, err
	}

	return NewConnManager(zapLogger, ConnManagerWithConfig(&ConnConfig{
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		InitRetryInterval: cfg.InitRetryInterval,
		MaxRetryInterval:  cfg.MaxRetryInterval,
		MaxRetryCount:     cfg.MaxRetryCount,
		SendBufferSize:    cfg.SendBufferSize,
		ReceiveBufferSize: cfg.ReceiveBufferSize,
		CloseTimeout:      cfg.CloseTimeout,
		RateLimit:         cfg.RateLimit,
	})), nil
}
