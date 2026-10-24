package conn

import (
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/xgrpc/register"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/fx"
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

func newConnRegistry(rdb redis.Cmdable, si register.ServiceInstance) (*ConnRegistry, error) {
	type config struct {
		KeyPrefix string        `mapstructure:"key_prefix"`
		TTL       time.Duration `mapstructure:"ttl"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.conn.registry", &cfg); err != nil {
		return nil, err
	}

	return NewConnRegistry(
		rdb,
		synp.ConnNode{
			ServiceName: si.Name,
			ServiceAddr: si.Addr,
		},
		ConnRegistryWithTTL(cfg.TTL),
		ConnRegistryWithKeyPrefix(cfg.KeyPrefix),
	), nil
}

func newConnManager() (*ConnManager, error) {
	type config = struct {
		ReadTimeout  time.Duration `mapstructure:"read_timeout"`
		WriteTimeout time.Duration `mapstructure:"write_timeout"`

		EstablishedReadTimeout time.Duration `mapstructure:"established_read_timeout"`

		InitRetryInterval time.Duration `mapstructure:"init_retry_interval"`
		MaxRetryInterval  time.Duration `mapstructure:"max_retry_interval"`
		MaxRetryCount     int32         `mapstructure:"max_retry_count"`

		SendBufferSize    int `mapstructure:"send_buffer_size"`
		ReceiveBufferSize int `mapstructure:"receive_buffer_size"`

		CloseTimeout time.Duration `mapstructure:"close_timeout"`
		RateLimit    int           `mapstructure:"rate_limit"`

		MaxFrameBytes int64 `mapstructure:"max_message_bytes"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.conn.manager", &cfg); err != nil {
		return nil, err
	}

	return NewConnManager(ConnManagerWithConfig(&ConnConfig{
		ReadTimeout:            cfg.ReadTimeout,
		WriteTimeout:           cfg.WriteTimeout,
		EstablishedReadTimeout: cfg.EstablishedReadTimeout,
		InitRetryInterval:      cfg.InitRetryInterval,
		MaxRetryInterval:       cfg.MaxRetryInterval,
		MaxRetryCount:          cfg.MaxRetryCount,
		SendBufferSize:         cfg.SendBufferSize,
		ReceiveBufferSize:      cfg.ReceiveBufferSize,
		CloseTimeout:           cfg.CloseTimeout,
		RateLimit:              cfg.RateLimit,
		MaxFrameBytes:          cfg.MaxFrameBytes,
	})), nil
}
