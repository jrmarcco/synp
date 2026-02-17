package lifecycle

import (
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/codec"
	"github.com/jrmarcco/synp/internal/pkg/message/downstream"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var ConnLifecycleFxModule = fx.Module(
	"ws-conn-lifecycle",
	fx.Provide(
		fx.Annotate(
			newConnLifecycleHandler,
			fx.As(new(synp.Handler)),
		),
	),
)

type connHandlerFxParams struct {
	fx.In

	Rdb   redis.Cmdable
	Codec codec.Codec

	UMsgHandlers []upstream.UMsgHandler `group:"upstream-message-handler"`
	DMsgHandler  downstream.DMsgHandler

	ConnRegistry synp.ConnRegistry

	Logger *zap.Logger
}

func newConnLifecycleHandler(params connHandlerFxParams) (*Handler, error) {
	type config struct {
		CacheRequestTimeout time.Duration `mapstructure:"cache_request_timeout"`
		CacheExpiration     time.Duration `mapstructure:"cache_expiration"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.conn.handler", &cfg); err != nil {
		return nil, err
	}

	return NewHandler(
		params.Rdb,
		cfg.CacheRequestTimeout,
		cfg.CacheExpiration,
		params.Codec,
		params.DMsgHandler,
		params.UMsgHandlers,
		params.ConnRegistry,
		params.Logger,
	), nil
}
