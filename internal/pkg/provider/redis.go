package provider

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func newRedisCmdable(logger *zap.Logger, lifecycle fx.Lifecycle) (redis.Cmdable, error) {
	type config struct {
		Addr     string `mapstructure:"addr"`
		Password string `mapstructure:"password"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("redis", &cfg); err != nil {
		return nil, err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
	})

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// 测试 redis 连接。
			if err := rdb.Ping(ctx).Err(); err != nil {
				logger.Error("[redis] failed to ping redis", zap.Error(err))
				return fmt.Errorf("failed to ping redis: %w", err)
			}
			return nil
		},
		OnStop: func(_ context.Context) error {
			// 关闭 redis 连接。
			if err := rdb.Close(); err != nil {
				logger.Error("[redis] failed to close redis", zap.Error(err))
				return fmt.Errorf("failed to close redis: %w", err)
			}

			logger.Info("[redis] redis closed")
			return nil
		},
	})

	return rdb, nil
}
