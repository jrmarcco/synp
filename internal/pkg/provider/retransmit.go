package provider

import (
	"context"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/retransmit"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func newRetransmitManager(pushFunc message.PushFunc, lifecycle fx.Lifecycle) (*retransmit.Manager, error) {
	type config struct {
		Interval int `mapstructure:"interval"`
		MaxRetry int `mapstructure:"max_retry"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("retransmit", &cfg); err != nil {
		return nil, err
	}

	manager := retransmit.NewManager(
		time.Duration(cfg.Interval)*time.Millisecond,
		int32(cfg.MaxRetry),
		pushFunc,
	)

	lifecycle.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			manager.Close()
			return nil
		},
	})

	return manager, nil
}
