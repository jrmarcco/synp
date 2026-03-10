package provider

import (
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/message/downstream"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
	"github.com/jrmarcco/synp/internal/pkg/retransmit"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xproducer"
	"github.com/spf13/viper"
)

func newFrontendMsgHandler(producer xproducer.Producer, pushFunc message.PushFunc) (*upstream.FrontendMsgHandler, error) {
	type config struct {
		Topic            string        `mapstructure:"topic"`
		OnReceiveTimeout time.Duration `mapstructure:"on_receive_timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.message_handler.frontend", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal frontend message handler config: %w", err)
	}

	return upstream.NewFrontendMsgHandler(
		cfg.Topic,
		cfg.OnReceiveTimeout,
		producer,
		pushFunc,
	), nil
}

func newBackendMsgHandler(pushFunc message.PushFunc, retransmitManager *retransmit.Manager) *downstream.BackendMsgHandler {
	return downstream.NewBackendMsgHandler(pushFunc, retransmitManager)
}
