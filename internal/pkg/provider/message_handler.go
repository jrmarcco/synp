package provider

import (
	"time"

	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/message/downstream"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
	"github.com/jrmarcco/synp/internal/pkg/retransmit"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xproducer"
	"github.com/spf13/viper"
)

func newFrontendMsgHandler(producer xproducer.Producer, pushFunc message.PushFunc) *upstream.FrontendMsgHandler {
	type config struct {
		Topic            string `mapstructure:"topic"`
		OnReceiveTimeout int    `mapstructure:"on_receive_timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.handler.message.frontend", &cfg); err != nil {
		panic(err)
	}

	return upstream.NewFrontendMsgHandler(
		cfg.Topic,
		time.Duration(cfg.OnReceiveTimeout)*time.Millisecond,
		producer,
		pushFunc,
	)
}

func newBackendMsgHandler(pushFunc message.PushFunc, retransmitManager *retransmit.Manager) *downstream.BackendMsgHandler {
	return downstream.NewBackendMsgHandler(pushFunc, retransmitManager)
}
