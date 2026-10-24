package provider

import (
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/message/notify"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
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

func newSyncMsgHandler(syncCaller upstream.SyncCaller, pushFunc message.PushFunc) (*upstream.SyncMsgHandler, error) {
	type config struct {
		Timeout time.Duration `mapstructure:"timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.message_handler.sync", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sync message handler config: %w", err)
	}

	return upstream.NewSyncMsgHandler(syncCaller, cfg.Timeout, pushFunc), nil
}

func newConversationSyncMsgHandler(syncCaller upstream.SyncCaller, pushFunc message.PushFunc) (*upstream.ConversationSyncMsgHandler, error) {
	type config struct {
		Timeout time.Duration `mapstructure:"timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.message_handler.sync", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sync message handler config: %w", err)
	}

	return upstream.NewConversationSyncMsgHandler(syncCaller, cfg.Timeout, pushFunc), nil
}

func newNotifyMsgHandler(pushFunc message.PushFunc) *notify.NotifyMsgHandler {
	return notify.NewNotifyMsgHandler(pushFunc)
}
