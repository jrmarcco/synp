package provider

import (
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func newKafkaConsumers(factory xconsumer.ConsumerFactory, logger *zap.Logger) (map[string]*gateway.Consumer, error) {
	consumers := make(map[string]*gateway.Consumer)

	pushMessageConsumer, err := pushMessageConsumer(factory, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create push message consumer: %w", err)
	}
	consumers[gateway.EventPushMessage] = pushMessageConsumer

	return consumers, err
}

func pushMessageConsumer(factory xconsumer.ConsumerFactory, logger *zap.Logger) (*gateway.Consumer, error) {
	type config struct {
		Topic         string        `mapstructure:"topic"`
		GroupID       string        `mapstructure:"group_id"`
		Partitions    int32         `mapstructure:"partitions"`
		HandleTimeout time.Duration `mapstructure:"handle_timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.gateway.consumer.event_message_downstream", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal push message consumer config: %w", err)
	}

	return gateway.NewConsumer(
		cfg.Topic,
		cfg.GroupID,
		cfg.Partitions,
		cfg.HandleTimeout,

		factory,
		logger,
	)
}
