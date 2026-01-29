package providers

import (
	"fmt"

	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func newKafkaConsumers(consumerFactory xconsumer.ConsumerFactory, logger *zap.Logger) (map[string]*gateway.Consumer, error) {
	consumers := make(map[string]*gateway.Consumer)

	pushMessageConsumer, err := pushMessageConsumer(consumerFactory, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create push message consumer: %w", err)
	}
	consumers[gateway.EventPushMessage] = pushMessageConsumer

	return consumers, err
}

func pushMessageConsumer(consumerFactory xconsumer.ConsumerFactory, logger *zap.Logger) (*gateway.Consumer, error) {
	type consumerConfig struct {
		Topic      string `mapstructure:"topic"`
		GroupID    string `mapstructure:"group_id"`
		Partitions int32  `mapstructure:"partitions"`
	}

	cfg := consumerConfig{}
	if err := viper.UnmarshalKey("synp.gateway.consumer.event_message_downstream", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal push message consumer config: %w", err)
	}

	return gateway.NewConsumer(
		consumerFactory,
		cfg.Topic,
		cfg.GroupID,
		cfg.Partitions,
		logger,
	)
}
