package provider

import (
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"github.com/jrmarcco/xgrpc/register"
	"github.com/spf13/viper"
)

func newKafkaConsumers(factory xconsumer.ConsumerFactory, si register.ServiceInstance) (map[string]*gateway.Consumer, error) {
	consumers := make(map[string]*gateway.Consumer)

	notifyMessageConsumer, err := notifyMessageConsumer(factory, si)
	if err != nil {
		return nil, fmt.Errorf("failed to create notify message consumer: %w", err)
	}
	consumers[gateway.EventNotifyMessage] = notifyMessageConsumer

	return consumers, nil
}

// notifyMessageConsumer 创建通知 topic 消费者。
//
// 通知 topic 为广播消费：每个网关节点独立消费全量，
// 因此 group_id 需按节点唯一化 ( 配置的 group_id + 本节点服务地址 )。
func notifyMessageConsumer(factory xconsumer.ConsumerFactory, si register.ServiceInstance) (*gateway.Consumer, error) {
	type config struct {
		Topic         string        `mapstructure:"topic"`
		GroupID       string        `mapstructure:"group_id"`
		Partitions    int32         `mapstructure:"partitions"`
		HandleTimeout time.Duration `mapstructure:"handle_timeout"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.gateway.consumer.event_message_notify", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal notify message consumer config: %w", err)
	}

	return gateway.NewConsumer(
		cfg.Topic,
		// 每节点唯一 group id，实现广播消费。
		fmt.Sprintf("%s-%s", cfg.GroupID, si.Addr),
		cfg.Partitions,
		cfg.HandleTimeout,

		factory,
	)
}
