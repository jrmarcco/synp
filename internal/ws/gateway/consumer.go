package gateway

import (
	"context"
	"fmt"

	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsume"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xprocess"
	"go.uber.org/zap"
)

type Consumer struct {
	topic      string
	groupID    string
	partitions int32

	factory   xconsume.ConsumerFactory
	processor *xprocess.ConsumeProcessor

	logger *zap.Logger
}

func NewConsumer(
	topic, groupID string,
	partitions int32,
	factory xconsume.ConsumerFactory,
	logger *zap.Logger,
) (*Consumer, error) {
	if topic == "" || groupID == "" || partitions <= 0 {
		return nil, fmt.Errorf("topic, groupID and partitions are required")
	}

	return &Consumer{
		factory: factory,

		topic:      topic,
		groupID:    groupID,
		partitions: partitions,

		logger: logger,
	}, nil
}

func (c *Consumer) StartConsumer(ctx context.Context, handler xprocess.MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	// 为每个分区创建主题配置。
	topics := make([]xprocess.TopicConfig, c.partitions)
	for i := range c.partitions {
		topics[i] = xprocess.TopicConfig{
			Topic:   c.topic,
			GroupID: c.groupID,
		}
	}

	c.processor = xprocess.NewConsumeProcessor(
		xprocess.ConsumeProcessorConfig{
			Topics:  topics,
			Handler: handler,
		},
		c.factory,
	)

	return c.processor.Start(ctx)
}

func (c *Consumer) StopConsumer(ctx context.Context) error {
	if c.processor != nil {
		return c.processor.Stop(ctx)
	}
	return nil
}
