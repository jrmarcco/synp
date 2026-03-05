package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xprocessor"
	"go.uber.org/zap"
)

type Consumer struct {
	topic         string
	groupID       string
	partitions    int32
	handleTimeout time.Duration

	factory   xconsumer.ConsumerFactory
	processor *xprocessor.ConsumeProcessor

	logger *zap.Logger
}

func NewConsumer(
	topic, groupID string,
	partitions int32,
	handleTimeout time.Duration,
	factory xconsumer.ConsumerFactory,
	logger *zap.Logger,
) (*Consumer, error) {
	if topic == "" || groupID == "" || partitions <= 0 {
		return nil, fmt.Errorf("topic, groupID and partitions are required")
	}

	return &Consumer{
		factory: factory,

		topic:         topic,
		groupID:       groupID,
		partitions:    partitions,
		handleTimeout: handleTimeout,

		logger: logger,
	}, nil
}

func (c *Consumer) StartConsumer(ctx context.Context, handler xprocessor.MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	// 为每个分区创建主题配置。
	topics := make([]xprocessor.TopicConfig, c.partitions)
	for i := range c.partitions {
		topics[i] = xprocessor.TopicConfig{
			Topic:   c.topic,
			GroupID: c.groupID,
		}
	}

	c.processor = xprocessor.NewConsumeProcessor(
		xprocessor.ConsumeProcessorConfig{
			Topics:        topics,
			Handler:       handler,
			HandleTimeout: c.handleTimeout,
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
