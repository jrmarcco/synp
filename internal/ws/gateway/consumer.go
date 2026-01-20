package gateway

import (
	"context"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
	"go.uber.org/zap"
)

type ConsumeFunc func(ctx context.Context, msg *xmq.Message) error

type Consumer struct {
	consumerFactory xconsumer.ConsumerFactory

	topic      string
	groupID    string
	partitions int32

	ctx        context.Context
	cancelFunc context.CancelFunc

	logger *zap.Logger
}

func (c *Consumer) Start(ctx context.Context, consumeFunc ConsumeFunc) error {
	for i := range c.partitions {
		partition := i

		consumer, err := c.consumerFactory.NewConsumer(c.topic, c.groupID)
		if err != nil {
			c.logger.Error(
				"[synp-gateway-consumer] failed to create consumer",
				zap.String("topic", c.topic),
				zap.String("group_id", c.groupID),
				zap.Int32("partition", partition),
				zap.Error(err),
			)
			return err
		}

		msgChan, err := consumer.ConsumeChan(ctx)
		if err != nil {
			c.logger.Error(
				"[synp-gateway-consumer] failed to get message channel from mq",
				zap.String("topic", c.topic),
				zap.String("group_id", c.groupID),
				zap.Int32("partition", partition),
				zap.Error(err),
			)
			return err
		}

		go c.consume(ctx, msgChan, consumeFunc)
	}
	return nil
}

func (c *Consumer) consume(ctx context.Context, msgChan <-chan *xmq.Message, consumeFunc ConsumeFunc) {
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("[synp-gateway-consumer] consumer context done")
			return
		case <-ctx.Done():
			c.logger.Info("[synp-gateway-consumer] method param context done")
			return
		case msg, ok := <-msgChan:
			if !ok {
				return
			}

			err := consumeFunc(ctx, msg)
			if err != nil {
				c.logger.Error(
					"[synp-gateway-consumer] failed to consume message",
					zap.String("message", string(msg.Val)),
					zap.Error(err),
				)
			}
			c.logger.Debug(
				"[synp-gateway-consumer] successfully consumed message",
				zap.String("message", string(msg.Val)),
			)
		}
	}
}

func (c *Consumer) Stop() error {
	c.cancelFunc()
	return nil
}

func NewConsumer(consumerFactory xconsumer.ConsumerFactory, topic, groupID string, partitions int32, logger *zap.Logger) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		consumerFactory: consumerFactory,

		topic:      topic,
		groupID:    groupID,
		partitions: partitions,

		ctx:        ctx,
		cancelFunc: cancel,

		logger: logger,
	}
}
