package xconsumer

import (
	"context"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
)

// ConsumerFactory 是创建消费者的工厂函数。
type ConsumerFactory interface {
	NewConsumer(topic, groupID string) (Consumer, error)
}

type Consumer interface {
	Consume(ctx context.Context) (*xmq.Message, error)
	ConsumeChan(ctx context.Context) (<-chan *xmq.Message, error)
	Close() error
}
