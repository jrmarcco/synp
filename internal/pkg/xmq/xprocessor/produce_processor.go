package xprocessor

import (
	"context"
	"fmt"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xproducer"
)

// ProduceProcessor 是通用的生产处理器。
// 它封装了消息生产的逻辑，提供统一的接口。
type ProduceProcessor struct {
	topic    string
	producer xproducer.Producer
}

// NewProduceProcessor 创建一个新的生产处理器。
func NewProduceProcessor(topic string, producer xproducer.Producer) (*ProduceProcessor, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	return &ProduceProcessor{
		topic:    topic,
		producer: producer,
	}, nil
}

// Produce 发送消息到指定的主题。
func (p *ProduceProcessor) Produce(ctx context.Context, key, val []byte) error {
	return p.producer.Produce(ctx, &xmq.Message{
		Topic: p.topic,
		Key:   key,
		Val:   val,
	})
}
