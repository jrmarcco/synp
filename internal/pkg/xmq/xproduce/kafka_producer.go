package xproduce

import (
	"context"
	"log/slog"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
	"github.com/segmentio/kafka-go"
)

var _ Producer = (*KafkaProducer)(nil)

type KafkaProducer struct {
	writer *kafka.Writer
}

func (p *KafkaProducer) Produce(ctx context.Context, msg *xmq.Message) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic: msg.Topic,
		Key:   msg.Key,
		Value: msg.Val,
	})
	if err != nil {
		return err
	}

	slog.Debug(
		"[synp-xmq-producer] successfully produced message to kafka",
		"topic", msg.Topic,
		"offset", msg.Offset,
		"message", string(msg.Val),
	)
	return nil
}

func NewKafkaProducer(writer *kafka.Writer) *KafkaProducer {
	return &KafkaProducer{
		writer: writer,
	}
}
