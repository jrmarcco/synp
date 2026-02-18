package xproducer

import (
	"context"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
)

var _ Producer = (*KafkaProducer)(nil)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(writer *kafka.Writer) *KafkaProducer {
	return &KafkaProducer{
		writer: writer,
	}
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
		"[xmq-producer] successfully produced message to kafka",
		"topic", msg.Topic,
		"offset", msg.Offset,
		"message", string(msg.Val),
	)
	return nil
}
