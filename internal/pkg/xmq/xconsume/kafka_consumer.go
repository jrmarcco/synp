package xconsume

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
	"github.com/segmentio/kafka-go"
)

const defaultMessageChanSize = 1024

// KafkaReaderCreateFunc 是创建 kafka Reader 的工厂函数。
type KafkaReaderCreateFunc func(topic string, groupId string) *kafka.Reader

var _ ConsumerFactory = (*KafkaConsumerFactory)(nil)

// KafkaConsumerFactory 是 kafka 消费者工厂。
// 负责创建 kafka 消费者。
type KafkaConsumerFactory struct {
	readerCreateFunc KafkaReaderCreateFunc
}

func NewKafkaConsumerFactory(readerCreateFunc KafkaReaderCreateFunc) *KafkaConsumerFactory {
	return &KafkaConsumerFactory{
		readerCreateFunc: readerCreateFunc,
	}
}

func (f *KafkaConsumerFactory) NewConsumer(topic, groupID string) (Consumer, error) {
	return NewKafkaConsumer(topic, groupID, f.readerCreateFunc), nil
}

var _ Consumer = (*KafkaConsumer)(nil)

// KafkaConsumer 是 kafka 消费者。
// 负责从 kafka 中消费消息，并转换为 xmq.Message。
type KafkaConsumer struct {
	topic   string
	groupID string

	reader *kafka.Reader

	messageChan chan *xmq.Message

	// 用于控制消费者生命周期。
	ctx        context.Context
	cancelFunc context.CancelFunc

	closeOnce sync.Once
}

func NewKafkaConsumer(topic, groupID string, readerCreateFunc KafkaReaderCreateFunc) *KafkaConsumer {
	ctx, cancel := context.WithCancel(context.Background())

	reader := readerCreateFunc(topic, groupID)
	consumer := &KafkaConsumer{
		topic:   topic,
		groupID: groupID,

		reader: reader,

		messageChan: make(chan *xmq.Message, defaultMessageChanSize),

		ctx:        ctx,
		cancelFunc: cancel,
	}

	go consumer.readMessage()
	return consumer
}

func (c *KafkaConsumer) Consume(ctx context.Context) (*xmq.Message, error) {
	select {
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-c.messageChan:
		if !ok {
			return nil, xmq.ErrConsumerClosed
		}
		return msg, nil
	}
}

func (c *KafkaConsumer) ConsumeChan(ctx context.Context) (<-chan *xmq.Message, error) {
	if c.ctx.Err() != nil {
		return nil, xmq.ErrConsumerClosed
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return c.messageChan, nil
}

func (c *KafkaConsumer) readMessage() {
	defer func() {
		close(c.messageChan)
	}()

	for {
		kafkaMsg, err := c.reader.ReadMessage(c.ctx)
		if err != nil {
			if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return
			}
			slog.Warn("[xmq-consumer] failed to read message from kafka", "err", err.Error())
			continue
		}

		msg := c.convertMessage(&kafkaMsg)
		select {
		case c.messageChan <- msg:
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *KafkaConsumer) convertMessage(kafkaMsg *kafka.Message) *xmq.Message {
	headers := xmq.Headers{}
	for _, header := range kafkaMsg.Headers {
		headers[header.Key] = string(header.Value)
	}

	return &xmq.Message{
		Headers: headers,

		Topic:     kafkaMsg.Topic,
		Partition: kafkaMsg.Partition,
		Offset:    kafkaMsg.Offset,

		Key: kafkaMsg.Key,
		Val: kafkaMsg.Value,
	}
}

func (c *KafkaConsumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.cancelFunc()
		err = c.reader.Close()
	})
	return err
}
