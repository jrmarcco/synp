package xprocessor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
)

// TopicConfig 定义单个消费者的配置。
type TopicConfig struct {
	Topic   string
	GroupID string
}

// ConsumeProcessorConfig 是消费处理器的配置。
type ConsumeProcessorConfig struct {
	Topics        []TopicConfig // 消费的主题配置列表
	Handler       MsgHandler    // 消息处理函数
	HandleTimeout time.Duration // 消息处理的超时时间
}

// ConsumeProcessor 是通用的消费处理器。
// 它负责管理多个消费者的生命周期，并将消息分发给处理函数。
type ConsumeProcessor struct {
	cfg     ConsumeProcessorConfig
	factory xconsumer.ConsumerFactory

	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewConsumeProcessor 创建一个新的消费处理器。
func NewConsumeProcessor(cfg ConsumeProcessorConfig, factory xconsumer.ConsumerFactory) *ConsumeProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumeProcessor{
		cfg:     cfg,
		factory: factory,

		ctx:        ctx,
		cancelFunc: cancel,
	}
}

// Start 启动处理器，为每个主题创建消费者并开始消费。
// ctx 仅用于控制启动过程的超时和取消。
func (p *ConsumeProcessor) Start(ctx context.Context) error {
	if p.cfg.Handler == nil {
		return fmt.Errorf("message handler is required")
	}
	if p.cfg.HandleTimeout <= 0 {
		return fmt.Errorf("handle timeout is required")
	}

	for _, topicCfg := range p.cfg.Topics {
		// 检查是否已取消。
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		consumer, err := p.factory.NewConsumer(topicCfg.Topic, topicCfg.GroupID)
		if err != nil {
			slog.Error(
				"[xmq-consumer-processor] failed to create consumer",
				"topic", topicCfg.Topic,
				"group_id", topicCfg.GroupID,
				"error", err.Error(),
			)
			return err
		}
		// 注意：
		// 	这里使用 p.ctx ( 内部 context ) 而不是传入的 ctx。
		//  方法参数 ctx 仅用于控制 Start 方法操作本身的超时和取消。
		//  p.ctx 用于控制消费 goroutine 的生命周期 ( 直到调用 Stop 方法 )。
		//
		//nolint:contextcheck // 这里需要使用长生命周期的 p.ctx
		msgChan, err := consumer.ConsumeChan(p.ctx)
		if err != nil {
			slog.Error(
				"[xmq-consumer-processor] failed to get message channel",
				"topic", topicCfg.Topic,
				"group_id", topicCfg.GroupID,
				"error", err.Error(),
			)
			return err
		}
		// 为每个主题启动一个 goroutine 来消费消息。
		//nolint:gosec,contextcheck // consume 内调用的Handler 是独立逻辑单元，使用 context.Background() 作为超时基础上下文。
		go p.consume(msgChan)
	}

	return nil
}

// consume 是通用的消费循环，从消息通道中读取并处理消息。
func (p *ConsumeProcessor) consume(msgChan <-chan *xmq.Message) {
	for {
		select {
		case <-p.ctx.Done():
			slog.Info("[xmq-consumer-processor] processor context done")
			return
		case msg, ok := <-msgChan:
			if !ok {
				slog.Info("[xmq-consumer-processor] message channel closed")
				return
			}

			handlerCtx, handlerCancel := context.WithTimeout(context.Background(), p.cfg.HandleTimeout)
			err := p.cfg.Handler(handlerCtx, msg)
			handlerCancel()

			if err != nil {
				slog.Error(
					"[xmq-consumer-processor] failed to process message",
					"topic", msg.Topic,
					"offset", msg.Offset,
					"partition", msg.Partition,
					"error", err.Error(),
				)
			}
		}
	}
}

// Stop 停止处理器，取消所有消费者。
func (p *ConsumeProcessor) Stop(_ context.Context) error {
	p.cancelFunc()
	return nil
}
