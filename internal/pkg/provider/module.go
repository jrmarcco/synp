package provider

import (
	"go.uber.org/fx"

	"github.com/jrmarcco/synp/internal/pkg/auth"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/message/downstream"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xproducer"
)

var (
	ZapLoggerFxModule       = fx.Module("zap-logger", fx.Provide(newLogger))
	RedisFxModule           = fx.Module("redis", fx.Provide(newRedisCmdable))
	CodecFxModule           = fx.Module("codec", fx.Provide(newCodec))
	MessagePushFuncFxModule = fx.Module("message-push-func", fx.Provide(message.DefaultPushFunc))
	RetransmitFxModule      = fx.Module("retransmit", fx.Provide(newRetransmitManager))
)

var (
	KafkaFxModule         = fx.Module("kafka", fx.Provide(newKafkaClient))
	KafkaProducerFxModule = fx.Module(
		"kafka-producer",
		fx.Provide(
			fx.Annotate(
				xproducer.NewKafkaProducer,
				fx.As(new(xproducer.Producer)),
			),
		),
	)
	KafkaConsumerFxModule = fx.Module(
		"kafka-consumer",
		fx.Provide(
			fx.Annotate(
				xconsumer.NewKafkaConsumerFactory,
				fx.As(new(xconsumer.ConsumerFactory)),
			),
			newKafkaConsumers,
		),
	)
)

var (
	ValidatorFxModule = fx.Module(
		"validator",
		fx.Provide(
			fx.Annotate(
				newValidator,
				fx.As(new(auth.Validator)),
			),
		),
	)

	MessageHandlerFxModule = fx.Module(
		"message-handler",
		fx.Provide(
			// 心跳消息处理器。
			fx.Annotate(
				upstream.NewHeartbeatMsgHandler,
				fx.As(new(upstream.UMsgHandler)),
				fx.ResultTags(`group:"upstream-message-handler"`),
			),

			// 前端消息处理器。
			fx.Annotate(
				newFrontendMsgHandler,
				fx.As(new(upstream.UMsgHandler)),
				fx.ResultTags(`group:"upstream-message-handler"`),
			),

			// 下行消息 ack 处理器。
			fx.Annotate(
				upstream.NewDownstreamAckHandler,
				fx.As(new(upstream.UMsgHandler)),
				fx.ResultTags(`group:"upstream-message-handler"`),
			),

			// 后端消息处理器。
			fx.Annotate(
				newBackendMsgHandler,
				fx.As(new(downstream.DMsgHandler)),
			),
		))
)
