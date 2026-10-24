package provider

import (
	"github.com/jrmarcco/synp/internal/pkg/auth"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/message/notify"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xconsumer"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xproducer"
	"go.uber.org/fx"
)

var (
	EtcdFxModule            = fx.Module("etcd", fx.Provide(newEtcdClient))
	RedisFxModule           = fx.Module("redis", fx.Provide(newRedisCmdable))
	CodecFxModule           = fx.Module("codec", fx.Provide(newCodec))
	MessagePushFuncFxModule = fx.Module("message-push-func", fx.Provide(message.DefaultPushFunc))
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

			// 首帧鉴权消息处理器。
			fx.Annotate(
				upstream.NewAuthMsgHandler,
				fx.As(new(upstream.UMsgHandler)),
				fx.ResultTags(`group:"upstream-message-handler"`),
			),

			// 前端消息处理器。
			fx.Annotate(
				newFrontendMsgHandler,
				fx.As(new(upstream.UMsgHandler)),
				fx.ResultTags(`group:"upstream-message-handler"`),
			),

			// 收件箱同步消息处理器。
			fx.Annotate(
				newSyncMsgHandler,
				fx.As(new(upstream.UMsgHandler)),
				fx.ResultTags(`group:"upstream-message-handler"`),
			),

			// 会话同步消息处理器。
			fx.Annotate(
				newConversationSyncMsgHandler,
				fx.As(new(upstream.UMsgHandler)),
				fx.ResultTags(`group:"upstream-message-handler"`),
			),

			// 后端通知消息处理器。
			fx.Annotate(
				newNotifyMsgHandler,
				fx.As(new(notify.NMsgHandler)),
			),
		),
	)
)
