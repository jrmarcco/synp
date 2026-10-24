package upstream

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/logging"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"github.com/jrmarcco/synp/internal/pkg/xmq"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xproducer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ UMsgHandler = (*FrontendMsgHandler)(nil)

// FrontendMsgHandler 前端消息处理器的实现，用于处理前端发送的 UPSTREAM 帧。
//
// 身份字段 ( business_id / user_id / session_id ) 由网关以连接鉴权 ( JWT ) 结果盖章，
// 前端自报的身份一律不可信——这是整个模型的安全前提。
type FrontendMsgHandler struct {
	topic            string        // 消息队列 topic
	onReceiveTimeout time.Duration // 接收消息超时时间

	producer xproducer.Producer
	pushFunc message.PushFunc
}

func NewFrontendMsgHandler(
	topic string,
	onReceiveTimeout time.Duration,
	producer xproducer.Producer,
	pushFunc message.PushFunc,
) *FrontendMsgHandler {
	return &FrontendMsgHandler{
		topic:            topic,
		onReceiveTimeout: onReceiveTimeout,

		producer: producer,
		pushFunc: pushFunc,
	}
}

func (h *FrontendMsgHandler) Handle(conn synp.Conn, msg *messagev1.Message) error {
	// 接收到前端消息，更新连接活跃时间。
	conn.UpdateActivityTime()

	now := time.Now().UnixMilli()

	ackPayload := &messagev1.AckPayload{
		IsSuccess:   true,
		TimestampMs: now,
	}

	// 转发消息到后端。
	if err := h.forwardToBackend(conn, msg); err != nil {
		ackPayload.IsSuccess = false
		ackPayload.TimestampMs = now
		ackPayload.ErrorMessage = err.Error()
	}

	body, err := protojson.Marshal(ackPayload)
	if err != nil {
		logging.For("frontend-msg-handler").Error(
			"[frontend-msg-handler] failed to marshal ack payload",
			"conn_id", conn.ID(),
			"error", err,
		)
		return fmt.Errorf("failed to marshal ack payload: %w", err)
	}

	// 向前端发送 ACK 消息。
	// UPSTREAM_ACK 仅表示网关已接收该帧，不代表业务处理成功；
	// 业务层面的处理结果通过同步链路 ( NOTIFY -> SYNC ) 获得。
	replyCtx, cancel := replyCtx()
	defer cancel()

	return h.pushFunc(replyCtx, conn, &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_UPSTREAM_ACK,
		MessageId:   msg.GetMessageId(),
		Body:        body,
	})
}

// forwardToBackend 转发消息到后端。
// 通信方式为投递 UpstreamMessage 到 Kafka 上行 topic，由后端订阅并处理。
func (h *FrontendMsgHandler) forwardToBackend(conn synp.Conn, msg *messagev1.Message) error {
	user := conn.Session().User()

	// 身份字段由网关盖章。
	upstreamMsg := &messagev1.UpstreamMessage{
		MessageId:    msg.GetMessageId(),
		BusinessId:   user.BID,
		UserId:       user.UID,
		SessionId:    user.SessionID,
		ConnectionId: conn.ID(),

		// 前端 UPSTREAM 帧的 body，原样转发，网关与后端均不解释。
		Body: msg.GetBody(),

		OccurredAt: timestamppb.Now(),
	}

	val, err := protojson.Marshal(upstreamMsg)
	if err != nil {
		logging.For("frontend-msg-handler").Error(
			"[frontend-msg-handler] failed to marshal upstream message",
			"conn_id", conn.ID(),
			"error", err,
		)
		return fmt.Errorf("failed to marshal upstream message: %w", err)
	}

	mqMsg := &xmq.Message{
		Topic: h.topic,
		// 分区键: hash(business_id, user_id)。
		// 同一发送者的消息落在同一分区内有序，便于后端串行分配 seq。
		// 不要用 message_id 做分区键——它每帧唯一，hash 后消息散落到随机分区。
		Key: fmt.Appendf(nil, "%d:%d", user.BID, user.UID),
		Val: val,
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.onReceiveTimeout)
	defer cancel()

	start := time.Now()
	produceErr := h.producer.Produce(ctx, mqMsg)
	metrics.KafkaProduceDuration.Observe(time.Since(start).Seconds())

	if err = produceErr; err != nil {
		logging.For("frontend-msg-handler").Error(
			"[frontend-msg-handler] failed to forward message to backend with message queue",
			"conn_id", conn.ID(),
			"error", err,
		)
		return fmt.Errorf("failed to forward message: %w", err)
	}

	return nil
}

func (h *FrontendMsgHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_UPSTREAM
}
