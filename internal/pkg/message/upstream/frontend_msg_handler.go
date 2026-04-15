package upstream

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/xmq"
	"github.com/jrmarcco/synp/internal/pkg/xmq/xproducer"
	"google.golang.org/protobuf/encoding/protojson"
)

var _ UMsgHandler = (*FrontendMsgHandler)(nil)

// FrontendMsgHandler 前端消息处理器的实现，用于处理前端发送的消息。
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
	if err := h.forwardToBackend(msg); err != nil {
		ackPayload.IsSuccess = false
		ackPayload.TimestampMs = now
		ackPayload.ErrorMessage = err.Error()
	}

	body, err := protojson.Marshal(ackPayload)
	if err != nil {
		slog.Error(
			"[frontend-msg-handler] failed to marshal ack payload",
			"conn_id", conn.ID(),
			"error", err,
		)
		return fmt.Errorf("failed to marshal ack payload: %w", err)
	}

	// 向前端发送 ACK 消息。
	return h.pushFunc(conn, &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_UPSTREAM_ACK,
		MessageId:   msg.GetMessageId(),
		Body:        body,
	})
}

// forwardToBackend 转发消息到后端。
// 通信方式为推送消息到 kafka 由后端订阅并处理。
func (h *FrontendMsgHandler) forwardToBackend(msg *messagev1.Message) error {
	// 只需要转发 Body ( 消息体 )。
	mqMsg := &xmq.Message{
		Topic: h.topic,
		Key:   []byte(msg.GetMessageId()),
		Val:   msg.GetBody(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.onReceiveTimeout)
	defer cancel()

	if err := h.producer.Produce(ctx, mqMsg); err != nil {
		slog.Error(
			"[frontend-msg-handler] failed to forward message to backend with messsage queue",
			"error", err,
		)
		return fmt.Errorf("failed to forward message: %w", err)
	}

	return nil
}

func (h *FrontendMsgHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_UPSTREAM
}
