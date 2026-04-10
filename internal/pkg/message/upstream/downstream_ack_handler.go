package upstream

import (
	"log/slog"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/retransmit"
)

var _ UMsgHandler = (*DownstreamAckHandler)(nil)

// DownstreamAckHandler 下行 ack 消息处理器。
// 前端收到下行消息后需要发送 ack 消息给网关。
// 网关收到 ack 消息后，会停止向前端推送下行消息的重试。
type DownstreamAckHandler struct {
	retransmitManager *retransmit.Manager
}

func NewDownstreamAckHandler(retransmitManager *retransmit.Manager) *DownstreamAckHandler {
	return &DownstreamAckHandler{
		retransmitManager: retransmitManager,
	}
}

func (h *DownstreamAckHandler) Handle(conn synp.Conn, msg *messagev1.Message) error {
	// 停止向前端推送 downstream 消息的重试。
	h.retransmitManager.Stop(conn.ID(), msg.MessageId)

	slog.Debug(
		"[downstream-ack-handler] received downstream ack message",
		"conn_id", conn.ID(),
		"message_id", msg.MessageId,
	)
	return nil
}

func (h *DownstreamAckHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_DOWNSTREAM_ACK
}
