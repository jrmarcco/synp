package upstream

import (
	"log/slog"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
)

var _ UMsgHandler = (*HeartbeatMsgHandler)(nil)

// HeartbeatMsgHandler 是心跳消息处理器的实现，用于处理心跳消息。
type HeartbeatMsgHandler struct {
	pushFunc message.PushFunc
}

func (h *HeartbeatMsgHandler) Handle(conn synp.Conn, msg *messagev1.Message) error {
	slog.Debug(
		"[heartbeat-msg-handler] received heartbeat message",
		"conn_id", conn.ID(),
		"message", msg.String(),
	)

	// 将心跳包直接返回给前端 ( ping & pong )。
	return h.pushFunc(conn, msg)
}

func (h *HeartbeatMsgHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_HEARTBEAT
}

func NewHeartbeatMsgHandler(pushFunc message.PushFunc) *HeartbeatMsgHandler {
	return &HeartbeatMsgHandler{
		pushFunc: pushFunc,
	}
}
