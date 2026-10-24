package upstream

import (
	"context"
	"log/slog"
	"time"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
)

const defaultHeartbeatTimeout = 3 * time.Second

var _ UMsgHandler = (*HeartbeatMsgHandler)(nil)

// HeartbeatMsgHandler 是心跳消息处理器的实现，用于处理心跳消息。
type HeartbeatMsgHandler struct {
	connRegistry synp.ConnRegistry

	pushFunc message.PushFunc
}

func NewHeartbeatMsgHandler(connRegistry synp.ConnRegistry, pushFunc message.PushFunc) *HeartbeatMsgHandler {
	return &HeartbeatMsgHandler{
		connRegistry: connRegistry,
		pushFunc:     pushFunc,
	}
}

func (h *HeartbeatMsgHandler) Handle(conn synp.Conn, msg *messagev1.Message) error {
	slog.Debug(
		"[heartbeat-msg-handler] received heartbeat message",
		"conn_id", conn.ID(),
		"message", msg.String(),
	)

	// 心跳等价于刷新连接路由的过期时间 ( 重复注册幂等并刷新 TTL )。
	ctx, cancel := context.WithTimeout(context.Background(), defaultHeartbeatTimeout)
	defer cancel()

	if err := h.connRegistry.Heartbeat(ctx, conn.Session().User()); err != nil {
		// 刷新失败不影响心跳应答，只影响分布式路由。
		slog.Warn(
			"[heartbeat-msg-handler] failed to refresh connection registry ttl",
			"conn_id", conn.ID(),
			"error", err,
		)
	}

	// 将心跳包直接返回给前端 ( ping & pong )。
	return h.pushFunc(conn, msg)
}

func (h *HeartbeatMsgHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_HEARTBEAT
}
