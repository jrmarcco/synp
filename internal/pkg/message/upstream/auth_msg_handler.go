package upstream

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/logging"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/auth"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"google.golang.org/protobuf/encoding/protojson"
)

const defaultAuthTimeout = 3 * time.Second

var _ UMsgHandler = (*AuthMsgHandler)(nil)

// AuthMsgHandler 是首帧鉴权消息处理器的实现。
//
// 新协议要求连接建立后的第一帧必须为 AUTH ( body = AuthPayload{ token } )，
// 鉴权完成前网关拒绝处理任何其它指令；超时未鉴权的连接由网关关闭
// ( 超时由 ws.Server 管理 )。
type AuthMsgHandler struct {
	validator      auth.Validator
	sessionBuilder session.Builder

	connManager  synp.ConnManager
	connRegistry synp.ConnRegistry

	pushFunc message.PushFunc
}

func NewAuthMsgHandler(
	validator auth.Validator,
	sessionBuilder session.Builder,
	connManager synp.ConnManager,
	connRegistry synp.ConnRegistry,
	pushFunc message.PushFunc,
) *AuthMsgHandler {
	return &AuthMsgHandler{
		validator:      validator,
		sessionBuilder: sessionBuilder,
		connManager:    connManager,
		connRegistry:   connRegistry,
		pushFunc:       pushFunc,
	}
}

func (h *AuthMsgHandler) Handle(conn synp.Conn, msg *messagev1.Message) error {
	conn.UpdateActivityTime()

	start := time.Now()
	defer func() {
		metrics.AuthDuration.Observe(time.Since(start).Seconds())
	}()

	// 重复 AUTH 帧幂等处理：已鉴权的连接直接回成功。
	select {
	case <-conn.Authenticated():
		ctx, cancel := replyCtx()
		defer cancel()
		return h.replyAuthAck(ctx, conn, msg.GetMessageId(), true, "")
	default:
	}

	payload := &messagev1.AuthPayload{}
	if err := protojson.Unmarshal(msg.GetBody(), payload); err != nil {
		logging.For("auth-msg-handler").Error(
			"[auth-msg-handler] failed to unmarshal auth payload",
			"conn_id", conn.ID(),
			"error", err,
		)
		// 失败路径上的应答发送是尽力而为，随后连接即被关闭。
		h.replyAuthAckBestEffort(conn, msg.GetMessageId(), fmt.Sprintf("invalid auth payload: %v", err))
		return conn.Close()
	}

	user, err := h.validator.Validate(context.Background(), payload.GetToken())
	if err != nil {
		logging.For("auth-msg-handler").Warn(
			"[auth-msg-handler] failed to validate token",
			"conn_id", conn.ID(),
			"error", err,
		)
		h.replyAuthAckBestEffort(conn, msg.GetMessageId(), fmt.Sprintf("invalid token: %v", err))
		// 鉴权失败：发送应答后关闭连接。
		return conn.Close()
	}

	// 沿用升级阶段解析的设备类型与 auto close 参数。
	anonymousUser := conn.Session().User()
	user.Device = anonymousUser.Device
	user.AutoClose = anonymousUser.AutoClose

	if err = h.bindIdentity(conn, user); err != nil {
		logging.For("auth-msg-handler").Error(
			"[auth-msg-handler] failed to bind identity",
			"conn_id", conn.ID(),
			"error", err,
		)
		h.replyAuthAckBestEffort(conn, msg.GetMessageId(), "internal error")
		return conn.Close()
	}

	conn.MarkAuthenticated()

	logging.For("auth-msg-handler").Info(
		"[auth-msg-handler] connection authenticated",
		"conn_id", conn.ID(),
		"business_id", user.BID,
		"user_id", user.UID,
	)

	ctx, cancel := replyCtx()
	defer cancel()
	return h.replyAuthAck(ctx, conn, msg.GetMessageId(), true, "")
}

// bindIdentity 鉴权通过后绑定连接身份：
// 创建 session、绑定连接管理器用户索引、注册连接路由。
func (h *AuthMsgHandler) bindIdentity(conn synp.Conn, user session.User) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultAuthTimeout)
	defer cancel()

	createdSession, _, err := h.sessionBuilder.Build(ctx, user)
	if err != nil {
		return fmt.Errorf("failed to build session: %w", err)
	}
	conn.BindSession(createdSession)

	if err = h.connManager.BindUser(conn, user); err != nil {
		return fmt.Errorf("failed to bind user: %w", err)
	}

	// 注册连接路由 ( 重复注册幂等并刷新 TTL )。
	// 注册失败不影响连接，只是分布式路由可能缺失。
	if err = h.connRegistry.Register(ctx, user, conn.ID()); err != nil {
		logging.For("auth-msg-handler").Error(
			"[auth-msg-handler] failed to register connection",
			"conn_id", conn.ID(),
			"error", err,
		)
	}

	return nil
}

// replyAuthAckBestEffort 尽力发送鉴权失败应答：
// 发送失败或入队超时都不影响随后的连接关闭。
func (h *AuthMsgHandler) replyAuthAckBestEffort(conn synp.Conn, messageID, errorMessage string) {
	ctx, cancel := replyCtx()
	defer cancel()

	_ = h.replyAuthAck(ctx, conn, messageID, false, errorMessage)
}

func (h *AuthMsgHandler) replyAuthAck(ctx context.Context, conn synp.Conn, messageID string, success bool, errorMessage string) error {
	body, err := protojson.Marshal(&messagev1.AuthAckPayload{
		Success:      success,
		ErrorMessage: errorMessage,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal auth ack payload: %w", err)
	}

	return h.pushFunc(ctx, conn, &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_AUTH_ACK,
		MessageId:   messageID,
		Body:        body,
	})
}

func (h *AuthMsgHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_AUTH
}
