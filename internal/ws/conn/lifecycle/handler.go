package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	cc "github.com/jrmarcco/synp/internal/pkg/codec"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/message/notify"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"github.com/redis/go-redis/v9"
)

const defaultRegisterTimeout = 3 * time.Second

var (
	ErrInvalidMessage     = errors.New("invalid message")
	ErrCacheMessage       = errors.New("cache message failed")
	ErrUncacheMessage     = errors.New("uncache message failed")
	ErrMessageDuplicated  = errors.New("message duplicated, ignore it")
	ErrUnknownMessageType = errors.New("unknown message (command) type")
	ErrMaxRetryExceeded   = errors.New("max retry exceeded")
	ErrNotAuthenticated   = errors.New("connection not authenticated")
)

var _ synp.Handler = (*Handler)(nil)

// Handler 是连接事件处理器。
type Handler struct {
	rdb redis.Cmdable

	cacheRequestTimeout time.Duration
	cacheExpiration     time.Duration

	codec cc.Codec

	nMsgHandler    notify.NMsgHandler
	uMsgHandlerMap map[commonv1.CommandType]upstream.UMsgHandler

	connRegistry synp.ConnRegistry

	logger *slog.Logger
}

func NewHandler(
	rdb redis.Cmdable,
	cacheRequestTimeout time.Duration,
	cacheExpiration time.Duration,
	codec cc.Codec,
	nMsgHandler notify.NMsgHandler,
	uMsgHandlers []upstream.UMsgHandler,
	connRegistry synp.ConnRegistry,
) *Handler {
	uMsgHandlerMap := make(map[commonv1.CommandType]upstream.UMsgHandler)
	for _, handler := range uMsgHandlers {
		uMsgHandlerMap[handler.CmdType()] = handler
	}

	return &Handler{
		rdb: rdb,

		cacheRequestTimeout: cacheRequestTimeout,
		cacheExpiration:     cacheExpiration,

		codec: codec,

		nMsgHandler:    nMsgHandler,
		uMsgHandlerMap: uMsgHandlerMap,
		connRegistry:   connRegistry,

		logger: logging.For("conn-lifecycle"),
	}
}

// OnConnect 处理连接建立事件。
// 注意：路由注册不在连接建立时进行，
// 而是在首帧 AUTH 鉴权通过后 ( 由 AuthMsgHandler 完成 )。
func (h *Handler) OnConnect(conn synp.Conn) error {
	h.logger.Debug(
		"[conn-lifecycle-handler] connection connected, waiting for auth",
		"conn_id", conn.ID(),
	)
	return nil
}

// OnDisconnect 处理连接断开事件。
func (h *Handler) OnDisconnect(conn synp.Conn) error {
	// 只有完成鉴权的连接才注册过路由，需要注销。
	select {
	case <-conn.Authenticated():
	default:
		h.logger.Debug(
			"[conn-lifecycle-handler] anonymous connection disconnected",
			"conn_id", conn.ID(),
		)
		return conn.Close()
	}

	user := conn.Session().User()

	// 从注册表中注销连接。
	ctx, cancel := context.WithTimeout(context.Background(), defaultRegisterTimeout)
	defer cancel()

	if err := h.connRegistry.Unregister(ctx, user, conn.ID()); err != nil {
		// 注销失败不影响连接关闭，只记录日志。
		h.logger.Error(
			"[conn-lifecycle-handler] failed to unregister connection",
			"conn_id", conn.ID(),
			"user", user,
			"error", err,
		)
	}

	h.logger.Debug(
		"[conn-lifecycle-handler] connection disconnected and unregistered",
		"conn_id", conn.ID(),
	)
	return conn.Close()
}

// OnReceiveFromFrontend 处理收到前端消息的事件。
func (h *Handler) OnReceiveFromFrontend(conn synp.Conn, payload []byte) error {
	// 解析 payload。
	msg, err := h.decodePayload(payload)
	if err != nil {
		h.logger.Error(
			"[conn-lifecycle-handler] failed to decode payload",
			"conn_id", conn.ID(),
			"user", conn.Session().User(),
			"error", err,
		)
		return err
	}

	// 鉴权守卫：鉴权完成前网关拒绝处理任何非 AUTH 指令。
	if msg.GetCommandType() != commonv1.CommandType_COMMAND_TYPE_AUTH && !h.authenticated(conn) {
		h.logger.Warn(
			"[conn-lifecycle-handler] reject message before authentication",
			"conn_id", conn.ID(),
			"command_type", msg.GetCommandType().String(),
		)
		return ErrNotAuthenticated
	}

	// 消息去重 ( 幂等 )。
	// 不参与去重的帧:
	//   - HEARTBEAT: 高频且幂等。
	//   - AUTH: 重复 AUTH 由 AuthMsgHandler 幂等处理。
	//   - SYNC / CONVERSATION_SYNC: 同步请求幂等由游标语义保证
	//     ( 重复拉取返回相同增量 )，不应被 Redis 可用性绑死。
	user := conn.Session().User()
	if msg.GetCommandType() != commonv1.CommandType_COMMAND_TYPE_HEARTBEAT &&
		msg.GetCommandType() != commonv1.CommandType_COMMAND_TYPE_AUTH &&
		msg.GetCommandType() != commonv1.CommandType_COMMAND_TYPE_SYNC &&
		msg.GetCommandType() != commonv1.CommandType_COMMAND_TYPE_CONVERSATION_SYNC {
		ok, cacheErr := h.cacheMessage(user.BID, user.UID, msg)
		if cacheErr != nil {
			h.logger.Error(
				"[conn-lifecycle-handler] failed to cache message",
				"conn_id", conn.ID(),
				"user", user,
				"error", cacheErr,
			)
			return fmt.Errorf("%w: %w", ErrCacheMessage, cacheErr)
		}

		if !ok {
			h.logger.Warn(
				"[conn-lifecycle-handler] message duplicated, ignore it",
				"conn_id", conn.ID(),
				"message_id", msg.GetMessageId(),
				"user", user,
			)
			return ErrMessageDuplicated
		}
	}

	// 热路径日志降级: 只记录元数据，不序列化完整消息体。
	metrics.MsgInTotal.WithLabelValues(msg.GetCommandType().String()).Inc()

	h.logger.Debug(
		"[conn-lifecycle-handler] received message from frontend",
		"conn_id", conn.ID(),
		"message_id", msg.GetMessageId(),
		"command_type", msg.GetCommandType().String(),
		"body_len", len(msg.GetBody()),
		"user", user,
	)

	// 处理消息。
	uMsgHandler, ok := h.uMsgHandlerMap[msg.GetCommandType()]
	if !ok {
		h.logger.Error(
			"[conn-lifecycle-handler] unknown message type from frontend",
			"conn_id", conn.ID(),
			"user", user,
		)
		return ErrUnknownMessageType
	}

	if err = uMsgHandler.Handle(conn, msg); err != nil {
		// 删除消息缓存。
		if h.needUncacheMessage(err) {
			uncacheErr := h.uncacheMessage(user.BID, user.UID, msg)
			if uncacheErr != nil {
				h.logger.Error(
					"[conn-lifecycle-handler] failed to uncache message",
					"conn_id", conn.ID(),
					"message", msg.String(),
					"error", uncacheErr,
				)
			}
		}
	}
	return err
}

func (h *Handler) authenticated(conn synp.Conn) bool {
	select {
	case <-conn.Authenticated():
		return true
	default:
		return false
	}
}

// decodePayload 解析 payload。
// 注：
//
//	这里解析的是整个消息字节流，消息体 ( body 字段 ) 需要另外解析。
func (h *Handler) decodePayload(payload []byte) (*messagev1.Message, error) {
	msg := &messagev1.Message{}
	if err := h.codec.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("%w: unknown message type", ErrInvalidMessage)
	}

	if msg.GetCommandType() != commonv1.CommandType_COMMAND_TYPE_HEARTBEAT && msg.GetMessageId() == "" {
		// 非心跳消息，message_id 不能为空。
		return nil, fmt.Errorf("%w: empty message_id", ErrInvalidMessage)
	}

	return msg, nil
}

// cacheMessage 缓存消息。
func (h *Handler) cacheMessage(bid, uid uint64, msg *messagev1.Message) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), h.cacheRequestTimeout)
	defer cancel()

	start := time.Now()
	defer func() {
		metrics.RedisDedupDuration.Observe(time.Since(start).Seconds())
	}()

	return h.rdb.SetNX(
		ctx,
		h.cacheKey(bid, uid, msg.GetMessageId()),
		msg.GetMessageId(),
		h.cacheExpiration,
	).Result()
}

func (h *Handler) needUncacheMessage(err error) bool {
	return errors.Is(err, ErrUnknownMessageType) || errors.Is(err, ErrMaxRetryExceeded)
}

// uncacheMessage 删除消息缓存。
func (h *Handler) uncacheMessage(bid, uid uint64, msg *messagev1.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), h.cacheRequestTimeout)
	defer cancel()

	if _, err := h.rdb.Del(ctx, h.cacheKey(bid, uid, msg.MessageId)).Result(); err != nil {
		return fmt.Errorf("%w: %w", ErrUncacheMessage, err)
	}

	return nil
}

func (h *Handler) cacheKey(bid, uid uint64, messageID string) string {
	return fmt.Sprintf("%d:%d:%s", bid, uid, messageID)
}

// OnReceiveFromBackend 处理收到后端通知消息的事件，向客户端下发 NOTIFY 帧。
func (h *Handler) OnReceiveFromBackend(ctx context.Context, conns []synp.Conn, msg *messagev1.NotifyMessage) error {
	if msg.GetMessageId() == "" {
		return fmt.Errorf("%w: empty message_id", ErrInvalidMessage)
	}
	if msg.GetReceiverId() == 0 {
		return fmt.Errorf("%w: empty receiver_id", ErrInvalidMessage)
	}

	return h.nMsgHandler.Handle(ctx, conns, msg)
}
