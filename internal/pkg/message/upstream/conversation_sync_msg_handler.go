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
	"google.golang.org/protobuf/encoding/protojson"
)

var _ UMsgHandler = (*ConversationSyncMsgHandler)(nil)

// ConversationSyncMsgHandler 是会话 timeline 增量同步 ( CONVERSATION_SYNC ) 处理器的实现。
// 用于读扩散 ( 大群 ) 场景拉取会话 timeline。
type ConversationSyncMsgHandler struct {
	syncCaller SyncCaller

	syncTimeout time.Duration

	pushFunc message.PushFunc
}

func NewConversationSyncMsgHandler(syncCaller SyncCaller, syncTimeout time.Duration, pushFunc message.PushFunc) *ConversationSyncMsgHandler {
	if syncTimeout <= 0 {
		syncTimeout = defaultSyncTimeout
	}

	return &ConversationSyncMsgHandler{
		syncCaller:  syncCaller,
		syncTimeout: syncTimeout,
		pushFunc:    pushFunc,
	}
}

func (h *ConversationSyncMsgHandler) Handle(conn synp.Conn, msg *messagev1.Message) error {
	conn.UpdateActivityTime()

	replyCtx, replyCancel := replyCtx()
	defer replyCancel()

	frame := &messagev1.ConversationSyncFrame{}
	if err := protojson.Unmarshal(msg.GetBody(), frame); err != nil {
		return h.replyAck(replyCtx, conn, msg.GetMessageId(), &messagev1.ConversationSyncAckPayload{
			IsSuccess:    false,
			ErrorMessage: fmt.Sprintf("invalid conversation sync frame: %v", err),
		})
	}

	// 身份字段由网关以连接鉴权结果盖章。
	user := conn.Session().User()

	ctx, cancel := context.WithTimeout(context.Background(), h.syncTimeout)
	defer cancel()

	start := time.Now()
	resp, err := h.syncCaller.ConversationSync(ctx, &messagev1.ConversationSyncRequest{
		BusinessId:     user.BID,
		UserId:         user.UID,
		ConversationId: frame.GetConversationId(),

		Cursor: frame.GetCursor(),
		Limit:  frame.GetLimit(),
	})
	metrics.SyncCallDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		logging.For("conversation-sync-msg-handler").Error(
			"[conversation-sync-msg-handler] failed to call conversation sync service",
			"conn_id", conn.ID(),
			"conversation_id", frame.GetConversationId(),
			"error", err,
		)
		return h.replyAck(replyCtx, conn, msg.GetMessageId(), &messagev1.ConversationSyncAckPayload{
			IsSuccess:    false,
			ErrorMessage: sanitizeSyncError("failed to conversation sync", err),
		})
	}

	return h.replyAck(replyCtx, conn, msg.GetMessageId(), &messagev1.ConversationSyncAckPayload{
		IsSuccess: true,
		Response:  resp,
	})
}

func (h *ConversationSyncMsgHandler) replyAck(ctx context.Context, conn synp.Conn, messageID string, payload *messagev1.ConversationSyncAckPayload) error {
	body, err := message.MarshalBody(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal conversation sync ack payload: %w", err)
	}

	return h.pushFunc(ctx, conn, &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_CONVERSATION_SYNC_ACK,
		MessageId:   messageID,
		Body:        body,
	})
}

func (h *ConversationSyncMsgHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_CONVERSATION_SYNC
}
