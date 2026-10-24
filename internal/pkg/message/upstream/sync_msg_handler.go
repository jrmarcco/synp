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

const defaultSyncTimeout = 10 * time.Second

var _ UMsgHandler = (*SyncMsgHandler)(nil)

// SyncMsgHandler 是收件箱增量同步 ( SYNC ) 处理器的实现。
//
// SYNC 帧不含任何身份字段：网关以连接鉴权得到的身份盖章后调用后端 SyncService，
// 应答放入 SYNC_ACK 帧返回。
type SyncMsgHandler struct {
	syncCaller SyncCaller

	syncTimeout time.Duration

	pushFunc message.PushFunc
}

func NewSyncMsgHandler(syncCaller SyncCaller, syncTimeout time.Duration, pushFunc message.PushFunc) *SyncMsgHandler {
	if syncTimeout <= 0 {
		syncTimeout = defaultSyncTimeout
	}

	return &SyncMsgHandler{
		syncCaller:  syncCaller,
		syncTimeout: syncTimeout,
		pushFunc:    pushFunc,
	}
}

func (h *SyncMsgHandler) Handle(conn synp.Conn, msg *messagev1.Message) error {
	conn.UpdateActivityTime()

	replyCtx, replyCancel := replyCtx()
	defer replyCancel()

	frame := &messagev1.SyncFrame{}
	if err := protojson.Unmarshal(msg.GetBody(), frame); err != nil {
		return h.replySyncAck(replyCtx, conn, msg.GetMessageId(), &messagev1.SyncAckPayload{
			IsSuccess:    false,
			ErrorMessage: fmt.Sprintf("invalid sync frame: %v", err),
		})
	}

	// 身份字段由网关以连接鉴权结果盖章。
	user := conn.Session().User()

	ctx, cancel := context.WithTimeout(context.Background(), h.syncTimeout)
	defer cancel()

	start := time.Now()
	resp, err := h.syncCaller.Sync(ctx, &messagev1.SyncRequest{
		BusinessId: user.BID,
		UserId:     user.UID,

		Cursor: frame.GetCursor(),
		Limit:  frame.GetLimit(),
	})
	metrics.SyncCallDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		logging.For("sync-msg-handler").Error(
			"[sync-msg-handler] failed to call sync service",
			"conn_id", conn.ID(),
			"error", err,
		)
		return h.replySyncAck(replyCtx, conn, msg.GetMessageId(), &messagev1.SyncAckPayload{
			IsSuccess:    false,
			ErrorMessage: fmt.Sprintf("failed to sync: %v", err),
		})
	}

	return h.replySyncAck(replyCtx, conn, msg.GetMessageId(), &messagev1.SyncAckPayload{
		IsSuccess: true,
		Response:  resp,
	})
}

func (h *SyncMsgHandler) replySyncAck(ctx context.Context, conn synp.Conn, messageID string, payload *messagev1.SyncAckPayload) error {
	body, err := protojson.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal sync ack payload: %w", err)
	}

	return h.pushFunc(ctx, conn, &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_SYNC_ACK,
		MessageId:   messageID,
		Body:        body,
	})
}

func (h *SyncMsgHandler) CmdType() commonv1.CommandType {
	return commonv1.CommandType_COMMAND_TYPE_SYNC
}
