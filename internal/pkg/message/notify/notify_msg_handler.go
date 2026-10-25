package notify

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
)

// defaultEnqueueTimeout 为 NOTIFY 帧发送入队等待的默认上限。
const defaultEnqueueTimeout = 200 * time.Millisecond

var _ NMsgHandler = (*NotifyMsgHandler)(nil)

// NotifyMsgHandler 是后端通知消息处理器，
// 将 Kafka 通知 topic / NotifyService 的 NotifyMessage 转换为 NOTIFY 帧下发。
//
// NOTIFY 帧只携带最新 seq，不携带消息本体——
// 通知丢失只影响实时性，不影响正确性 ( 客户端重连/心跳时主动 SYNC 兜底 )，
// 因此不做重传。
type NotifyMsgHandler struct {
	pushFunc message.PushFunc

	// enqueueTimeout 为单个连接的发送入队等待上限，
	// 超时丢弃该连接的 NOTIFY 帧，避免慢客户端阻塞消费循环。
	enqueueTimeout time.Duration
}

func NewNotifyMsgHandler(pushFunc message.PushFunc, enqueueTimeout time.Duration) *NotifyMsgHandler {
	if enqueueTimeout <= 0 {
		enqueueTimeout = defaultEnqueueTimeout
	}
	return &NotifyMsgHandler{
		pushFunc:       pushFunc,
		enqueueTimeout: enqueueTimeout,
	}
}

func (h *NotifyMsgHandler) Handle(ctx context.Context, conns []synp.Conn, notifyMsg *messagev1.NotifyMessage) error {
	body, err := message.MarshalBody(&messagev1.NotifyPayload{
		LatestSeq: notifyMsg.GetLatestSeq(),
	})
	if err != nil {
		return err
	}

	// NOTIFY 帧的 message_id 每次下发独立生成:
	// 帧唯一 ID 语义要求不可复用触发消息的 UPSTREAM 帧 ID，
	// 否则客户端无法以 NOTIFY.messageId 做帧关联/去重
	// ( 同一条消息向多端下发时 NOTIFY 帧并非唯一 )。
	frameID, err := newFrameID()
	if err != nil {
		return err
	}

	notifyFrame := &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_NOTIFY,
		MessageId:   frameID,
		Body:        body,
	}

	for _, conn := range conns {
		// 入队等待同时受消费侧处理超时与单连接 enqueueTimeout 约束。
		enqueueCtx, cancel := context.WithTimeout(ctx, h.enqueueTimeout)

		if err = h.pushFunc(enqueueCtx, conn, notifyFrame); err != nil {
			cancel()

			// NOTIFY 允许丢 ( 客户端重连/心跳时主动 SYNC 兜底 )，
			// 超时只计数与记日志，不阻塞其它连接、不中断消费。
			if errors.Is(err, synp.ErrSendEnqueueTimeout) {
				metrics.NotifyDropTotal.Inc()
				logging.For("notify-msg-handler").Warn(
					"[notify-msg-handler] notify frame dropped due to enqueue timeout",
					"conn_id", conn.ID(),
					"message_id", notifyMsg.GetMessageId(),
				)
				continue
			}
			return err
		}
		cancel()

		// 成功发送消息到前端，更新连接活跃时间。
		conn.UpdateActivityTime()
	}
	return nil
}

// newFrameID 生成 NOTIFY 下行帧的唯一 ID ( 随机 hex，仅用于日志排查 )。
func newFrameID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
