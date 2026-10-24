package notify

import (
	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"google.golang.org/protobuf/encoding/protojson"
)

var _ NMsgHandler = (*NotifyMsgHandler)(nil)

// NotifyMsgHandler 是后端通知消息处理器，
// 将 Kafka 通知 topic / NotifyService 的 NotifyMessage 转换为 NOTIFY 帧下发。
//
// NOTIFY 帧只携带最新 seq，不携带消息本体——
// 通知丢失只影响实时性，不影响正确性 ( 客户端重连/心跳时主动 SYNC 兜底 )，
// 因此不做重传。
type NotifyMsgHandler struct {
	pushFunc message.PushFunc
}

func NewNotifyMsgHandler(pushFunc message.PushFunc) *NotifyMsgHandler {
	return &NotifyMsgHandler{
		pushFunc: pushFunc,
	}
}

func (h *NotifyMsgHandler) Handle(conns []synp.Conn, notifyMsg *messagev1.NotifyMessage) error {
	body, err := protojson.Marshal(&messagev1.NotifyPayload{
		LatestSeq: notifyMsg.GetLatestSeq(),
	})
	if err != nil {
		return err
	}

	// NOTIFY 帧的 message_id 复用 NotifyMessage 的 message_id，仅用于日志排查。
	notifyFrame := &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_NOTIFY,
		MessageId:   notifyMsg.GetMessageId(),
		Body:        body,
	}

	for _, conn := range conns {
		if err = h.pushFunc(conn, notifyFrame); err != nil {
			return err
		}

		// 成功发送消息到前端，更新连接活跃时间。
		conn.UpdateActivityTime()
	}
	return nil
}
