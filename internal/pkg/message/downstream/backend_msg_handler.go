package downstream

import (
	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/retransmit"
)

var _ DMsgHandler = (*BackendMsgHandler)(nil)

// BackendMsgHandler 后端消息 ( 下行消息 ) 处理器，用于处理后端推送的消息。
type BackendMsgHandler struct {
	pushFunc          message.PushFunc
	retransmitManager *retransmit.Manager
}

func NewBackendMsgHandler(
	pushFunc message.PushFunc,
	retransmitManager *retransmit.Manager,
) *BackendMsgHandler {
	return &BackendMsgHandler{
		pushFunc:          pushFunc,
		retransmitManager: retransmitManager,
	}
}

func (h *BackendMsgHandler) Handle(conns []synp.Conn, pushMsg *messagev1.PushMessage) error {
	downstreamMsg := &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_DOWNSTREAM,
		MessageId:   pushMsg.GetMessageId(),
		Body:        pushMsg.GetBody(),
	}

	// 设置重试。
	// 前端返回 ack 消息后停止重试。
	defer h.retransmitManager.Start(conns, downstreamMsg)

	for _, conn := range conns {
		if err := h.pushFunc(conn, downstreamMsg); err != nil {
			return err
		}

		// 成功发送消息到前端，更新连接活跃时间。
		conn.UpdateActivityTime()
	}
	return nil
}
