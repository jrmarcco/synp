package notify

import (
	"context"

	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
)

//go:generate mockgen -source=types.go -destination=mock/notify.mock.go -package=notifymock -typed Handler

// NMsgHandler 是 notify ( 下行通知 ) 消息处理器的接口。
type NMsgHandler interface {
	// Handle 处理消息，ctx 携带消费侧处理超时。
	// 单个连接的发送入队等待受 enqueueTimeout 限制，
	// 超时丢弃该连接的 NOTIFY 帧 ( 通知可丢，仅影响实时性 )。
	Handle(ctx context.Context, conns []synp.Conn, msg *messagev1.NotifyMessage) error
}
