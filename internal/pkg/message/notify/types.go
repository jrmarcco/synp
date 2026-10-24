package notify

import (
	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
)

//go:generate mockgen -source=types.go -destination=mock/notify.mock.go -package=notifymock -typed Handler

// NMsgHandler 是 notify ( 下行通知 ) 消息处理器的接口。
type NMsgHandler interface {
	// Handle 处理消息。
	// 注：
	//	Handle 方法没有 context.Context 参数是因为消息处理通常在 synp.Conn 的上下文中进行。
	Handle(conns []synp.Conn, msg *messagev1.NotifyMessage) error
}
