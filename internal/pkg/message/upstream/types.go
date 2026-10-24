package upstream

import (
	"context"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
)

//go:generate mockgen -source=types.go -destination=mock/upstream.mock.go -package=upstreammock -typed Handler

// UMsgHandler 是 upstream 消息处理器的接口。
type UMsgHandler interface {
	// Handle 处理消息。
	// 注：
	//	Handle 方法没有 context.Context 参数是因为消息处理通常在 synp.Conn 的上下文中进行。
	Handle(conn synp.Conn, msg *messagev1.Message) error

	// CmdType 返回消息类型。
	CmdType() commonv1.CommandType
}

// SyncCaller 是对后端 SyncService ( proto/message/v1/sync.proto ) 的调用抽象。
// 同步请求经 WS 帧到达网关后，由网关以连接身份盖章调用，
// 应答原样放入 *_ACK 帧返回——客户端与后端之间不建立第二条通道。
type SyncCaller interface {
	// Sync 返回该用户收件箱中 ( cursor, 服务端最大 seq ] 的增量条目。
	Sync(ctx context.Context, req *messagev1.SyncRequest) (*messagev1.SyncResponse, error)

	// ConversationSync 返回指定会话 timeline 的增量消息，用于读扩散 ( 大群 ) 场景。
	ConversationSync(ctx context.Context, req *messagev1.ConversationSyncRequest) (*messagev1.ConversationSyncResponse, error)
}
