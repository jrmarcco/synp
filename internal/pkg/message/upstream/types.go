package upstream

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// defaultReplyEnqueueTimeout 为 *_ACK 应答帧发送入队等待的默认上限。
// 超时后 ACK 失败由客户端重试兜底，不应阻塞消息处理循环。
const defaultReplyEnqueueTimeout = 200 * time.Millisecond

// replyCtx 返回应答帧发送用的入队超时 context。
func replyCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultReplyEnqueueTimeout)
}

// sanitizeSyncError 将后端同步服务的 gRPC 错误转换为客户端可见文案。
//
// 仅透出后端有意声明的业务错误 ( InvalidArgument / PermissionDenied 的
// status message )；超时与内部错误只回通用文案——
// 原始错误文本 ( 可能含内部实现细节 ) 仅进服务端日志。
func sanitizeSyncError(prefix string, err error) string {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument, codes.PermissionDenied:
			return fmt.Sprintf("%s: %s", prefix, st.Message())
		case codes.DeadlineExceeded, codes.Canceled:
			return fmt.Sprintf("%s: timed out, please retry", prefix)
		default:
			return fmt.Sprintf("%s: internal error, please retry", prefix)
		}
	}
	return fmt.Sprintf("%s: internal error, please retry", prefix)
}
