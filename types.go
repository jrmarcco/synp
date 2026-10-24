package synp

import (
	"context"
	"errors"
	"net"
	"time"

	"go.uber.org/multierr"

	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/compression"
	"github.com/jrmarcco/synp/internal/pkg/session"
)

var ErrRateLimited = errors.New("request too frequently, please try again later")

// ErrSendEnqueueTimeout 表示出站帧在 sendChan 满时等待入队超时。
// 慢客户端保护: 调用方 ( 通知推送等 ) 可据此丢弃可丢帧而非无限阻塞。
var ErrSendEnqueueTimeout = errors.New("send enqueue timeout")

//go:generate mockgen -source=types.go -destination=mock/synp.mock.go -package=synpmock -typed

type Server interface {
	Start() error
	Shutdown() error
	GracefulShutdown() error
}

// Upgrader 是连接升级器，用于将 HTTP 连接升级为 WebSocket 连接。
type Upgrader interface {
	Name() string
	Upgrade(conn net.Conn) (session.Session, *compression.State, error)
}

// Conn 是用户连接的抽象，封装了底层的网络连接 ( 如 WebSocket、TCP 连接 ) 。
type Conn interface {
	ID() string
	Session() session.Session

	// BindSession 在首帧 AUTH 鉴权成功后绑定真实身份。
	BindSession(sess session.Session)

	// Authenticated 返回鉴权完成信号，channel 关闭表示已完成鉴权。
	Authenticated() <-chan struct{}

	// MarkAuthenticated 标记连接鉴权完成。
	MarkAuthenticated()

	// Send 将 payload 入队发送。
	// ctx 控制入队等待——sendChan 满时最多等待至 ctx 超时，
	// 避免慢客户端阻塞调用方 ( 如 Kafka 消费循环 )。
	Send(ctx context.Context, payload []byte) error
	Receive() <-chan []byte

	UpdateActivityTime()

	TryCloseIfIdle(idleTimeout time.Duration) bool

	// TryCloseIfSlow 检查出站队列持续超时的慢客户端并断开。
	// 入队连续超时次数达到 minOverflow 时关闭连接。
	TryCloseIfSlow(minOverflow int32) bool

	Close() error

	Closed() <-chan struct{}
}

// ConnNode 是连接节点，用于表示用户连接所在的网关节点。
type ConnNode struct {
	ServiceName string
	ServiceAddr string
}

// ConnRegistry 是连接注册表，用于管理连接的生命周期。
// 需要支持多设备场景，一个用户可以在多个网关节点上有连接。
type ConnRegistry interface {
	// Register 注册用户连接到当前网关节点 ( 连接粒度路由记录 )。
	Register(ctx context.Context, user session.User, connID string) error

	// Unregister 注销用户连接 ( 不影响同节点其它连接的路由记录 )。
	Unregister(ctx context.Context, user session.User, connID string) error

	// FindNodes 查找用户所有连接所在的网关节点。
	FindNodes(ctx context.Context, bid, uid uint64) ([]ConnNode, error)

	// Heartbeat 心跳，刷新连接路由记录的过期时间。
	Heartbeat(ctx context.Context, user session.User, connID string) error
}

// ConnManager 是连接管理器，用于管理连接的生命周期。
type ConnManager interface {
	NewConn(ctx context.Context, netConn net.Conn, sess session.Session, compressionState *compression.State) (Conn, error)

	// BindUser 将完成首帧 AUTH 鉴权的连接绑定到用户索引。
	BindUser(conn Conn, user session.User) error

	RemoveConn(user session.User) bool
	RemoveUserConn(user session.User) bool

	// RemoveConnByID 按连接 ID 移除连接，连接断开的统一清理入口。
	RemoveConnByID(connID string) bool

	FindConn(user session.User) (Conn, bool)
	FindUserConn(user session.User) ([]Conn, bool)

	// RangeConns 遍历当前节点全部连接 ( 含未鉴权连接 )，
	// fn 返回 false 时提前终止。供关停排空等全量巡检使用。
	RangeConns(fn func(Conn) bool)

	// RangeConnsShard 分片遍历 ( 仅调用 hash(connID)%shards==shard 的连接 )，
	// 供周期性巡检摊薄单次扫描开销。
	RangeConnsShard(shard, shards uint64, fn func(Conn) bool)

	// ConnCount 返回当前连接总数 ( 含未鉴权连接 )。
	ConnCount() int64

	// UserCount 返回当前在线用户数。
	UserCount() int64
}

// Handler 是连接生命周期相关事件的回调接口。
type Handler interface {
	OnConnect(conn Conn) error
	OnDisconnect(conn Conn) error

	// OnReceiveFromFrontend 收到后端（业务服务端）消息的回调。
	OnReceiveFromFrontend(conn Conn, payload []byte) error

	// OnReceiveFromBackend 收到后端通知消息的回调，用于向客户端下发 NOTIFY 帧。
	// ctx 携带消费侧的处理超时，一路传递到发送路径。
	OnReceiveFromBackend(ctx context.Context, conns []Conn, notifyMsg *messagev1.NotifyMessage) error
}

var _ Handler = (*HandlerWrapper)(nil)

// HandlerWrapper 是 Handler 的包装器，用于组合多个 Handler。
type HandlerWrapper struct {
	handlers []Handler
}

func NewHandlerWrapper(handlers ...Handler) *HandlerWrapper {
	return &HandlerWrapper{
		handlers: handlers,
	}
}

func (w *HandlerWrapper) OnConnect(conn Conn) error {
	var err error
	for _, handler := range w.handlers {
		err = multierr.Append(err, handler.OnConnect(conn))
	}
	return err
}

func (w *HandlerWrapper) OnDisconnect(conn Conn) error {
	var err error
	for _, handler := range w.handlers {
		err = multierr.Append(err, handler.OnDisconnect(conn))
	}
	return err
}

func (w *HandlerWrapper) OnReceiveFromFrontend(conn Conn, payload []byte) error {
	var err error
	for _, handler := range w.handlers {
		handleErr := handler.OnReceiveFromFrontend(conn, payload)
		if errors.Is(handleErr, ErrRateLimited) {
			// 限流直接中断。
			return nil
		}
		err = multierr.Append(err, handleErr)
	}
	return err
}

func (w *HandlerWrapper) OnReceiveFromBackend(ctx context.Context, conns []Conn, notifyMsg *messagev1.NotifyMessage) error {
	var err error
	for _, handler := range w.handlers {
		err = multierr.Append(err, handler.OnReceiveFromBackend(ctx, conns, notifyMsg))
	}
	return err
}
