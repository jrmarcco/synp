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

	Send(payload []byte) error
	Receive() <-chan []byte

	UpdateActivityTime()

	TryCloseIfIdle(idleTimeout time.Duration) bool

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
	// Register 注册用户连接到当前网关节点。
	Register(ctx context.Context, user session.User) error

	// Unregister 注销用户连接。
	Unregister(ctx context.Context, user session.User) error

	// FindNodes 查找用户所有连接所在的网关节点。
	FindNodes(ctx context.Context, bid, uid uint64) ([]ConnNode, error)

	// Heartbeat 心跳，防止连接注册信息过期。
	Heartbeat(ctx context.Context, user session.User) error
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
}

// Handler 是连接生命周期相关事件的回调接口。
type Handler interface {
	OnConnect(conn Conn) error
	OnDisconnect(conn Conn) error

	// OnReceiveFromFrontend 收到后端（业务服务端）消息的回调。
	OnReceiveFromFrontend(conn Conn, payload []byte) error

	// OnReceiveFromBackend 收到前端（业务客户端）消息的回调，用于向客户端下发 NOTIFY 帧。
	OnReceiveFromBackend(conns []Conn, notifyMsg *messagev1.NotifyMessage) error
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

func (w *HandlerWrapper) OnReceiveFromBackend(conns []Conn, notifyMsg *messagev1.NotifyMessage) error {
	var err error
	for _, handler := range w.handlers {
		err = multierr.Append(err, handler.OnReceiveFromBackend(conns, notifyMsg))
	}
	return err
}
