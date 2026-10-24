package ws

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/limiter"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/jrmarcco/synp/internal/pkg/xmq"
	wsc "github.com/jrmarcco/synp/internal/ws/conn"
	"github.com/jrmarcco/synp/internal/ws/conn/lifecycle"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

// defaultAuthTimeout 为连接建立后等待首帧 AUTH 的默认超时时间。
const defaultAuthTimeout = 15 * time.Second

var _ synp.Server = (*Server)(nil)

type Server struct {
	config *Config

	listener net.Listener
	upgrader synp.Upgrader

	connHandler synp.Handler
	connManager synp.ConnManager

	consumers map[string]*gateway.Consumer

	connLimiter *limiter.TokenLimiter
	backoff     *backoff.ExponentialBackOff

	acceptNewConn    atomic.Bool
	idleScanInterval time.Duration // TODO: 空闲连接扫描间隔 还未配置完成

	// authTimeout 为连接建立后等待首帧 AUTH 的超时时间，
	// 超时未鉴权的连接由网关关闭。
	authTimeout time.Duration

	ctx        context.Context
	cancelFunc context.CancelFunc

	logger *zap.Logger
}

func NewServer(
	config *Config,
	upgrader synp.Upgrader,
	connHandler synp.Handler,
	connManager synp.ConnManager,
	consumers map[string]*gateway.Consumer,
	logger *zap.Logger,
	opts ...option.Opt[Server],
) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		config: config,

		upgrader:    upgrader,
		connHandler: connHandler,
		connManager: connManager,

		connLimiter: limiter.NewTokenLimiter(limiter.DefaultConfig()), // 默认令牌桶限流器
		backoff:     backoff.NewExponentialBackOff(),                  // 默认退避策略

		consumers: consumers,

		authTimeout: defaultAuthTimeout,

		ctx:        ctx,
		cancelFunc: cancel,

		logger: logger,
	}
	s.acceptNewConn.Store(true)

	option.Apply(s, opts...)
	return s
}

// Start 启动 WebSocket 服务器。
func (s *Server) Start() error {
	// 端口启用并监听 upgrade 请求，完成 WebSocket 的初始化工作。
	lc := net.ListenConfig{}
	ln, err := lc.Listen(s.ctx, s.config.Network, s.config.Address())
	if err != nil {
		return err
	}

	s.logger.Info("[server] ws server started", zap.String("addr", s.config.Address()))
	s.listener = ln

	go s.acceptConn()
	go s.cleanIdleConn()

	// 初始化网关业务消息消费者。
	if err := s.initConsumers(); err != nil {
		s.logger.Error("[server] failed to initialize consumers", zap.Error(err))
		return err
	}
	return nil
}

// acceptConn 接收 WebSocket 连接。
func (s *Server) acceptConn() {
	for {
		s.logger.Debug("[server] starting to accept new connection")

		// 判断是否接收新连接。
		if !s.acceptNewConn.Load() {
			s.logger.Info("[server] server is not accepting new connections")
			return
		}

		// 接收连接前先获取令牌。
		if !s.connLimiter.Acquire() {
			next := s.backoff.NextBackOff()

			s.logger.Warn(
				"[server] connection limit reached, reject new connection",
				zap.Duration("next_backoff", next),
			)
			time.Sleep(next)
			continue
		}

		// 成功获取令牌，重置退避策略。
		s.backoff.Reset()

		conn, err := s.listener.Accept()
		if err != nil {
			// 接收连接失败，归还令牌。
			s.connLimiter.Release()

			s.logger.Error("[server] failed to accept connection", zap.Error(err))
			if errors.Is(err, net.ErrClosed) {
				// net.ErrClosed 表示 listener 已关闭，此时可以退出。
				return
			}

			var netOpErr *net.OpError
			if errors.As(err, &netOpErr) && (netOpErr.Timeout() || netOpErr.Temporary()) {
				// 如果是 timeout 或 temporary 错误，那么可以继续尝试接收连接。
				continue
			}

			// 确保循环继续而不是意外退出。
			continue
		}

		// 在追求极致性能的情况下，这里可以考虑使用连接池。
		// 在绝大多数情况下，goroutine : connection = 1 : 1 即可。
		go s.handleConn(conn)
	}
}

// handleConn 处理 WebSocket 连接。
func (s *Server) handleConn(conn net.Conn) {
	// 归还令牌。
	defer s.connLimiter.Release()

	// 关闭连接。
	defer func() {
		err := conn.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Warn("[server] failed to close connection", zap.Error(err))
		}
	}()

	// 处理 upgrade 请求。
	sess, compressionState, err := s.upgrader.Upgrade(conn)
	if err != nil {
		s.logger.Error(
			"[server] failed to upgrade connection from HTTP to WebSocket",
			zap.Error(err),
		)
		return
	}

	// 创建、管理连接。
	// 注意这里的连接指的是 synp.Conn 而不是 net.Conn 接口。
	// 新协议下连接建立时身份未知 ( 首帧 AUTH 完成鉴权 )，创建的是匿名连接。
	synpConn, err := s.connManager.NewConn(s.ctx, conn, sess, compressionState)
	if err != nil {
		s.logger.Error("[server] failed to create synp connection", zap.Error(err))
		return
	}

	defer func() {
		// 匿名连接与已绑定连接统一按连接 ID 清理。
		s.connManager.RemoveConnByID(synpConn.ID())
		if err := synpConn.Close(); err != nil {
			s.logger.Error(
				"[server] failed to close synp connection",
				zap.String("conn_id", synpConn.ID()),
				zap.Error(err),
			)
		}
	}()

	// 处理 on connect & on disconnect 事件。
	if err := s.connHandler.OnConnect(synpConn); err != nil {
		s.logger.Error(
			"[server] failed to handle on connect lifecycle event",
			zap.String("conn_id", synpConn.ID()),
			zap.Error(err),
		)
		// on connect 事件失败，直接返回。
		// 这里最好直接返回以防止后续的事件处理发生不可预料的错误。
		return
	}
	defer func() {
		if err := s.connHandler.OnDisconnect(synpConn); err != nil {
			s.logger.Error(
				"[server] failed to handle on disconnect lifecycle event",
				zap.String("conn_id", synpConn.ID()),
				zap.Error(err),
			)
		}
	}()

	// 等待首帧 AUTH 超时定时器。
	// 鉴权完成后停止；超时未鉴权的连接由网关关闭。
	authTimer := time.NewTimer(s.authTimeout)
	defer authTimer.Stop()
	stopAuthTimer := sync.OnceFunc(func() { _ = authTimer.Stop() })

	// 处理收发消息。
	for {
		select {
		case message, ok := <-synpConn.Receive():
			if !ok {
				return
			}

			// 鉴权完成后不再关心 AUTH 超时。
			select {
			case <-synpConn.Authenticated():
				stopAuthTimer()
			default:
			}

			if err := s.connHandler.OnReceiveFromFrontend(synpConn, message); err != nil {
				// 处理前端（业务客户端）发送的消息失败。
				s.logger.Error(
					"[server] failed to handle on receive from frontend event",
					zap.Error(err),
				)

				// 如果连接已关闭，则直接返回 ( wsc => internal/ws/conn )。
				if errors.Is(err, wsc.ErrConnClosed) {
					return
				}

				// 鉴权失败或未鉴权连接发送了非法指令，直接关闭连接。
				if errors.Is(err, lifecycle.ErrNotAuthenticated) {
					return
				}

				// TODO:
				// 如果是消息重复是否需要再发送 ACK 消息？
				// 还是返回特定的重复错误？
				// 目前做法是直接忽略消息。
			}
		case <-authTimer.C:
			// 双重检查：鉴权可能刚刚完成。
			select {
			case <-synpConn.Authenticated():
				continue
			default:
			}

			s.logger.Info(
				"[server] close connection because of auth timeout",
				zap.String("conn_id", synpConn.ID()),
			)
			return
		case <-synpConn.Closed():
			s.logger.Info("[server] synp connection has been closed")
			return
		case <-s.ctx.Done():
			s.logger.Info("[server] server has been closed")
			return
		}
	}
}

// cleanIdleConn 清理空闲连接。
func (s *Server) cleanIdleConn() {
	ticker := time.NewTicker(s.idleScanInterval)
	defer ticker.Stop()

	s.logger.Debug("[server] idle connection cleaner started")

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("[server] idle connection cleaner stopped by context done")
			return
		case <-ticker.C:
			// TODO:关闭所有空闲连接。
		}
	}
}

// initConsumers 初始化网关业务消息消费者。
func (s *Server) initConsumers() error {
	for key := range s.consumers {
		switch key {
		case gateway.EventNotifyMessage:
			consumer, ok := s.consumers[key]
			if !ok {
				s.logger.Warn("[server] consumer not found", zap.String("event", key))
				continue
			}
			if err := consumer.StartConsumer(s.ctx, s.consumeNotifyMessage); err != nil {
				s.logger.Error("[server] failed to start notify message consumer", zap.Error(err))
				return err
			}
		case gateway.EventScaleUp:
			consumer, ok := s.consumers[key]
			if !ok {
				s.logger.Warn("[server] consumer not found", zap.String("event", key))
				continue
			}
			if err := consumer.StartConsumer(s.ctx, s.consumeScaleUp); err != nil {
				s.logger.Error("[server] failed to start scale up consumer", zap.Error(err))
				return err
			}
		}
	}
	return nil
}

// consumeNotifyMessage 消费通知 topic 的 NotifyMessage 事件。
//
// 通知 topic 为广播消费：每个网关节点独立消费全量，
// 按本地连接过滤后下发，节点上无该用户连接时忽略 ( 通知丢失只影响实时性 )。
func (s *Server) consumeNotifyMessage(_ context.Context, msg *xmq.Message) error {
	notifyMsg := &messagev1.NotifyMessage{}
	// 后端 ( 业务服务端 ) 投递到通知 topic 的记录使用 protojson 编码。
	if err := protojson.Unmarshal(msg.Val, notifyMsg); err != nil {
		s.logger.Error(
			"[server] failed to unmarshal notify message",
			zap.String("message", string(msg.Val)),
			zap.Error(err),
		)
		return err
	}

	conns, ok := s.connManager.FindUserConn(session.User{
		BID: notifyMsg.GetBusinessId(),
		UID: notifyMsg.GetReceiverId(),
	})
	if !ok {
		// 本节点无该用户连接，忽略。
		// 广播模型下其它节点会向其本地连接下发。
		s.logger.Debug(
			"[server] receiver not on this node, ignore notify message",
			zap.Uint64("business_id", notifyMsg.GetBusinessId()),
			zap.Uint64("receiver_id", notifyMsg.GetReceiverId()),
		)
		return nil
	}

	if err := s.connHandler.OnReceiveFromBackend(conns, notifyMsg); err != nil {
		s.logger.Error(
			"[server] failed to handle on receive from backend event",
			zap.String("message", string(msg.Val)),
			zap.Error(err),
		)
		return err
	}
	return nil
}

// consumeScaleUp 消费 scale up 事件。
func (s *Server) consumeScaleUp(_ context.Context, _ *xmq.Message) error {
	// TODO: not implemented
	panic("not implemented")
}

func (s *Server) Shutdown() error {
	s.logger.Info("[server] shutting down ws server")
	// 关闭限流器。
	if err := s.connLimiter.Close(); err != nil {
		s.logger.Error("[server] failed to close connection limiter", zap.Error(err))
		return err
	}

	s.cancelFunc()
	s.logger.Info("[server] ws server shut down")
	return nil
}

func (s *Server) GracefulShutdown() error {
	s.logger.Info("[server] graceful shutting down ws server")

	s.acceptNewConn.Store(false)
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Warn("[server] failed to close net listener", zap.Error(err))
		}
	}

	// TODO: 优雅关闭。

	<-s.ctx.Done()
	return s.Shutdown()
}
