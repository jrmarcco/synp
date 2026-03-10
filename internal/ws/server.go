package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/jrmarcco/jit/bean/option"
	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/limiter"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/jrmarcco/synp/internal/pkg/xmq"
	wsc "github.com/jrmarcco/synp/internal/ws/conn"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"go.uber.org/zap"
)

var ErrUnknownReceiver = errors.New("unknown receiver")

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

	acceptNewConn atomic.Bool

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

	// 初始化网关业务消息消费者。
	for key := range s.consumers {
		switch key {
		case gateway.EventPushMessage:
			consumer, ok := s.consumers[key]
			if !ok {
				s.logger.Warn("[server] consumer not found", zap.String("event", key))
				continue
			}
			if err := consumer.StartConsumer(s.ctx, s.consumePushMessage); err != nil {
				s.logger.Error(
					"[server] failed to start push message consumer",
					zap.Error(err),
				)
				return err
			}
		case gateway.EventScaleUp:
			consumer, ok := s.consumers[key]
			if !ok {
				s.logger.Warn("[server] consumer not found", zap.String("event", key))
				continue
			}
			if err := consumer.StartConsumer(s.ctx, s.consumeScaleUp); err != nil {
				s.logger.Error(
					"[server] failed to start scale up consumer",
					zap.Error(err),
				)
				return err
			}
		}
	}

	return nil
}

// acceptConn 接收 WebSocket 连接。
func (s *Server) acceptConn() {
	for {
		s.logger.Debug("[server] accepting new connection")

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
	synpConn, err := s.connManager.NewConn(s.ctx, conn, sess, compressionState)
	if err != nil {
		s.logger.Error("[server] failed to create synp connection", zap.Error(err))
		return
	}

	user := synpConn.Session().User()
	defer func() {
		s.connManager.RemoveConn(user)
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

	// 处理收发消息。
	for {
		select {
		case message, ok := <-synpConn.Receive():
			if !ok {
				return
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
			}
		case <-synpConn.Closed():
			s.logger.Info("[server] synp connection has been closed")
			return
		case <-s.ctx.Done():
			s.logger.Info("[server] server has been closed")
			return
		}
	}
}

// consumePushMessage 消费 push message 事件。
func (s *Server) consumePushMessage(_ context.Context, msg *xmq.Message) error {
	pushMsg := &messagev1.PushMessage{}
	// 后端 ( 业务服务端 ) 推送到消息队列的消息必须使用 json 格式，
	// 所以直接使用 json 进行解码即可。
	if err := json.Unmarshal(msg.Val, pushMsg); err != nil {
		s.logger.Error(
			"[server] failed to unmarshal push message",
			zap.String("message", string(msg.Val)),
			zap.Error(err),
		)
		return err
	}

	conns, err := s.findConns(pushMsg)
	if err != nil {
		s.logger.Error(
			"[server] failed to find connection for user",
			zap.String("message", string(msg.Val)),
			zap.Error(err),
		)
		return err
	}

	if err = s.connHandler.OnReceiveFromBackend(conns, pushMsg); err != nil {
		s.logger.Error(
			"[server] failed to handle on receive from backend event",
			zap.String("message", string(msg.Val)),
			zap.Error(err),
		)
		return err
	}
	return nil
}

// findConns 根据下行消息查找对应的连接。
// msg 是网关接收到的下行消息 ( 由业务服务端推送至网关 )。
func (s *Server) findConns(msg *messagev1.PushMessage) ([]synp.Conn, error) {
	conns, ok := s.connManager.FindUserConn(session.User{
		BID: msg.GetBizId(),
		UID: msg.GetReceiverId(),
	})
	if !ok {
		return nil, fmt.Errorf("%w: user_id=%d, biz_id=%d", ErrUnknownReceiver, msg.GetReceiverId(), msg.GetBizId())
	}
	return conns, nil
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
