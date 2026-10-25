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
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/jrmarcco/synp/internal/pkg/xmq"
	wsc "github.com/jrmarcco/synp/internal/ws/conn"
	"github.com/jrmarcco/synp/internal/ws/conn/lifecycle"
	"github.com/jrmarcco/synp/internal/ws/gateway"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	// defaultAuthTimeout 为连接建立后等待首帧 AUTH 的默认超时时间。
	defaultAuthTimeout = 15 * time.Second

	// defaultIdleScanInterval / defaultIdleTimeout 为空闲连接扫描的默认参数。
	// 假设客户端每 30 秒一次心跳，2 分钟空闲超时可容忍连续 3~4 次心跳丢失。
	defaultIdleScanInterval = 30 * time.Second
	defaultIdleTimeout      = 2 * time.Minute

	// defaultDrainTimeout 为优雅关闭时等待存量连接排空的超时时间。
	defaultDrainTimeout = 10 * time.Second

	// drainPollInterval 为排空轮询间隔。
	drainPollInterval = 100 * time.Millisecond

	// slowCloseOverflowCnt 为慢客户端断开的入队连续超时阈值。
	// 默认扫描周期 30s 时约等于持续 ~90s 无法下发。
	slowCloseOverflowCnt = int32(3)
)

var _ synp.Server = (*Server)(nil)

type Server struct {
	config *Config

	listener net.Listener
	upgrader synp.Upgrader

	connHandler synp.Handler
	connManager synp.ConnManager

	consumers map[string]*gateway.Consumer

	connLimiter *limiter.ConnLimiter
	backoff     *backoff.ExponentialBackOff

	acceptNewConn atomic.Bool

	// authTimeout 为连接建立后等待首帧 AUTH 的超时时间，
	// 超时未鉴权的连接由网关关闭。
	authTimeout time.Duration

	// idleScanInterval / idleTimeout 为空闲连接扫描参数。
	idleScanInterval time.Duration
	idleTimeout      time.Duration

	// drainTimeout 为优雅关闭时等待存量连接排空的超时时间。
	drainTimeout time.Duration

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewServer(
	config *Config,
	upgrader synp.Upgrader,
	connHandler synp.Handler,
	connManager synp.ConnManager,
	consumers map[string]*gateway.Consumer,
	opts ...option.Opt[Server],
) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		config: config,

		upgrader:    upgrader,
		connHandler: connHandler,
		connManager: connManager,

		consumers: consumers,

		connLimiter: limiter.NewConnLimiter(0),
		backoff:     backoff.NewExponentialBackOff(),

		authTimeout:      defaultAuthTimeout,
		idleScanInterval: defaultIdleScanInterval,
		idleTimeout:      defaultIdleTimeout,
		drainTimeout:     defaultDrainTimeout,

		ctx:        ctx,
		cancelFunc: cancel,
	}
	s.acceptNewConn.Store(true)

	option.Apply(s, opts...)
	return s
}

// Start 启动 WebSocket 服务器。
func (s *Server) Start() error {
	// 参数校验: 非法的扫描/超时参数回落默认值，避免 NewTicker(0) 等 panic。
	s.normalizeParams()

	// 端口启用并监听 upgrade 请求，完成 WebSocket 的初始化工作。
	lc := net.ListenConfig{}
	ln, err := lc.Listen(s.ctx, s.config.Network, s.config.Address())
	if err != nil {
		return err
	}

	logging.For("server").Info("ws server started", "addr", s.config.Address())
	s.listener = ln

	go s.acceptConn()
	go s.cleanIdleConn()

	// 初始化网关业务消息消费者。
	if err := s.initConsumers(); err != nil {
		logging.For("server").Error("failed to initialize consumers", "error", err)
		return err
	}
	return nil
}

// normalizeParams 校验并回落服务参数默认值。
func (s *Server) normalizeParams() {
	if s.authTimeout <= 0 {
		s.authTimeout = defaultAuthTimeout
	}
	if s.idleScanInterval <= 0 {
		s.idleScanInterval = defaultIdleScanInterval
	}
	if s.idleTimeout <= s.idleScanInterval {
		s.idleTimeout = defaultIdleTimeout
	}
	if s.drainTimeout <= 0 {
		s.drainTimeout = defaultDrainTimeout
	}
}

// acceptConn 接收 WebSocket 连接。
func (s *Server) acceptConn() {
	for {
		logging.For("server").Debug("starting to accept new connection")

		// 判断是否接收新连接。
		if !s.acceptNewConn.Load() {
			logging.For("server").Info("server is not accepting new connections")
			return
		}

		// 接收连接前先检查连接数上限。
		if !s.connLimiter.Acquire() {
			next := s.backoff.NextBackOff()

			logging.For("server").Warn(
				"connection limit reached, reject new connection",
				"max_connections", s.connLimiter.Max(),
				"current_connections", s.connLimiter.Curr(),
				"next_backoff", next,
			)
			time.Sleep(next)
			continue
		}

		// 成功占用配额，重置退避策略。
		s.backoff.Reset()

		conn, err := s.listener.Accept()
		if err != nil {
			// 接收连接失败，归还配额。
			s.connLimiter.Release()

			logging.For("server").Error("failed to accept connection", "error", err)
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
	// 归还连接配额。
	defer s.connLimiter.Release()

	// 关闭连接。
	defer func() {
		err := conn.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			logging.For("server").Warn("failed to close connection", "error", err)
		}
	}()

	// 处理 upgrade 请求。
	sess, compressionState, err := s.upgrader.Upgrade(conn)
	if err != nil {
		metrics.HandshakeTotal.WithLabelValues("failure").Inc()
		logging.For("server").Error(
			"failed to upgrade connection from HTTP to WebSocket",
			"error", err,
		)
		return
	}
	metrics.HandshakeTotal.WithLabelValues("success").Inc()

	// 创建、管理连接。
	// 注意这里的连接指的是 synp.Conn 而不是 net.Conn 接口。
	// 新协议下连接建立时身份未知 ( 首帧 AUTH 完成鉴权 )，创建的是匿名连接。
	synpConn, err := s.connManager.NewConn(s.ctx, conn, sess, compressionState)
	if err != nil {
		logging.For("server").Error("failed to create synp connection", "error", err)
		return
	}

	defer func() {
		// 匿名连接与已绑定连接统一按连接 ID 清理。
		s.connManager.RemoveConnByID(synpConn.ID())
		if err := synpConn.Close(); err != nil {
			logging.For("server").Error(
				"failed to close synp connection",
				"conn_id", synpConn.ID(),
				"error", err,
			)
		}
	}()

	// 处理 on connect & on disconnect 事件。
	if err := s.connHandler.OnConnect(synpConn); err != nil {
		logging.For("server").Error(
			"failed to handle on connect lifecycle event",
			"conn_id", synpConn.ID(),
			"error", err,
		)
		// on connect 事件失败，直接返回。
		// 这里最好直接返回以防止后续的事件处理发生不可预料的错误。
		return
	}
	defer func() {
		if err := s.connHandler.OnDisconnect(synpConn); err != nil {
			logging.For("server").Error(
				"failed to handle on disconnect lifecycle event",
				"conn_id", synpConn.ID(),
				"error", err,
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
				logging.For("server").Error(
					"failed to handle on receive from frontend event",
					"error", err,
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

			logging.For("server").Info(
				"close connection because of auth timeout",
				"conn_id", synpConn.ID(),
			)
			return
		case <-synpConn.Closed():
			logging.For("server").Info("synp connection has been closed")
			return
		case <-s.ctx.Done():
			logging.For("server").Info("server has been closed")
			return
		}
	}
}

// idleScanShards 为空闲扫描分片数。
// 扫描周期 = idle_scan_interval / shards ( 默认 30s/30 = 每秒扫 1 个分片 )。
const idleScanShards = 30

// cleanIdleConn 清理空闲连接与慢客户端。
// 按分片巡检: 每个周期只扫描 1/idleScanShards 的连接，
// 避免大规模连接下单次扫描集中获取全部连接锁。
func (s *Server) cleanIdleConn() {
	scanInterval := s.idleScanInterval / idleScanShards
	if scanInterval <= 0 {
		scanInterval = time.Second
	}

	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	logging.For("server").Debug(
		"idle connection cleaner started",
		"scan_interval", scanInterval,
		"shards", idleScanShards,
		"idle_timeout", s.idleTimeout,
	)

	var shard uint64
	for {
		select {
		case <-s.ctx.Done():
			logging.For("server").Info("idle connection cleaner stopped by context done")
			return
		case <-ticker.C:
			s.connManager.RangeConnsShard(shard%idleScanShards, idleScanShards, func(conn synp.Conn) bool {
				// 空闲清理 ( 启用 auto close 的连接 ) 与
				// 慢客户端隔离 ( 出站入队持续超时 ) 统一巡检。
				// 连续 3 个扫描周期入队超时视为僵死连接。
				if conn.TryCloseIfIdle(s.idleTimeout) {
					return true
				}
				conn.TryCloseIfSlow(slowCloseOverflowCnt)
				return true
			})
			shard++
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
				logging.For("server").Warn("consumer not found", "event", key)
				continue
			}
			if err := consumer.StartConsumer(s.ctx, s.consumeNotifyMessage); err != nil {
				logging.For("server").Error("failed to start notify message consumer", "error", err)
				return err
			}
		case gateway.EventScaleUp:
			consumer, ok := s.consumers[key]
			if !ok {
				logging.For("server").Warn("consumer not found", "event", key)
				continue
			}
			if err := consumer.StartConsumer(s.ctx, s.consumeScaleUp); err != nil {
				logging.For("server").Error("failed to start scale up consumer", "error", err)
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
func (s *Server) consumeNotifyMessage(ctx context.Context, msg *xmq.Message) error {
	start := time.Now()
	defer func() {
		metrics.ConsumerHandleDuration.Observe(time.Since(start).Seconds())
	}()

	notifyMsg := &messagev1.NotifyMessage{}
	// 后端 ( 业务服务端 ) 投递到通知 topic 的记录使用 protojson 编码。
	if err := protojson.Unmarshal(msg.Val, notifyMsg); err != nil {
		logging.For("server").Error(
			"failed to unmarshal notify message",
			"message", string(msg.Val),
			"error", err,
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
		logging.For("server").Debug(
			"receiver not on this node, ignore notify message",
			"business_id", notifyMsg.GetBusinessId(),
			"receiver_id", notifyMsg.GetReceiverId(),
		)
		return nil
	}

	if err := s.connHandler.OnReceiveFromBackend(ctx, conns, notifyMsg); err != nil {
		logging.For("server").Error(
			"failed to handle on receive from backend event",
			"message", string(msg.Val),
			"error", err,
		)
		return err
	}
	return nil
}

// consumeScaleUp 消费 scale up 事件。
func (s *Server) consumeScaleUp(_ context.Context, _ *xmq.Message) error {
	// 尚未实现，返回错误触发消费侧重试而非 panic 终止进程。
	return errors.New("[server] scale up consumer not implemented")
}

// Shutdown 关闭 WebSocket 服务器。
func (s *Server) Shutdown() error {
	logging.For("server").Info("shutting down ws server")
	s.cancelFunc()
	logging.For("server").Info("ws server shut down")
	return nil
}

// GracefulShutdown 优雅关闭 WebSocket 服务器。
//
// 顺序: 停止接受新连接 -> 停止 Kafka 消费 -> 等待存量连接排空 ( 带超时 )
// -> 强制关闭剩余连接 -> 取消服务器 context。
// 注意不能先等待 ctx.Done 再取消 ctx——那会永久阻塞 ( 自锁 )。
func (s *Server) GracefulShutdown() error {
	logging.For("server").Info("graceful shutting down ws server")

	// 1. 停止接受新连接。
	s.acceptNewConn.Store(false)
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			logging.For("server").Warn("failed to close net listener", "error", err)
		}
	}

	// 2. 停止 Kafka 消费，不再产生新的下行通知。
	s.stopConsumers()

	// 3. 等待存量连接排空 ( handleConn 退出时连接自行关闭 )。
	deadline := time.Now().Add(s.drainTimeout)
	for time.Now().Before(deadline) {
		if s.connManager.ConnCount() == 0 {
			break
		}
		time.Sleep(drainPollInterval)
	}

	// 4. 关闭排空超时后仍存活的连接。
	if remained := s.connManager.ConnCount(); remained > 0 {
		logging.For("server").Info(
			"drain timeout exceeded, closing remaining connections",
			"count", remained,
		)
		s.connManager.RangeConns(func(conn synp.Conn) bool {
			_ = conn.Close()
			return true
		})
	}

	return s.Shutdown()
}

// stopConsumers 停止全部 Kafka 消费者。
func (s *Server) stopConsumers() {
	ctx, cancel := context.WithTimeout(context.Background(), s.drainTimeout)
	defer cancel()

	for event, consumer := range s.consumers {
		if err := consumer.StopConsumer(ctx); err != nil {
			logging.For("server").Warn(
				"failed to stop consumer",
				"event", event,
				"error", err,
			)
		}
	}
}
