package conn

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/jit/xretry"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/compression"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/jrmarcco/synp/internal/pkg/xws"
	"go.uber.org/multierr"
	"go.uber.org/ratelimit"
)

var ErrConnClosed = errors.New("connection closed")

var _ synp.Conn = (*Conn)(nil)

// Conn 代表一个 WebSocket 连接，封装了 net.Conn。
// 同时负责 WebSocket 消息的读写。
// 读写操作分别由独立的 goroutine 负责。
type Conn struct {
	id string

	// sessMu 保护 sess 的替换 ( AUTH 鉴权完成后绑定真实身份 )。
	sessMu sync.RWMutex
	sess   session.Session

	// authDone 在首帧 AUTH 鉴权成功后关闭。
	// 未鉴权连接由网关拒绝处理任何其它指令，超时未鉴权由网关关闭。
	authOnce sync.Once
	authDone chan struct{}

	// enqueueOverflowCnt 为出站入队连续超时次数，
	// 用于巡检识别持续无法下发的慢客户端 ( 僵死连接 )。
	enqueueOverflowCnt atomic.Int32

	netConn net.Conn

	reader      *xws.Reader
	readTimeout time.Duration

	// establishedReadTimeout 为鉴权完成后的读超时。
	establishedReadTimeout time.Duration

	writer       *xws.Writer
	writeTimeout time.Duration

	compressionState *compression.State

	// 重试策略
	initRetryInterval time.Duration
	maxRetryInterval  time.Duration
	maxRetryCount     int32

	// 通信 channel
	sendChan    chan []byte
	receiveChan chan []byte

	// 空闲连接管理
	mu           sync.RWMutex
	autoClose    bool
	activityTime time.Time

	// 限流：
	//
	// 	这里使用 uber 的 ratelimit 库，是一个基于漏桶算法 ( Leaky Bucket ) 的限流器。
	// 	WebSocket 消息处理需要平滑，避免突发消息阻塞 receiveChan。
	// 	同时达到限流时优先考虑阻塞等待。
	limitRate int
	limiter   ratelimit.Limiter

	// maxFrameBytes 为入站数据帧大小上限。
	maxFrameBytes int64

	ctx        context.Context
	cancelFunc context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

func ConnWithReadTimeout(readTimeout time.Duration) option.Opt[Conn] {
	return func(c *Conn) {
		c.readTimeout = readTimeout
	}
}

// ConnWithEstablishedReadTimeout 设置鉴权完成后的读超时。
func ConnWithEstablishedReadTimeout(timeout time.Duration) option.Opt[Conn] {
	return func(c *Conn) {
		if timeout > 0 {
			c.establishedReadTimeout = timeout
		}
	}
}

func ConnWithWriteTimeout(writeTimeout time.Duration) option.Opt[Conn] {
	return func(c *Conn) {
		c.writeTimeout = writeTimeout
	}
}

func ConnWithCompression(state *compression.State) option.Opt[Conn] {
	return func(c *Conn) {
		c.compressionState = state
	}
}

func ConnWithRetry(initRetryInterval, maxRetryInterval time.Duration, maxRetryCount int32) option.Opt[Conn] {
	return func(c *Conn) {
		c.initRetryInterval = initRetryInterval
		c.maxRetryInterval = maxRetryInterval
		c.maxRetryCount = maxRetryCount
	}
}

func ConnWithReadBuffer(receiveBufferSize int) option.Opt[Conn] {
	return func(c *Conn) {
		c.receiveChan = make(chan []byte, receiveBufferSize)
	}
}

func ConnWithWriteBuffer(sendBufferSize int) option.Opt[Conn] {
	return func(c *Conn) {
		c.sendChan = make(chan []byte, sendBufferSize)
	}
}

func ConnWithAutoClose(autoClose bool) option.Opt[Conn] {
	return func(c *Conn) {
		c.autoClose = autoClose
	}
}

// ConnWithMaxFrameBytes 设置入站数据帧大小上限。
func ConnWithMaxFrameBytes(maxFrameBytes int64) option.Opt[Conn] {
	return func(c *Conn) {
		if maxFrameBytes > 0 {
			c.maxFrameBytes = maxFrameBytes
		}
	}
}

// ConnWithRateLimit 限流器 option。
// rate 为每秒请求上限。
func ConnWithRateLimit(rate int) option.Opt[Conn] {
	return func(c *Conn) {
		if rate > 0 {
			c.limitRate = rate
			c.limiter = ratelimit.New(rate)
		}
	}
}

func NewConn(
	parentCtx context.Context,
	id string,
	sess session.Session,
	netConn net.Conn,
	opts ...option.Opt[Conn],
) *Conn {
	ctx, cancel := context.WithCancel(parentCtx)

	c := &Conn{
		id:   id,
		sess: sess,

		authDone: make(chan struct{}),

		netConn: netConn,

		readTimeout:            DefaultReadTiemout,
		writeTimeout:           DefaultWriteTiemout,
		establishedReadTimeout: DefaultEstablishedReadTimeout,

		initRetryInterval: DefaultInitRetryInterval,
		maxRetryInterval:  DefaultMaxRetryInterval,
		maxRetryCount:     DefaultMaxRetryCount,

		sendChan:    make(chan []byte, DefaultSendBufferSize),
		receiveChan: make(chan []byte, DefaultReceiveBufferSize),

		activityTime: time.Now(),

		ctx:        ctx,
		cancelFunc: cancel,
	}

	option.Apply(c, opts...)

	// 在 option 应用之后才能确定 compressionState。
	// 所以只能在这里初始化 writer 和 reader。
	var (
		compressionEnabled bool
		compressLevel      int
		minCompressSize    int
	)
	if c.compressionState != nil {
		compressionEnabled = c.compressionState.Enabled
		compressLevel = c.compressionState.Level
		minCompressSize = c.compressionState.MinCompressSize
	}
	c.reader = xws.NewServerSideReader(netConn, c.maxFrameBytes)
	c.writer = xws.NewServerSideWriter(netConn, compressionEnabled, compressLevel, minCompressSize)

	// 启动收发数据的 goroutine。
	//nolint:contextcheck // writeLoop 内部使用 c.ctx。
	go c.writeLoop()
	//nolint:contextcheck // readLoop 内部使用 c.ctx。
	go c.readLoop()

	return c
}

func (c *Conn) writeLoop() {
	defer func() {
		_ = c.Close()
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		case payload, ok := <-c.sendChan:
			if !ok {
				return
			}

			if !c.trySend(payload) {
				// 发送失败，关闭连接。
				return
			}
		}
	}
}

// trySend 是实际发送消息给客户端的逻辑。
// 在发送失败时，会根据配置使用指数退避策略进行重试，最终重试失败才会返回 false。
// 注意：
//
//	只允许在发生超时时进行重试。
func (c *Conn) trySend(payload []byte) bool {
	// 这里可以忽略 error。
	// 创建 ws.Conn 的时候就应该确保重试策略的参数正确。
	retryStrategy, _ := xretry.NewExponentialBackoffStrategy(
		c.initRetryInterval,
		c.maxRetryInterval,
		c.maxRetryCount,
	)

	for {
		select {
		case <-c.ctx.Done():
			return false
		default:
		}

		// 设置写超时。
		_ = c.netConn.SetWriteDeadline(time.Now().Add(c.writeTimeout))

		_, err := c.writer.Write(payload)
		if err == nil {
			return true
		}

		logging.For("conn").Error(
			"failed to send message to client",
			"conn_id", c.id,
			"payload_len", len(payload),
			"compression_state", c.compressionState,
			"user", c.Session().User(),
			"error", err,
		)

		// 检查错误，如果是超时错误则允许重试。
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			duration, ok := retryStrategy.Next()
			if !ok {
				// 重试达到上限。
				logging.For("conn").Error(
					"failed to resend message to client, retry reach max",
					"conn_id", c.id,
					"compression_state", c.compressionState,
					"user", c.Session().User(),
				)
				return false
			}

			select {
			case <-c.ctx.Done():
				return false
			case <-time.After(duration):
				continue
			}
		}
		// 非超时错误，直接失败。
		return false
	}
}

func (c *Conn) readLoop() {
	defer func() {
		close(c.receiveChan)
		_ = c.Close()
	}()

	for {
		if c.limiter != nil {
			// 获取限流器令牌。
			c.limiter.Take()
		}

		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// 设置读超时: 鉴权前短超时 ( AUTH 超时兜底 )，鉴权后长超时。
		readTimeout := c.readTimeout
		select {
		case <-c.authDone:
			if c.establishedReadTimeout > 0 {
				readTimeout = c.establishedReadTimeout
			}
		default:
		}
		_ = c.netConn.SetReadDeadline(time.Now().Add(readTimeout))

		payload, err := c.reader.Read()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}

			var wsErr wsutil.ClosedError
			if errors.As(err, &wsErr) && (wsErr.Code == ws.StatusNoStatusRcvd || wsErr.Code == ws.StatusGoingAway) {
				// 客户端关闭连接，记录日志直接返回。
				logging.For("conn").Debug(
					"client closed connection",
					"conn_id", c.id,
					"compression_state", c.compressionState,
					"user", c.Session().User(),
				)
				return
			}

			// 其他错误，直接返回。
			logging.For("conn").Error(
				"failed to read message from client",
				"conn_id", c.id,
				"compression_state", c.compressionState,
				"user", c.Session().User(),
				"error", err,
			)
			return
		}

		select {
		case <-c.ctx.Done():
			return
		case c.receiveChan <- payload:
		}
	}
}

func (c *Conn) ID() string {
	return c.id
}

func (c *Conn) Session() session.Session {
	c.sessMu.RLock()
	defer c.sessMu.RUnlock()
	return c.sess
}

// BindSession 在首帧 AUTH 鉴权成功后，将匿名 Session 替换为携带真实身份的 Session。
func (c *Conn) BindSession(sess session.Session) {
	c.sessMu.Lock()
	c.sess = sess
	c.sessMu.Unlock()
}

// Authenticated 返回鉴权完成信号。
// channel 关闭表示连接已完成首帧 AUTH 鉴权。
func (c *Conn) Authenticated() <-chan struct{} {
	return c.authDone
}

// MarkAuthenticated 标记连接鉴权完成，重复调用是安全的。
// 鉴权完成后立即把读超时切换为长超时，
// 避免空闲连接在每个短超时周期被无效唤醒。
func (c *Conn) MarkAuthenticated() {
	c.authOnce.Do(func() {
		close(c.authDone)

		if c.establishedReadTimeout > 0 {
			_ = c.netConn.SetReadDeadline(time.Now().Add(c.establishedReadTimeout))
		}
	})
}

func (c *Conn) Send(ctx context.Context, payload []byte) error {
	select {
	case <-c.ctx.Done():
		return ErrConnClosed
	case c.sendChan <- payload:
		c.enqueueOverflowCnt.Store(0)

		if c.ctx.Err() != nil {
			return ErrConnClosed
		}
		return nil
	case <-ctx.Done():
		// 入队超时计为一次慢客户端证据，由巡检决定断开。
		c.enqueueOverflowCnt.Add(1)
		return synp.ErrSendEnqueueTimeout
	}
}

// TryCloseIfSlow 出站入队连续超时达到 minOverflow 时关闭连接 ( 慢客户端隔离 )。
func (c *Conn) TryCloseIfSlow(minOverflow int32) bool {
	if c.enqueueOverflowCnt.Load() < minOverflow {
		return false
	}

	logging.For("conn").Warn(
		"close slow client because of continuous enqueue overflow",
		"conn_id", c.id,
		"overflow_cnt", c.enqueueOverflowCnt.Load(),
	)
	return c.Close() == nil
}

func (c *Conn) Receive() <-chan []byte {
	return c.receiveChan
}

func (c *Conn) UpdateActivityTime() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ctx.Err() == nil {
		c.activityTime = time.Now()
	}
}

func (c *Conn) TryCloseIfIdle(idleTimeout time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ctx.Err() != nil {
		// 连接已经被关闭。
		return true
	}

	// 没有设置自动关闭或空闲时间小于空闲超时时间，不关闭连接。
	if !c.autoClose || time.Since(c.activityTime) < idleTimeout {
		return false
	}

	if err := c.Close(); err != nil {
		// 关闭直白只记录日志。
		logging.For("conn").Error(
			"failed to close connection",
			"conn_id", c.id,
			"user", c.Session().User(),
			"error", err,
		)
	}
	return true
}

func (c *Conn) Close() error {
	// 注意:
	//
	// 不要关闭 c.sendChan，
	// 这会导致 Send 方法中的 c.sendChan <- payload 分支发生 panic：send on closed channel。
	c.closeOnce.Do(func() {
		// 取消 context。
		c.cancelFunc()

		// 尝试发送 WebSocket 关闭帧。
		_ = c.netConn.SetWriteDeadline(time.Now().Add(DefaultCloseTimeout))
		_ = wsutil.WriteServerMessage(c.netConn, ws.OpClose, []byte{})

		// 关闭底层连接 ( net.Conn )。
		c.closeErr = c.netConn.Close()

		// 销毁 session。
		ctx, cancel := context.WithTimeout(context.Background(), DefaultCloseTimeout)
		defer cancel()
		c.closeErr = multierr.Append(
			c.closeErr,
			c.Session().Destroy(ctx),
		)
	})
	return c.closeErr
}

func (c *Conn) Closed() <-chan struct{} {
	return c.ctx.Done()
}
