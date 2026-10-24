package conn

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
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
	"golang.org/x/time/rate"
)

const (
	sendBatchSize = 8
)

var _ synp.Conn = (*WriteFirstConn)(nil)

// WriteFirstConn 代表一个 WebSocket 连接，封装了 net.Conn。
// 使用单个 goroutine 同时负责读写操作。
// 与 ReadWriteConn 不同的是，
// WriteFirstConn 在每次循环时会先批量执行写操作。
type WriteFirstConn struct {
	id string

	sessMu sync.RWMutex
	sess   session.Session

	authOnce sync.Once
	authDone chan struct{}

	netConn net.Conn

	reader      *xws.Reader
	readTimeout time.Duration // 这里应该设置一个极短的 readTimeout 以避免阻长时间阻塞操作。

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

	// 限流
	limiter *rate.Limiter // golang.org/x/time/rate 标准扩展库。

	ctx        context.Context
	cancelFunc context.CancelFunc

	closeOnce sync.Once
	closeErr  error

	logger *slog.Logger
}

func NewWriteFirstConn(
	parentCtx context.Context,
	id string,
	sess session.Session,
	netConn net.Conn,
	limitRate int,
	opts ...option.Opt[WriteFirstConn],
) (*WriteFirstConn, error) {
	if limitRate <= 0 {
		return nil, errors.New("limitRate must be greater than 0")
	}

	ctx, cancel := context.WithCancel(parentCtx)

	c := &WriteFirstConn{
		id:   id,
		sess: sess,

		authDone: make(chan struct{}),

		netConn: netConn,

		readTimeout:  DefaultReadTiemout,
		writeTimeout: DefaultWriteTiemout,

		initRetryInterval: DefaultInitRetryInterval,
		maxRetryInterval:  DefaultMaxRetryInterval,
		maxRetryCount:     DefaultMaxRetryCount,

		sendChan:    make(chan []byte, DefaultSendBufferSize),
		receiveChan: make(chan []byte, DefaultReceiveBufferSize),

		activityTime: time.Now(),

		limiter: rate.NewLimiter(rate.Limit(limitRate), limitRate),

		ctx:        ctx,
		cancelFunc: cancel,

		logger: logging.For("write-first-conn"),
	}

	option.Apply(c, opts...)

	var compressionEnabled bool
	if c.compressionState != nil {
		compressionEnabled = c.compressionState.Enabled
	}
	c.reader = xws.NewServerSideReader(netConn, 0)
	c.writer = xws.NewServerSideWriter(netConn, compressionEnabled, 0, 0)

	//nolint:contextcheck // readWriteLoop 内部使用 c.ctx。
	go c.writeFirstLoop()

	return c, nil
}

func (c *WriteFirstConn) writeFirstLoop() {
	defer func() {
		close(c.receiveChan)
		_ = c.Close()
	}()

	for {
		// 批量写入 ( write to websocket )。
		for range sendBatchSize {
			var payload []byte
			select {
			case <-c.ctx.Done():
				return
			case payload = <-c.sendChan:
			default:
				payload = nil
			}

			if payload == nil {
				// 进入读操作。
				break
			}

			if !c.trySend(payload) {
				return
			}
		}

		// 限流。
		// 这里使用 golang.org/x/time/rate 标准扩展库，非阻塞操作。
		// uber 的 ratelimit 库是阻塞操作。
		if c.limiter != nil && !c.limiter.Allow() {
			continue
		}

		_ = c.netConn.SetReadDeadline(time.Now().Add(c.readTimeout))
		payload, err := c.reader.Read()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// 读超时。
				// 继续循环回到写操作。
				continue
			}

			var wsErr wsutil.ClosedError
			if errors.As(err, &wsErr) && (wsErr.Code == ws.StatusNoStatusRcvd) || (wsErr.Code == ws.StatusGoingAway) {
				c.logger.Info(
					"[write-first-conn] client closed connection",
					"conn_id", c.id,
					"user", c.Session().User(),
				)
				return
			}

			c.logger.Error(
				"[write-first-conn] failed to read message from client",
				"conn_id", c.id,
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

//nolint:dupl // ignore.
func (c *WriteFirstConn) trySend(payload []byte) bool {
	retryStrategy, _ := xretry.NewExponentialBackoffStrategy(
		c.initRetryInterval,
		c.maxRetryInterval,
		c.maxRetryCount,
	)

	for range c.maxRetryCount {
		select {
		case <-c.ctx.Done():
			return false
		default:
		}

		_ = c.netConn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		_, err := c.writer.Write(payload)
		if err == nil {
			return true
		}

		c.logger.Error(
			"[read-write-conn] failed to send message to client",
			"conn_id", c.id,
			"user", c.Session().User(),
			"error", err,
		)

		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			duration, shouldRetry := retryStrategy.Next()
			if !shouldRetry {
				break
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

	// 重试达到上限。
	c.logger.Error(
		"[read-write-conn] failed to send message to client, retry reach max",
		"conn_id", c.id,
		"user", c.Session().User(),
	)
	return false
}

func (c *WriteFirstConn) ID() string {
	return c.id
}

func (c *WriteFirstConn) Session() session.Session {
	c.sessMu.RLock()
	defer c.sessMu.RUnlock()
	return c.sess
}

// BindSession 在首帧 AUTH 鉴权成功后，将匿名 Session 替换为携带真实身份的 Session。
func (c *WriteFirstConn) BindSession(sess session.Session) {
	c.sessMu.Lock()
	c.sess = sess
	c.sessMu.Unlock()
}

// Authenticated 返回鉴权完成信号。
func (c *WriteFirstConn) Authenticated() <-chan struct{} {
	return c.authDone
}

// MarkAuthenticated 标记连接鉴权完成，重复调用是安全的。
func (c *WriteFirstConn) MarkAuthenticated() {
	c.authOnce.Do(func() {
		close(c.authDone)
	})
}

func (c *WriteFirstConn) Send(ctx context.Context, payload []byte) error {
	select {
	case <-c.ctx.Done():
		return ErrConnClosed
	case c.sendChan <- payload:
		if c.ctx.Err() != nil {
			return ErrConnClosed
		}
		return nil
	case <-ctx.Done():
		return synp.ErrSendEnqueueTimeout
	}
}

func (c *WriteFirstConn) Receive() <-chan []byte {
	return c.receiveChan
}

func (c *WriteFirstConn) UpdateActivityTime() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ctx.Err() == nil {
		c.activityTime = time.Now()
	}
}

func (c *WriteFirstConn) TryCloseIfIdle(idleTimeout time.Duration) bool {
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
		c.logger.Error(
			"[conn] failed to close connection",
			"conn_id", c.id,
			"user", c.Session().User(),
			"error", err,
		)
	}
	return true
}

// TryCloseIfSlow 未启用 ( 实验连接类型，无入队超时计数 )。
func (c *WriteFirstConn) TryCloseIfSlow(_ int32) bool {
	return false
}

func (c *WriteFirstConn) Close() error {
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

func (c *WriteFirstConn) Closed() <-chan struct{} {
	return c.ctx.Done()
}
