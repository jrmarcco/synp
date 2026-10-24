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
)

var _ synp.Conn = (*ReadWriteConn)(nil)

// ReadWriteConn 代表一个 WebSocket 连接，封装了 net.Conn。
// 使用单个 goroutine 同时负责读写操作。
type ReadWriteConn struct {
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

	ctx        context.Context
	cancelFunc context.CancelFunc

	closeOnce sync.Once
	closeErr  error

	logger *slog.Logger
}

func NewReadWriteConn(
	parentCtx context.Context,
	id string,
	sess session.Session,
	netConn net.Conn,
	opts ...option.Opt[ReadWriteConn],
) *ReadWriteConn {
	ctx, cancel := context.WithCancel(parentCtx)

	c := &ReadWriteConn{
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

		ctx:        ctx,
		cancelFunc: cancel,

		logger: logging.For("read-write-conn"),
	}

	option.Apply(c, opts...)

	var compressionEnabled bool
	if c.compressionState != nil {
		compressionEnabled = c.compressionState.Enabled
	}
	c.reader = xws.NewServerSideReader(netConn, 0)
	c.writer = xws.NewServerSideWriter(netConn, compressionEnabled, 0, 0)

	//nolint:contextcheck // readWriteLoop 内部使用 c.ctx。
	go c.readWriteLoop()

	return c
}

func (c *ReadWriteConn) readWriteLoop() {
	defer func() {
		close(c.receiveChan)
		_ = c.Close()
	}()

	for {
		// 对写操作、关闭信号优先响应。
		// 只有在没有写信号和关闭信号时，才会在 default 分支中尝试读操作。
		select {
		case <-c.ctx.Done():
			return

		case payload, ok := <-c.sendChan:
			if !ok {
				return
			}

			// 执行写操作。
			if !c.trySend(payload) {
				return
			}

		default:
			// 执行阻塞读。
			_ = c.netConn.SetReadDeadline(time.Now().Add(c.readTimeout))
			payload, err := c.reader.Read()

			if err == nil {
				// 读取数据成功。
				select {
				case c.receiveChan <- payload:
				default:
					// receiveChan 已满，丢弃数据。
					// 注意：丢弃数据是必要的反压保护。
					c.logger.Warn(
						"[read-write-conn] receive channel is full, dropping message",
						"conn_id", c.id,
						"user", c.Session().User(),
					)
				}
			}

			// 检查读取错误。
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// 读取超时，即当前没有可读数据，可以忽略。
			} else {
				// 其他不可恢复的错误。
				var wsErr wsutil.ClosedError
				if errors.As(err, &wsErr) && (wsErr.Code == ws.StatusNoStatusRcvd) || (wsErr.Code == ws.StatusGoingAway) {
					c.logger.Info(
						"[read-write-conn] client closed connection",
						"conn_id", c.id,
						"user", c.Session().User(),
					)
					return
				}

				c.logger.Error(
					"[read-write-conn] failed to read message from client",
					"conn_id", c.id,
					"user", c.Session().User(),
					"error", err,
				)
				return
			}
		}
	}
}

//nolint:dupl // ignore.
func (c *ReadWriteConn) trySend(payload []byte) bool {
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

func (c *ReadWriteConn) ID() string {
	return c.id
}

func (c *ReadWriteConn) Session() session.Session {
	c.sessMu.RLock()
	defer c.sessMu.RUnlock()
	return c.sess
}

// BindSession 在首帧 AUTH 鉴权成功后，将匿名 Session 替换为携带真实身份的 Session。
func (c *ReadWriteConn) BindSession(sess session.Session) {
	c.sessMu.Lock()
	c.sess = sess
	c.sessMu.Unlock()
}

// Authenticated 返回鉴权完成信号。
func (c *ReadWriteConn) Authenticated() <-chan struct{} {
	return c.authDone
}

// MarkAuthenticated 标记连接鉴权完成，重复调用是安全的。
func (c *ReadWriteConn) MarkAuthenticated() {
	c.authOnce.Do(func() {
		close(c.authDone)
	})
}

func (c *ReadWriteConn) Send(ctx context.Context, payload []byte) error {
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

func (c *ReadWriteConn) Receive() <-chan []byte {
	return c.receiveChan
}

func (c *ReadWriteConn) UpdateActivityTime() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ctx.Err() == nil {
		c.activityTime = time.Now()
	}
}

func (c *ReadWriteConn) TryCloseIfIdle(idleTimeout time.Duration) bool {
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
func (c *ReadWriteConn) TryCloseIfSlow(_ int32) bool {
	return false
}

func (c *ReadWriteConn) Close() error {
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

func (c *ReadWriteConn) Closed() <-chan struct{} {
	return c.ctx.Done()
}
