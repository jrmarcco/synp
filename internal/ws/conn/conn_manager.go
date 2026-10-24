package conn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/jit/xsync"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/compression"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"go.uber.org/zap"
)

const (
	// 默认读写超时时间
	DefaultReadTiemout  = 15 * time.Second
	DefaultWriteTiemout = 10 * time.Second

	// 默认缓冲大小
	DefaultSendBufferSize    = 256
	DefaultReceiveBufferSize = 256

	// 默认重试策略
	DefaultInitRetryInterval = 1 * time.Second
	DefaultMaxRetryInterval  = 5 * time.Second
	DefaultMaxRetryCount     = 3

	DefaultCloseTimeout = time.Second
	DefaultRateLimit    = 10
)

// DeviceConns 管理单个用户的多设备连接。
// 这里不直接使用 sync.Map 是因为一个用户最多只会有 3 个设备连接。
// 相比起直接使用 sync.Map 性能更好且内存占用更低。
type DeviceConns struct {
	mu    sync.RWMutex
	conns map[session.Device]synp.Conn
}

func newDeviceConns() *DeviceConns {
	const maxDeviceConnCnt = 3
	return &DeviceConns{
		// 一个用户最多只会有 3 个设备连接。
		conns: make(map[session.Device]synp.Conn, maxDeviceConnCnt),
	}
}

func (dc *DeviceConns) add(device session.Device, conn synp.Conn) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.conns[device] = conn
}

func (dc *DeviceConns) remove(device session.Device) (synp.Conn, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if conn, ok := dc.conns[device]; ok {
		delete(dc.conns, device)
		return conn, true
	}
	return nil, false
}

// removeByID 按连接 ID 移除连接，返回被移除的连接对应的设备类型。
func (dc *DeviceConns) removeByID(connID string) (session.Device, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	for device, conn := range dc.conns {
		if conn.ID() == connID {
			delete(dc.conns, device)
			return device, true
		}
	}
	return "", false
}

func (dc *DeviceConns) find(device session.Device) (synp.Conn, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	conn, ok := dc.conns[device]
	return conn, ok
}

func (dc *DeviceConns) findAll() ([]synp.Conn, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	conns := make([]synp.Conn, 0, len(dc.conns))
	for _, conn := range dc.conns {
		conns = append(conns, conn)
	}
	return conns, len(conns) > 0
}

func (dc *DeviceConns) clear() int {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	cnt := len(dc.conns)
	clear(dc.conns)
	return cnt
}

type ConnConfig struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	InitRetryInterval time.Duration
	MaxRetryInterval  time.Duration
	MaxRetryCount     int32

	SendBufferSize    int
	ReceiveBufferSize int

	CloseTimeout time.Duration
	RateLimit    int
}

var _ synp.ConnManager = (*ConnManager)(nil)

// ConnManager 管理网关节点上的全部连接。
//
// 连接在建立时身份未知 ( 新协议鉴权在连接建立后的首帧 AUTH 完成 )，
// 先进入 pending 区；鉴权成功后由 BindUser 绑定到用户索引。
type ConnManager struct {
	cfg *ConnConfig

	// pending 保存未完成鉴权的连接 ( connID -> conn )。
	pending *xsync.Map[string, synp.Conn]

	conns   *xsync.Map[string, *DeviceConns]
	connCnt atomic.Int64
	userCnt atomic.Int64

	logger *zap.Logger
}

func ConnManagerWithConfig(cfg *ConnConfig) option.Opt[ConnManager] {
	return func(m *ConnManager) {
		m.cfg = cfg
	}
}

func NewConnManager(logger *zap.Logger, opts ...option.Opt[ConnManager]) *ConnManager {
	cfg := &ConnConfig{
		ReadTimeout:       DefaultReadTiemout,
		WriteTimeout:      DefaultWriteTiemout,
		InitRetryInterval: DefaultInitRetryInterval,
		MaxRetryInterval:  DefaultMaxRetryInterval,
		MaxRetryCount:     DefaultMaxRetryCount,
		SendBufferSize:    DefaultSendBufferSize,
		ReceiveBufferSize: DefaultReceiveBufferSize,
		CloseTimeout:      DefaultCloseTimeout,
		RateLimit:         DefaultRateLimit,
	}

	cm := &ConnManager{
		cfg:     cfg,
		pending: &xsync.Map[string, synp.Conn]{},
		conns:   &xsync.Map[string, *DeviceConns]{},
		logger:  logger,
	}

	option.Apply(cm, opts...)

	return cm
}

// NewConn 创建连接。
// 新协议下连接建立时身份未知，连接先注册到 pending 区，
// 完成首帧 AUTH 鉴权后由 BindUser 绑定到用户索引。
func (m *ConnManager) NewConn(ctx context.Context, netConn net.Conn, sess session.Session, compressionState *compression.State) (synp.Conn, error) {
	// 匿名连接无法从 user 推导 connID，由网关生成全局唯一 ID。
	connID, err := newConnID()
	if err != nil {
		return nil, err
	}

	opts := m.convertToConnOpts(sess.User(), compressionState)
	conn := NewConn(ctx, connID, sess, netConn, m.logger, opts...)

	m.pending.Store(connID, conn)
	m.connCnt.Add(1)

	m.logger.Info(
		"[conn-manager] successfully create connection",
		zap.String("conn_id", connID),
		zap.Any("device", sess.User().Device),
	)
	return conn, nil
}

// BindUser 将完成鉴权的连接绑定到用户索引。
// 同一用户同一设备已有连接时关闭旧连接 ( 顶号 )。
func (m *ConnManager) BindUser(conn synp.Conn, user session.User) error {
	connKey := user.ConnKey()
	device := user.Device

	// 检查同一设备是否已有连接，如果有则关闭旧连接。
	if _, exists := m.loadExistingConn(connKey, device); exists {
		m.logger.Info(
			"[conn-manager] found existing connection for same device, closing old connection",
			zap.String("conn_key", connKey),
			zap.String("device", string(device)),
			zap.Any("user", user),
		)

		_ = m.RemoveConn(user)
	}

	m.storeConn(connKey, device, conn)
	m.pending.Delete(conn.ID())

	m.logger.Info(
		"[conn-manager] successfully bind connection to user",
		zap.String("conn_id", conn.ID()),
		zap.String("conn_key", connKey),
		zap.String("device", string(device)),
	)
	return nil
}

func (m *ConnManager) loadExistingConn(key string, device session.Device) (synp.Conn, bool) {
	dc, ok := m.conns.Load(key)
	if !ok {
		return nil, false
	}

	conn, ok := dc.find(device)
	if !ok {
		return nil, false
	}
	return conn, true
}

func (m *ConnManager) storeConn(cid string, device session.Device, conn synp.Conn) {
	dc, ok := m.conns.LoadOrStore(cid, newDeviceConns())
	if !ok {
		// LoadOrStore 返回 false 代表新创建了 DeviceConns。
		// 用户数 +1。
		m.userCnt.Add(1)
	}
	dc.add(device, conn)
}

func (m *ConnManager) convertToConnOpts(user session.User, compressionState *compression.State) []option.Opt[Conn] {
	var opts []option.Opt[Conn]

	if compressionState != nil {
		opts = append(opts, ConnWithCompression(compressionState))
	}

	if m.cfg.ReadTimeout > 0 {
		opts = append(opts, ConnWithReadTimeout(m.cfg.ReadTimeout))
	}
	if m.cfg.WriteTimeout > 0 {
		opts = append(opts, ConnWithWriteTimeout(m.cfg.WriteTimeout))
	}

	if m.cfg.SendBufferSize > 0 {
		opts = append(opts, ConnWithWriteBuffer(m.cfg.SendBufferSize))
	}
	if m.cfg.ReceiveBufferSize > 0 {
		opts = append(opts, ConnWithReadBuffer(m.cfg.ReceiveBufferSize))
	}

	if m.cfg.InitRetryInterval > 0 && m.cfg.MaxRetryInterval > 0 && m.cfg.MaxRetryCount > 0 {
		opts = append(opts, ConnWithRetry(m.cfg.InitRetryInterval, m.cfg.MaxRetryInterval, m.cfg.MaxRetryCount))
	}

	opts = append(
		opts,
		ConnWithAutoClose(user.AutoClose),
		ConnWithRateLimit(m.cfg.RateLimit),
	)

	return opts
}

// RemoveConn 移除并关闭用户指定设备的连接。
func (m *ConnManager) RemoveConn(user session.User) bool {
	connKey := user.ConnKey()

	dc, ok := m.conns.Load(connKey)
	if !ok {
		return false
	}

	conn, ok := dc.remove(user.Device)
	if ok {
		defer func() {
			// 关闭连接。
			err := conn.Close()
			if err != nil {
				m.logger.Warn(
					"[conn-manager] failed to close connection",
					zap.String("conn_id", conn.ID()),
					zap.Error(err),
				)
			}
		}()

		m.connCnt.Add(-1)

		if len(dc.conns) == 0 {
			m.conns.Delete(connKey)
			m.userCnt.Add(-1)
		}
	}

	return ok
}

func (m *ConnManager) RemoveUserConn(user session.User) bool {
	dc, ok := m.conns.Load(user.ConnKey())
	if !ok {
		return false
	}

	m.userCnt.Add(-1)

	cnt := dc.clear()
	m.connCnt.Add(-int64(cnt))
	return true
}

// RemoveConnByID 按连接 ID 移除连接，同时覆盖 pending 区与用户索引区。
// 连接断开的统一清理入口。
func (m *ConnManager) RemoveConnByID(connID string) bool {
	removed := false

	if _, ok := m.pending.LoadAndDelete(connID); ok {
		m.connCnt.Add(-1)
		removed = true
	}

	// pending 中不存在则说明已绑定用户，在用户索引区查找。
	if !removed {
		var boundKeys []string

		m.conns.Range(func(key string, dc *DeviceConns) bool {
			if _, ok := dc.removeByID(connID); ok {
				boundKeys = append(boundKeys, key)
				m.connCnt.Add(-1)

				if len(dc.conns) == 0 {
					m.conns.Delete(key)
					m.userCnt.Add(-1)
				}
				return false
			}
			return true
		})

		removed = len(boundKeys) > 0
	}

	return removed
}

// FindConn 查找用户单个连接 ( 具体到设备 )。
func (m *ConnManager) FindConn(user session.User) (synp.Conn, bool) {
	dc, ok := m.conns.Load(user.ConnKey())
	if !ok {
		return nil, false
	}

	conn, ok := dc.find(user.Device)
	if !ok {
		return nil, false
	}
	return conn, true
}

// FindUserConn 查找用户所有连接 ( 多设备 )。
func (m *ConnManager) FindUserConn(user session.User) ([]synp.Conn, bool) {
	connKey := user.ConnKey()
	dc, ok := m.conns.Load(connKey)
	if !ok {
		return nil, false
	}
	return dc.findAll()
}

// newConnID 生成网关内唯一的连接 ID。
func newConnID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
