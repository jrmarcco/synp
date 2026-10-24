package conn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/jit/xsync"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/compression"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/session"
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

	// DefaultEstablishedReadTimeout 为鉴权完成后的连接读超时。
	// 鉴权前使用短超时 ( DefaultReadTiemout ) 保障 AUTH 超时及时生效；
	// 鉴权后连接由应用层心跳保活，长超时显著减少空闲连接的
	// 周期性 deadline 唤醒 ( 10 万连接下从 ~6.7k 次/s 降至 ~1.1k 次/s )。
	DefaultEstablishedReadTimeout = 90 * time.Second
)

// pendingConnKey 为反向索引中未绑定连接 ( 尚未完成 AUTH ) 的占位 key。
const pendingConnKey = ""

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

// remove 按设备移除连接。
// remaining 为锁内统计的剩余连接数，供调用方判断是否清理用户索引，
// 必须在持锁时计算以避免与并发 add 的竞态。
func (dc *DeviceConns) remove(device session.Device) (conn synp.Conn, remaining int, ok bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if conn, ok = dc.conns[device]; ok {
		delete(dc.conns, device)
	}
	return conn, len(dc.conns), ok
}

// removeByID 按连接 ID 移除连接。
// 返回值语义同 remove。
func (dc *DeviceConns) removeByID(connID string) (device session.Device, conn synp.Conn, remaining int, ok bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	for device, conn = range dc.conns {
		if conn.ID() == connID {
			delete(dc.conns, device)
			return device, conn, len(dc.conns), true
		}
	}
	return "", nil, 0, false
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

// each 遍历连接，fn 返回 false 时提前终止。
func (dc *DeviceConns) each(fn func(synp.Conn) bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	for _, conn := range dc.conns {
		if !fn(conn) {
			return
		}
	}
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

	// EstablishedReadTimeout 为鉴权完成后的读超时 ( 0 使用默认 90s )。
	EstablishedReadTimeout time.Duration

	InitRetryInterval time.Duration
	MaxRetryInterval  time.Duration
	MaxRetryCount     int32

	SendBufferSize    int
	ReceiveBufferSize int

	CloseTimeout time.Duration
	RateLimit    int

	// MaxFrameBytes 为入站数据帧大小上限 ( 0 使用 xws 默认值 64KiB )。
	MaxFrameBytes int64
}

var _ synp.ConnManager = (*ConnManager)(nil)

// ConnManager 管理网关节点上的全部连接。
//
// 连接在建立时身份未知 ( 新协议鉴权在连接建立后的首帧 AUTH 完成 )，
// 先进入 pending 区；鉴权成功后由 BindUser 绑定到用户索引。
//
// connIndex 为 connID → connKey 的反向索引，
// 使断开清理 ( RemoveConnByID ) 无需遍历用户索引，复杂度 O(1)。
type ConnManager struct {
	cfg *ConnConfig

	// pending 保存未完成鉴权的连接 ( connID -> conn )。
	pending *xsync.Map[string, synp.Conn]

	conns   *xsync.Map[string, *DeviceConns]
	connCnt atomic.Int64
	userCnt atomic.Int64

	// connIndex 为 connID → connKey 反向索引 ( 未绑定连接的 key 为空串 )。
	connIndex *xsync.Map[string, string]

	// userLocks 为按 connKey 哈希分片的绑定锁，
	// 序列化同一用户的 BindUser/RemoveConn/RemoveConnByID，
	// 消除并发顶号时的覆盖与计数泄漏窗口。
	userLocks [userLockShards]sync.Mutex

	logger *slog.Logger
}

// userLockShards 为绑定锁分片数。
// 锁粒度为「同一用户的绑定操作」，分片避免全局串行化。
const userLockShards = 256

// userLockShard 返回 connKey 对应的分片锁。
func (m *ConnManager) userLockShard(connKey string) *sync.Mutex {
	var hash uint32
	for _, c := range connKey {
		hash = hash*31 + uint32(c)
	}
	return &m.userLocks[hash%userLockShards]
}

func ConnManagerWithConfig(cfg *ConnConfig) option.Opt[ConnManager] {
	return func(m *ConnManager) {
		m.cfg = cfg
	}
}

func NewConnManager(opts ...option.Opt[ConnManager]) *ConnManager {
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
		cfg:       cfg,
		pending:   &xsync.Map[string, synp.Conn]{},
		conns:     &xsync.Map[string, *DeviceConns]{},
		connIndex: &xsync.Map[string, string]{},
		logger:    logging.For("conn.manager"),
	}

	option.Apply(cm, opts...)

	return cm
}

// NewConn 创建连接。
// 新协议下连接建立时身份未知，连接先注册到 pending 区，
// 完成首帧 AUTH 鉴权后由 BindUser 绑定到用户索引。
//
// 连接计数语义: connCnt 为当前在线连接总数 ( 含未鉴权连接 )，
// 在 NewConn 时 +1、断开清理时 -1，BindUser 仅移动归属不计新增。
func (m *ConnManager) NewConn(ctx context.Context, netConn net.Conn, sess session.Session, compressionState *compression.State) (synp.Conn, error) {
	// 匿名连接无法从 user 推导 connID，由网关生成全局唯一 ID。
	connID, err := newConnID()
	if err != nil {
		return nil, err
	}

	opts := m.convertToConnOpts(sess.User(), compressionState)
	conn := NewConn(ctx, connID, sess, netConn, opts...)

	m.pending.Store(connID, conn)
	m.connIndex.Store(connID, pendingConnKey)
	m.connCnt.Add(1)

	m.logger.Debug(
		"[conn-manager] successfully create connection",
		"conn_id", connID,
		"device", sess.User().Device,
	)
	return conn, nil
}

// BindUser 将完成鉴权的连接绑定到用户索引。
// 同一用户同一设备已有连接时关闭旧连接 ( 顶号 )。
func (m *ConnManager) BindUser(conn synp.Conn, user session.User) error {
	connKey := user.ConnKey()
	device := user.Device

	var kickedConn synp.Conn

	func() {
		// 同一用户的绑定/解绑操作串行化，防止并发顶号互相覆盖。
		// 锁只覆盖索引操作；连接关闭 ( 含 close frame 写超时 ) 在锁外执行。
		lock := m.userLockShard(connKey)
		lock.Lock()
		defer lock.Unlock()

		// 检查同一设备是否已有连接，如果有则移除旧连接 ( 顶号 )。
		if _, exists := m.loadExistingConn(connKey, device); exists {
			m.logger.Info(
				"[conn-manager] found existing connection for same device, closing old connection",
				"conn_key", connKey,
				"device", string(device),
				"user", user,
			)

			kickedConn, _ = m.removeConnLocked(user)
		}

		m.storeConn(connKey, device, conn)
		m.pending.Delete(conn.ID())
		m.connIndex.Store(conn.ID(), connKey)
	}()

	if kickedConn != nil {
		m.closeConn(kickedConn)
	}

	m.logger.Debug(
		"[conn-manager] successfully bind connection to user",
		"conn_id", conn.ID(),
		"conn_key", connKey,
		"device", string(device),
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
	if m.cfg.EstablishedReadTimeout > 0 {
		opts = append(opts, ConnWithEstablishedReadTimeout(m.cfg.EstablishedReadTimeout))
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

	if m.cfg.MaxFrameBytes > 0 {
		opts = append(opts, ConnWithMaxFrameBytes(m.cfg.MaxFrameBytes))
	}

	opts = append(
		opts,
		ConnWithAutoClose(user.AutoClose),
		ConnWithRateLimit(m.cfg.RateLimit),
	)

	return opts
}

// RemoveConn 移除并关闭用户指定设备的连接 ( 顶号等场景 )。
func (m *ConnManager) RemoveConn(user session.User) bool {
	conn, ok := func() (synp.Conn, bool) {
		lock := m.userLockShard(user.ConnKey())
		lock.Lock()
		defer lock.Unlock()

		return m.removeConnLocked(user)
	}()
	if ok {
		// 连接关闭 ( 含 close frame 写超时 ) 移出锁外。
		m.closeConn(conn)
	}
	return ok
}

// removeConnLocked 为 RemoveConn 的持锁实现，只做索引操作、不关闭连接。
// 调用方必须已持有该用户的分片锁。
func (m *ConnManager) removeConnLocked(user session.User) (synp.Conn, bool) {
	connKey := user.ConnKey()

	dc, ok := m.conns.Load(connKey)
	if !ok {
		return nil, false
	}

	conn, remaining, ok := dc.remove(user.Device)
	if ok {
		m.removeConnEntry(connKey, remaining)

		// 反向索引与连接计数随移除一并收口。
		m.connIndex.Delete(conn.ID())
		m.connCnt.Add(-1)
	}

	return conn, ok
}

// RemoveConnByID 按连接 ID 移除连接。
// 经反向索引 O(1) 定位，同时覆盖 pending 区与用户索引区。
// 连接断开的统一清理入口。
func (m *ConnManager) RemoveConnByID(connID string) bool {
	// 反向索引定位归属。
	connKey, ok := m.connIndex.LoadAndDelete(connID)
	if !ok {
		// 未注册或已被并发清理。
		return false
	}

	// pending 区: 尚未完成鉴权的连接。
	if conn, existed := m.pending.LoadAndDelete(connID); existed {
		m.connCnt.Add(-1)
		m.closeConn(conn)
		return true
	}

	// 用户索引区: 已绑定连接。
	conn, removed := func() (synp.Conn, bool) {
		lock := m.userLockShard(connKey)
		lock.Lock()
		defer lock.Unlock()

		dc, ok := m.conns.Load(connKey)
		if !ok {
			return nil, false
		}

		if _, conn, remaining, ok := dc.removeByID(connID); ok {
			m.removeConnEntry(connKey, remaining)
			m.connCnt.Add(-1)
			return conn, true
		}
		return nil, false
	}()

	if removed {
		m.closeConn(conn)
		return true
	}

	return false
}

// removeConnEntry 在用户连接数归零时清理用户索引。
func (m *ConnManager) removeConnEntry(connKey string, remaining int) {
	if remaining == 0 {
		if _, deleted := m.conns.LoadAndDelete(connKey); deleted {
			m.userCnt.Add(-1)
		}
	}
}

func (m *ConnManager) closeConn(conn synp.Conn) {
	if err := conn.Close(); err != nil {
		m.logger.Warn(
			"[conn-manager] failed to close connection",
			"conn_id", conn.ID(),
			"error", err,
		)
	}
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
	dc, ok := m.conns.Load(user.ConnKey())
	if !ok {
		return nil, false
	}
	return dc.findAll()
}

// RangeConns 遍历当前节点全部连接 ( 含未鉴权连接 )，
// fn 返回 false 时提前终止。供关停排空等全量巡检使用。
func (m *ConnManager) RangeConns(fn func(synp.Conn) bool) {
	m.pending.Range(func(_ string, conn synp.Conn) bool {
		return fn(conn)
	})

	m.conns.Range(func(_ string, dc *DeviceConns) bool {
		dc.each(fn)
		return true
	})
}

// RangeConnsShard 分片遍历: 仅对 hash(connID) % shards == shard 的连接调用 fn。
// 空闲扫描等周期性巡检按分片摊薄单次开销，
// 大规模连接下避免单次扫描集中获取全部连接锁。
func (m *ConnManager) RangeConnsShard(shard, shards uint64, fn func(synp.Conn) bool) {
	if shards <= 0 {
		shards = 1
	}

	filter := func(conn synp.Conn) bool {
		if connIDShard(conn.ID(), shards) == shard {
			return fn(conn)
		}
		return true
	}

	m.pending.Range(func(_ string, conn synp.Conn) bool {
		return filter(conn)
	})

	m.conns.Range(func(_ string, dc *DeviceConns) bool {
		dc.each(filter)
		return true
	})
}

// connIDShard 计算 connID 的分片号 ( FNV-1a 风格散列 )。
func connIDShard(connID string, shards uint64) uint64 {
	var hash uint64
	for _, c := range connID {
		hash = hash*31 + uint64(c)
	}
	return hash % shards
}

// ConnCount 返回当前连接总数 ( 含未鉴权连接 )。
func (m *ConnManager) ConnCount() int64 {
	return m.connCnt.Load()
}

// UserCount 返回当前在线用户数。
func (m *ConnManager) UserCount() int64 {
	return m.userCnt.Load()
}

// newConnID 生成网关内唯一的连接 ID。
func newConnID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
