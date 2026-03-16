package conn

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jrmarcco/jit/bean/option"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	defaultTTL       = time.Hour // 连接注册信息的过期时间 ( 防止网关crash后没清理 )
	defaultKeyPrefix = "synp:conn:registry"
)

//go:embed lua/conn_register.lua
var connRegisterLua string

//go:embed lua/conn_find_nodes.lua
var connFindNodesLua string

// 脚本对象缓存。
// 有限执行 EVALSHA 减少重复传输脚本文件和解析开销。
var (
	connRegisterScript  = redis.NewScript(connRegisterLua)
	connFindNodesScript = redis.NewScript(connFindNodesLua)
)

var _ synp.ConnRegistry = (*ConnRegistry)(nil)

// ConnRegistry 是连接注册表。
//
// 使用 Redis ZSET 存储用户连接所在网关节点及其过期时间（score）。
//
//	┌────────────────────────────────────┐
//	│              Redis                 │
//	│ user → zset ( node, expire_at_ms ) │
//	│  ├─ node-1: 1710000001000          │  ← 手机连接
//	│  └─ node-2: 1710000002000          │  ← PC连接
//	└────────────────────────────────────┘
//
// 说明：
//  1. 每个节点有自己的过期时间，避免 key 级 TTL 导致脏节点被续命。
//  2. FindNodes 时会先清理过期节点，再返回当前有效节点。
type ConnRegistry struct {
	rdb redis.Cmdable

	node      string
	ttl       time.Duration
	keyPrefix string

	logger *zap.Logger
}

func ConnRegistryWithTTL(ttl time.Duration) option.Opt[ConnRegistry] {
	return func(r *ConnRegistry) {
		r.ttl = ttl
	}
}

func ConnRegistryWithKeyPrefix(keyPrefix string) option.Opt[ConnRegistry] {
	return func(r *ConnRegistry) {
		r.keyPrefix = keyPrefix
	}
}

func NewConnRegistry(rdb redis.Cmdable, node string, logger *zap.Logger, opts ...option.Opt[ConnRegistry]) *ConnRegistry {
	registry := &ConnRegistry{
		rdb: rdb,

		node:      node,
		ttl:       defaultTTL,
		keyPrefix: defaultKeyPrefix,

		logger: logger,
	}

	option.Apply(registry, opts...)
	return registry
}

// Register 注册用户连接到当前网关节点。
//
// 使用 Redis ZSET 结构存储。
//   - key: key_prefix:{bid}:{uid}
//   - member: node
//   - score: 节点过期时间戳 ( unix ms )
func (r *ConnRegistry) Register(ctx context.Context, user session.User) error {
	key := r.key(user.BID, user.UID)
	ttlMillis := r.ttl.Milliseconds()
	if ttlMillis <= 0 {
		return fmt.Errorf("invalid ttl: %s", r.ttl)
	}
	expireAtMillis := time.Now().Add(r.ttl).UnixMilli()

	// 原子执行 ZADD + PEXPIRE：
	// - ZADD: 	  更新当前 node 的过期时间。
	// - PEXPIRE: 兜底控制 key 生命周期，避免长期冷数据堆积。
	res, err := connRegisterScript.Run(ctx, r.rdb, []string{key}, expireAtMillis, r.node, ttlMillis).Result()
	if err != nil {
		r.logger.Error(
			"[conn-registry] failed to register connection",
			zap.String("key", key),
			zap.String("node", r.node),
			zap.Error(err),
		)
		return fmt.Errorf("failed to register connection: %w", err)
	}

	ok, err := parseLuaBoolResult(res)
	if err != nil {
		return fmt.Errorf("failed to register connection: %w", err)
	}
	if !ok {
		return fmt.Errorf("failed to register connection: pexpire returned 0")
	}

	return nil
}

// Unregister 注销用户连接。
func (r *ConnRegistry) Unregister(ctx context.Context, user session.User) error {
	key := r.key(user.BID, user.UID)
	if err := r.rdb.ZRem(ctx, key, r.node).Err(); err != nil {
		r.logger.Error(
			"[conn-registry] failed to unregister connection",
			zap.String("key", key),
			zap.String("node", r.node),
			zap.Error(err),
		)
		return fmt.Errorf("failed to unregister connection: %w", err)
	}
	return nil
}

// FindNodes 查找用户所有连接所在的网关节点。
func (r *ConnRegistry) FindNodes(ctx context.Context, bid, uid uint64) ([]string, error) {
	key := r.key(bid, uid)
	now := time.Now().UnixMilli()

	// 使用 Lua 原子执行：
	//  1. 清理过期节点。
	//  2. 查询有效节点。
	// 避免客户端侧 pipeline 的并发窗口与额外往返。
	res, err := connFindNodesScript.Run(ctx, r.rdb, []string{key}, now).Result()
	if err != nil {
		r.logger.Error(
			"[conn-registry] failed to find nodes",
			zap.String("key", key),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to find nodes: %w", err)
	}

	members, err := parseLuaStringSliceResult(res)
	if err != nil {
		return nil, fmt.Errorf("failed to find nodes: %w", err)
	}

	return members, nil
}

// Heartbeat 心跳 ( 防止连接注册信息过期 )。
func (r *ConnRegistry) Heartbeat(ctx context.Context, user session.User) error {
	// 心跳等价于刷新当前 node 的过期时间。
	return r.Register(ctx, user)
}

func (r *ConnRegistry) key(bid, uid uint64) string {
	return fmt.Sprintf("%s:%d:%d", r.keyPrefix, bid, uid)
}

// parseLuaBoolResult 解析 Lua 脚本返回的 bool 结果。
func parseLuaBoolResult(res any) (bool, error) {
	switch v := res.(type) {
	case int64:
		return v == 1, nil
	case bool:
		return v, nil
	default:
		return false, fmt.Errorf("unexpected lua bool result type %T", res)
	}
}

// parseLuaStringSliceResult 解析 Lua 脚本返回的 string 切片结果。
func parseLuaStringSliceResult(res any) ([]string, error) {
	// 优先尝试 []string。
	if members, ok := res.([]string); ok {
		return members, nil
	}

	// []string 失败再回退 []any 逐项转换。
	rawMembers, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected lua string slice result type %T", res)
	}

	members := make([]string, 0, len(rawMembers))
	for _, item := range rawMembers {
		node, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected lua member type %T", item)
		}
		members = append(members, node)
	}
	return members, nil
}
