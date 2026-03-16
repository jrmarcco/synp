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

var _ synp.ConnRegistry = (*ConnRegistry)(nil)

// ConnRegistry 是连接注册表。
//
// ┌───────────────┐
// │     Redis     │
// │ user  →  set  │
// │  ├─ node-1    │  ← 手机连接
// │  └─ node-2    │  ← PC连接
// └───────────────┘
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
// 使用 Redis Set 结构存储。
//   - key: key_prefix:{bid}:{uid}
//   - val: [node-1, node-2]
func (r *ConnRegistry) Register(ctx context.Context, user session.User) error {
	key := r.key(user.BID, user.UID)
	ttlMillis := r.ttl.Milliseconds()
	if ttlMillis <= 0 {
		return fmt.Errorf("invalid ttl: %s", r.ttl)
	}

	// 原子地执行 SADD + PEXPIRE 避免 SADD 成功但 EXPIRE 失败导致脏数据常驻。
	// 这里使用的 PEXPIRE 命令以毫秒为单位 ( EXPIRE 命令以秒为单位 )。
	res, err := r.rdb.Eval(ctx, connRegisterLua, []string{key}, r.node, ttlMillis).Result()
	if err != nil {
		r.logger.Error(
			"[conn-registry] failed to register connection",
			zap.String("key", key),
			zap.String("node", r.node),
			zap.Error(err),
		)
		return fmt.Errorf("failed to register connection: %w", err)
	}

	if res != "ok" {
		return fmt.Errorf("failed to register connection: failed to set pexpire")
	}

	return nil
}

// Unregister 注销用户连接。
func (r *ConnRegistry) Unregister(ctx context.Context, user session.User) error {
	key := r.key(user.BID, user.UID)
	if err := r.rdb.SRem(ctx, key, r.node).Err(); err != nil {
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

	members, err := r.rdb.SMembers(ctx, key).Result()
	if err != nil {
		r.logger.Error(
			"[conn-registry] failed to find nodes",
			zap.String("key", key),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to find nodes: %w", err)
	}
	return members, nil
}

// Heartbeat 心跳 ( 防止连接注册信息过期 )。
func (r *ConnRegistry) Heartbeat(ctx context.Context, user session.User) error {
	key := r.key(user.BID, user.UID)
	if err := r.rdb.Expire(ctx, key, r.ttl).Err(); err != nil {
		r.logger.Error(
			"[conn-registry] failed to heartbeat",
			zap.String("key", key),
			zap.String("node", r.node),
			zap.Error(err),
		)
		return fmt.Errorf("failed to heartbeat: %w", err)
	}
	return nil
}

func (r *ConnRegistry) key(bid, uid uint64) string {
	return fmt.Sprintf("%s:%d:%d", r.keyPrefix, bid, uid)
}
