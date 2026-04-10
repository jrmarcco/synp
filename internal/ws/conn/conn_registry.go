package conn

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/jrmarcco/xgrpc/register"
	gocache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	defaultTTL           = time.Hour              // 连接注册信息的过期时间 ( 防止网关crash后没清理 )
	defaultCacheTTL      = time.Millisecond * 500 // 非空连接节点缓存时间
	defaultEmptyCacheTTL = time.Millisecond * 100 // 空结果缓存时间 ( 负缓存，避免穿透 )
	defaultKeyPrefix     = "synp:conn:registry"
	memberVersionV1      = "v1"
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
//
// 本地缓存 ttl 配置建议：
//  1. 一致性优先场景 ( 上线用户要尽快被看到 )
//     - cacheTTL: 200ms
//     - emptyCacheTTL: 50ms
//  2. 均衡场景 ( 兼顾一致性和性能 )
//     - cacheTTL: 400ms
//     - emptyCacheTTL: 100ms
//  3. 性能优先场景 ( 降低 Redis 压力 )
//     - cacheTTL: 800ms
//     - emptyCacheTTL: 200ms
//
// 在非严格一致性哈希路由的情况下建议先使用均衡场景并观察两个指标:
//  1. Redis FindNodes QPS/RT。
//  2. “刚连上却判离线”的短时比例 ( 或首次推送失败率 )。
type ConnRegistry struct {
	rdb redis.Cmdable

	// 本地缓存。
	lcache *gocache.Cache
	// 本地缓存 TTL ( 非空结果 )。
	// 建议配置区间为 200ms ~ 1000ms。
	cacheTTL time.Duration
	// 本地缓存 TTL ( 空结果 )。
	// emptyCacheTTL 建议是 cacheTTL 的 1/3~ 1/5，建议配置区间为 50ms ~ 300ms。
	// 不建议 emptyCacheTTL > cacheTTL。
	emptyCacheTTL time.Duration

	// 网关节点标识。
	// xgrpc 的 ServiceInstance ( 包含服务名称、地址等信息 )。
	si  register.ServiceInstance
	ttl time.Duration

	keyPrefix string

	logger *zap.Logger
}

func ConnRegistryWithTTL(ttl time.Duration) option.Opt[ConnRegistry] {
	return func(r *ConnRegistry) {
		r.ttl = ttl
	}
}

func ConnRegistryWithCacheTTL(cacheTTL time.Duration) option.Opt[ConnRegistry] {
	return func(r *ConnRegistry) {
		r.cacheTTL = cacheTTL
	}
}

func ConnRegistryWithEmptyCacheTTL(emptyCacheTTL time.Duration) option.Opt[ConnRegistry] {
	return func(r *ConnRegistry) {
		r.emptyCacheTTL = emptyCacheTTL
	}
}

func ConnRegistryWithKeyPrefix(keyPrefix string) option.Opt[ConnRegistry] {
	return func(r *ConnRegistry) {
		r.keyPrefix = keyPrefix
	}
}

func NewConnRegistry(
	rdb redis.Cmdable,
	cache *gocache.Cache,
	si register.ServiceInstance,
	logger *zap.Logger,
	opts ...option.Opt[ConnRegistry],
) *ConnRegistry {
	registry := &ConnRegistry{
		rdb: rdb,

		si:        si,
		ttl:       defaultTTL,
		keyPrefix: defaultKeyPrefix,

		lcache:        cache,
		cacheTTL:      defaultCacheTTL,
		emptyCacheTTL: defaultEmptyCacheTTL,

		logger: logger,
	}

	option.Apply(registry, opts...)
	return registry
}

// Register 注册用户连接到当前网关节点。
//
// 使用 Redis ZSET 结构存储。
//
//   - key: key_prefix:{bid}:{uid}
//   - member: node
//   - score: 节点过期时间戳 ( unix ms )
func (r *ConnRegistry) Register(ctx context.Context, user session.User) error {
	ttlMillis := r.ttl.Milliseconds()
	if ttlMillis <= 0 {
		return fmt.Errorf("invalid ttl: %s", r.ttl)
	}
	expireAtMillis := time.Now().Add(r.ttl).UnixMilli()

	// 原子执行 ZADD + PEXPIRE：
	//
	// - ZADD: 	  更新当前 node 的过期时间。
	// - PEXPIRE: 兜底控制 key 生命周期，避免长期冷数据堆积。
	key := r.key(user.BID, user.UID)
	member := encodeMember(r.si.Name, r.si.Addr)
	res, err := connRegisterScript.Run(ctx, r.rdb, []string{key}, expireAtMillis, member, ttlMillis).Result()
	if err != nil {
		r.logger.Error(
			"[conn-registry] failed to register connection",
			zap.String("key", key),
			zap.String("service_name", r.si.Name),
			zap.String("service_addr", r.si.Addr),
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

	// 写路径失效本地缓存，避免刚注册后仍读取旧节点列表。
	r.invalidateLocalCache(key)
	return nil
}

// Unregister 注销用户连接。
func (r *ConnRegistry) Unregister(ctx context.Context, user session.User) error {
	key := r.key(user.BID, user.UID)
	member := encodeMember(r.si.Name, r.si.Addr)
	if err := r.rdb.ZRem(ctx, key, member).Err(); err != nil {
		r.logger.Error(
			"[conn-registry] failed to unregister connection",
			zap.String("key", key),
			zap.String("service_name", r.si.Name),
			zap.String("service_addr", r.si.Addr),
			zap.Error(err),
		)
		return fmt.Errorf("failed to unregister connection: %w", err)
	}

	// 写路径失效本地缓存，避免刚下线后仍读取到旧节点列表。
	r.invalidateLocalCache(key)
	return nil
}

// FindNodes 查找用户所有连接所在的网关节点。
func (r *ConnRegistry) FindNodes(ctx context.Context, bid, uid uint64) ([]synp.ConnNode, error) {
	key := r.key(bid, uid)

	// 先从本地缓存获取 ( 默认 TTL 为 500 ms )。
	// 这里客户端使用一致性哈希负载均衡策略才能保证缓存命中。
	if nodes, ok := r.getCachedNodes(key); ok {
		return nodes, nil
	}

	// 使用 Lua 原子执行：
	//  1. 清理过期节点。
	//  2. 查询有效节点。
	// 避免客户端侧 pipeline 的并发窗口与额外往返。
	now := time.Now().UnixMilli()
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

	nodes := make([]synp.ConnNode, 0, len(members))
	for _, member := range members {
		name, addr, err := decodeMember(member)
		if err != nil {
			r.logger.Warn(
				"[conn-registry] failed to decode member",
				zap.String("member", member),
				zap.Error(err),
			)
			continue
		}
		nodes = append(nodes, synp.ConnNode{
			ServiceName: name,
			ServiceAddr: addr,
		})
	}

	r.setCachedNodes(key, nodes)

	return nodes, nil
}

// Heartbeat 心跳 ( 防止连接注册信息过期 )。
func (r *ConnRegistry) Heartbeat(ctx context.Context, user session.User) error {
	// 心跳等价于刷新当前 node 的过期时间。
	return r.Register(ctx, user)
}

func (r *ConnRegistry) key(bid, uid uint64) string {
	return fmt.Sprintf("%s:%d:%d", r.keyPrefix, bid, uid)
}

// getCachedNodes 从本地缓存获取节点列表。
func (r *ConnRegistry) getCachedNodes(key string) ([]synp.ConnNode, bool) {
	if r.lcache == nil {
		return nil, false
	}

	val, ok := r.lcache.Get(key)
	if !ok {
		return nil, false
	}

	nodes, ok := val.([]synp.ConnNode)
	return nodes, ok
}

// setCachedNodes 缓存节点列表到本地。
func (r *ConnRegistry) setCachedNodes(key string, nodes []synp.ConnNode) {
	if r.lcache == nil {
		return
	}

	ttl := r.cacheTTL
	if len(nodes) == 0 {
		ttl = r.emptyCacheTTL
	}
	if ttl <= 0 {
		return
	}
	r.lcache.Set(key, nodes, ttl)
}

// invalidateLocalCache 失效本地缓存。
func (r *ConnRegistry) invalidateLocalCache(key string) {
	if r.lcache == nil {
		return
	}
	r.lcache.Delete(key)
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

func encodeMember(serviceName, serviceAddr string) string {
	return strings.Join([]string{
		memberVersionV1,
		base64.RawURLEncoding.EncodeToString([]byte(serviceName)),
		base64.RawURLEncoding.EncodeToString([]byte(serviceAddr)),
	}, "|")
}

func decodeMember(member string) (serviceName, serviceAddr string, err error) {
	// 成员由 3 部分组成：
	//  1. 版本号
	//  2. 服务名称 ( base64 编码 )
	//  3. 服务地址 ( base64 编码 )
	const memberParts = 3

	parts := strings.SplitN(member, "|", memberParts)
	if len(parts) != memberParts || parts[0] != memberVersionV1 {
		return "", "", fmt.Errorf("invalid member: %s", member)
	}

	nodeBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("decode node: %w", err)
	}
	serviceNameBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", fmt.Errorf("decode service name: %w", err)
	}
	return string(nodeBytes), string(serviceNameBytes), nil
}
