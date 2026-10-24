package conn

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/redis/go-redis/v9"
)

const (
	defaultTTL       = time.Hour // 连接注册信息的过期时间 ( 防止网关crash后没清理 )
	defaultKeyPrefix = "synp:conn:registry"

	// nodeMemberVersionV2 为当前 member 编码版本:
	// v2:{name}:{addr}:{connID}。
	// 连接粒度的 member 使同一节点上多设备/多连接各自持有路由记录，
	// 单个连接断开不再误删整节点的记录 ( v1 遗留格式仍可解析 )。
	nodeMemberVersionV2 = "v2"
	nodeMemberVersionV1 = "v1"
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
// 使用 Redis ZSET 存储用户连接所在网关节点及其过期时间 ( score )。
//
// TODO: 注册结构需要调整。
//
//	┌──────────────────────────────────────┐
//	│              Redis                   │
//	│ user → zset ( node member, expire )  │
//	│  ├─ v1:<name>:<addr>: 1710000001000  │  ← 手机连接
//	│  └─ v1:<name>:<addr>: 1710000002000  │  ← PC连接
//	└──────────────────────────────────────┘
//
// 说明：
//  1. 每个节点有自己的过期时间，避免 key 级 TTL 导致脏节点被续命。
//  2. FindNodes 时会先清理过期节点，再返回当前有效节点。
//
// 注：更优方案是独立出一个 Route Service。
//  1. 网关节点 -> gRPC -> Route Service -> Redis ( 用户建立连接时注册 )。
//  2. 业务服务端 -> gRPC -> Route Service ( 下行消息转发时查询目标网关集群 )。
type ConnRegistry struct {
	rdb redis.Cmdable

	// 网关节点标识。
	// 使用 xgrpc 中 ServiceInstance 的 Name 和 Addr 字段。
	node      synp.ConnNode
	ttl       time.Duration
	keyPrefix string

	logger *slog.Logger
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

func NewConnRegistry(rdb redis.Cmdable, node synp.ConnNode, opts ...option.Opt[ConnRegistry]) *ConnRegistry {
	registry := &ConnRegistry{
		rdb: rdb,

		node:      node,
		ttl:       defaultTTL,
		keyPrefix: defaultKeyPrefix,

		logger: logging.For("conn.registry"),
	}

	option.Apply(registry, opts...)
	return registry
}

// Register 注册用户连接到当前网关节点。
//
// 使用 Redis ZSET 结构存储，member 为连接粒度:
//
//   - key: key_prefix:{bid}:{uid}
//   - member: v2:{base64url(service_name)}:{base64url(service_addr)}:{connID}
//   - score: 该连接路由记录的过期时间戳 ( unix ms )
func (r *ConnRegistry) Register(ctx context.Context, user session.User, connID string) error {
	start := time.Now()
	defer func() {
		metrics.RegistryOpDuration.WithLabelValues("register").Observe(time.Since(start).Seconds())
	}()

	key := r.key(user.BID, user.UID)
	ttlMillis := r.ttl.Milliseconds()
	if ttlMillis <= 0 {
		return fmt.Errorf("invalid ttl: %s", r.ttl)
	}
	expireAtMillis := time.Now().Add(r.ttl).UnixMilli()

	// 原子执行 ZADD + PEXPIRE：
	//
	// - ZADD: 	  写入/续期当前连接的路由记录。
	// - PEXPIRE: 兜底控制 key 生命周期，避免长期冷数据堆积。
	member := encodeNodeMember(r.node, connID)
	res, err := connRegisterScript.Run(ctx, r.rdb, []string{key}, expireAtMillis, member, ttlMillis).Result()
	if err != nil {
		r.logger.Error(
			"[conn-registry] failed to register connection",
			"key", key,
			"conn_id", connID,
			"service_name", r.node.ServiceName,
			"service_addr", r.node.ServiceAddr,
			"error", err,
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

// Unregister 注销用户连接 ( 连接粒度，不影响同节点的其它连接 )。
func (r *ConnRegistry) Unregister(ctx context.Context, user session.User, connID string) error {
	start := time.Now()
	defer func() {
		metrics.RegistryOpDuration.WithLabelValues("unregister").Observe(time.Since(start).Seconds())
	}()

	key := r.key(user.BID, user.UID)
	if err := r.rdb.ZRem(ctx, key, encodeNodeMember(r.node, connID)).Err(); err != nil {
		r.logger.Error(
			"[conn-registry] failed to unregister connection",
			"key", key,
			"conn_id", connID,
			"service_name", r.node.ServiceName,
			"service_addr", r.node.ServiceAddr,
			"error", err,
		)
		return fmt.Errorf("failed to unregister connection: %w", err)
	}
	return nil
}

// FindNodes 查找用户所有连接所在的网关节点。
func (r *ConnRegistry) FindNodes(ctx context.Context, bid, uid uint64) ([]synp.ConnNode, error) {
	start := time.Now()
	defer func() {
		metrics.RegistryOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
	}()

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
			"key", key,
			"error", err,
		)
		return nil, fmt.Errorf("failed to find nodes: %w", err)
	}

	members, err := parseLuaStringSliceResult(res)
	if err != nil {
		return nil, fmt.Errorf("failed to find nodes: %w", err)
	}

	nodes := make([]synp.ConnNode, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		node, err := decodeNodeMember(member)
		if err != nil {
			return nil, fmt.Errorf("failed to decode node member: %w", err)
		}

		// 同一节点可能有多个连接的路由记录 ( 多设备 )，按节点去重。
		dedupKey := node.ServiceName + "|" + node.ServiceAddr
		if _, ok := seen[dedupKey]; ok {
			continue
		}
		seen[dedupKey] = struct{}{}

		nodes = append(nodes, node)
	}
	return nodes, nil
}

// Heartbeat 心跳 ( 防止连接注册信息过期 )。
func (r *ConnRegistry) Heartbeat(ctx context.Context, user session.User, connID string) error {
	start := time.Now()
	defer func() {
		metrics.RegistryOpDuration.WithLabelValues("heartbeat").Observe(time.Since(start).Seconds())
	}()

	// 心跳等价于刷新当前连接路由记录的过期时间。
	return r.Register(ctx, user, connID)
}

func (r *ConnRegistry) key(bid, uid uint64) string {
	return fmt.Sprintf("%s:%d:%d", r.keyPrefix, bid, uid)
}

// encodeNodeMember 生成连接粒度的路由 member:
// v2:{base64url(service_name)}:{base64url(service_addr)}:{connID}。
func encodeNodeMember(node synp.ConnNode, connID string) string {
	encoder := base64.RawURLEncoding
	return nodeMemberVersionV2 + ":" +
		encoder.EncodeToString([]byte(node.ServiceName)) + ":" +
		encoder.EncodeToString([]byte(node.ServiceAddr)) + ":" +
		connID
}

// decodeNodeMember 解析路由 member，兼容 v1 ( 节点粒度 ) 与 v2 ( 连接粒度 ) 格式。
// 返回的 ConnNode 已剥离连接维度 ( 节点信息 )。
func decodeNodeMember(member string) (synp.ConnNode, error) {
	version, payload, ok := strings.Cut(member, ":")
	if !ok || (version != nodeMemberVersionV2 && version != nodeMemberVersionV1) {
		return synp.ConnNode{}, fmt.Errorf("unsupported node member format %q", member)
	}

	switch version {
	case nodeMemberVersionV2:
		// v2: {name}:{addr}:{connID} —— connID 为网关生成的 hex，无需解码。
		encodedName, rest, ok := strings.Cut(payload, ":")
		if !ok {
			return synp.ConnNode{}, fmt.Errorf("invalid node member format %q", member)
		}
		encodedAddr, _, ok := strings.Cut(rest, ":")
		if !ok {
			return synp.ConnNode{}, fmt.Errorf("invalid node member format %q", member)
		}
		return decodeNodeParts(encodedName, encodedAddr)
	default:
		// v1: {name}:{addr}。
		encodedName, encodedAddr, ok := strings.Cut(payload, ":")
		if !ok {
			return synp.ConnNode{}, fmt.Errorf("invalid node member format %q", member)
		}
		return decodeNodeParts(encodedName, encodedAddr)
	}
}

func decodeNodeParts(encodedName, encodedAddr string) (synp.ConnNode, error) {
	decoder := base64.RawURLEncoding
	serviceName, err := decoder.DecodeString(encodedName)
	if err != nil {
		return synp.ConnNode{}, fmt.Errorf("invalid service name: %w", err)
	}
	serviceAddr, err := decoder.DecodeString(encodedAddr)
	if err != nil {
		return synp.ConnNode{}, fmt.Errorf("invalid service addr: %w", err)
	}

	return synp.ConnNode{
		ServiceName: string(serviceName),
		ServiceAddr: string(serviceAddr),
	}, nil
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
