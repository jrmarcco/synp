// Package metrics 提供网关的 Prometheus 指标集。
//
// 指标在包内注册 ( 默认注册表 )，由 admin 端口的 /metrics 暴露。
// 连接数/用户数使用 GaugeFunc 在抓取时向 ConnManager 即时求值，
// 避免维护定时采样 goroutine。
package metrics

import (
	"github.com/jrmarcco/synp/internal/pkg/buildinfo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Namespace 为全部指标的前缀。
var namespace = "synp"

var (
	// OnlineConns 当前连接总数 ( 含未鉴权连接 )，抓取时即时求值。
	OnlineConns prometheus.GaugeFunc

	// OnlineUsers 当前在线用户数，抓取时即时求值。
	OnlineUsers prometheus.GaugeFunc
)

// InitConnStats 注入连接统计的采集函数并注册连接数指标。
// 由装配层在 ConnManager 就绪后调用一次。
func InitConnStats(connCount, userCount func() int64) {
	OnlineConns = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "online_connections",
		Help:      "Current connections on this node (including unauthenticated).",
	}, func() float64 { return float64(connCount()) })

	OnlineUsers = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "online_users",
		Help:      "Current online users on this node.",
	}, func() float64 { return float64(userCount()) })
}

var (
	// HandshakeTotal WebSocket 升级计数 ( result: success | failure )。
	HandshakeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "handshake_total",
		Help:      "WebSocket upgrade attempts.",
	}, []string{"result"})

	// AuthDuration 首帧 AUTH 鉴权耗时。
	AuthDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "auth_duration_seconds",
		Help:      "AUTH frame processing duration (verify + bind + register).",
		Buckets:   prometheus.DefBuckets,
	})

	// MsgInTotal 入站帧计数 ( 按 command_type )。
	MsgInTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "msg_in_total",
		Help:      "Inbound frames by command type.",
	}, []string{"command_type"})

	// MsgOutTotal 出站帧计数 ( 按 command_type )。
	MsgOutTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "msg_out_total",
		Help:      "Outbound frames by command type.",
	}, []string{"command_type"})

	// NotifyDropTotal NOTIFY 帧因入队超时被丢弃的计数 ( 通知可丢，仅影响实时性 )。
	NotifyDropTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "notify_drop_total",
		Help:      "NOTIFY frames dropped due to enqueue timeout.",
	})

	// SendEnqueueTimeoutTotal 出站帧入队超时计数。
	SendEnqueueTimeoutTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "send_enqueue_timeout_total",
		Help:      "Outbound frames failed to enqueue within timeout.",
	})
)

var (
	// RedisDedupDuration 消息去重 ( SETNX ) 耗时。
	RedisDedupDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "redis_dedup_duration_seconds",
		Help:      "Redis SETNX dedup latency.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 10), // 0.5ms ~ 256ms
	})

	// KafkaProduceDuration 上行消息 Kafka 写入耗时 ( acks=all )。
	KafkaProduceDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "kafka_produce_duration_seconds",
		Help:      "Kafka produce latency for upstream messages.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms ~ 2s
	})

	// SyncCallDuration 后端 SyncService 调用耗时。
	SyncCallDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "sync_call_duration_seconds",
		Help:      "Backend SyncService call latency.",
		Buckets:   prometheus.ExponentialBuckets(0.005, 2, 10), // 5ms ~ 2.5s
	})

	// RegistryOpDuration 连接路由注册表操作耗时 ( op: register | unregister | heartbeat | find )。
	RegistryOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "registry_op_duration_seconds",
		Help:      "Connection registry operation latency.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 10),
	}, []string{"op"})

	// ConsumerHandleDuration 通知消息消费处理耗时 ( 含查找连接与逐连接入队 )。
	ConsumerHandleDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "consumer_handle_duration_seconds",
		Help:      "Notify consumer handle latency ( find conns + enqueue ).",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13), // 0.5ms ~ 4s
	})

	// buildInfo 版本信息 ( 值恒为 1, label 携带版本元数据 )。
	buildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "build_info",
		Help:      "Build metadata; the value is always 1.",
	}, []string{"version", "commit", "date"})
)

func init() {
	buildInfo.WithLabelValues(buildinfo.Version, buildinfo.Commit, buildinfo.Date).Set(1)
}
