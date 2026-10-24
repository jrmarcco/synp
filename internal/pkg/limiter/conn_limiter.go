package limiter

import (
	"log/slog"
	"sync/atomic"
)

// defaultMaxConnections 为未配置时的最大并发连接数。
const defaultMaxConnections = int64(50000)

// ConnLimiter 基于最大连接数的准入限流器。
//
// 相比渐增令牌桶模型，显式 max_connections 语义直接、容量可预测:
// Acquire 原子递增，达到上限即拒绝，连接关闭时 Release 归还配额。
type ConnLimiter struct {
	max  int64
	curr atomic.Int64
}

// NewConnLimiter 创建连接准入限流器。
// maxConns <= 0 时使用默认容量。
func NewConnLimiter(maxConns int64) *ConnLimiter {
	if maxConns <= 0 {
		maxConns = defaultMaxConnections
	}

	limiter := &ConnLimiter{max: maxConns}
	slog.Debug(
		"[conn-limiter] initialized",
		"max_connections", maxConns,
	)
	return limiter
}

// Acquire 尝试占用一个连接配额，达到上限返回 false。
// 非阻塞操作。
func (l *ConnLimiter) Acquire() bool {
	for {
		curr := l.curr.Load()
		if curr >= l.max {
			return false
		}
		if l.curr.CompareAndSwap(curr, curr+1) {
			return true
		}
	}
}

// Release 归还一个连接配额。
func (l *ConnLimiter) Release() {
	for {
		curr := l.curr.Load()
		if curr <= 0 {
			return
		}
		if l.curr.CompareAndSwap(curr, curr-1) {
			return
		}
	}
}

// Curr 返回当前占用的连接配额数。
func (l *ConnLimiter) Curr() int64 {
	return l.curr.Load()
}

// Max 返回最大连接容量。
func (l *ConnLimiter) Max() int64 {
	return l.max
}
