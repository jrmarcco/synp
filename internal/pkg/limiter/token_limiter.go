package limiter

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
)

type TokenLimiterConfig struct {
	InitCapacity     int64
	MaxCapacity      int64
	IncreaseStep     int64
	IncreaseInterval time.Duration
}

func DefaultConfig() TokenLimiterConfig {
	// 默认配置。
	//
	// 从初始容量扩容到最大容量需要：
	//  |-- 增量：50000 - 2000 = 48000
	//  |--次数：48000 ÷ 500 = 96次
	//  |--时间：96 × 2秒 = 192秒（约3.2分钟）
	//
	// 推荐设置：
	//  |-- 小规模场景：InitCapacity: 500, MaxCapacity: 10000
	//  |-- 大规模场景：InitCapacity: 5000, MaxCapacity: 100000
	const (
		defaultInitCapacity     = int64(2000)
		defaultMaxCapacity      = int64(50000)
		defaultIncreaseStep     = int64(500)
		defaultIncreaseInterval = 2 * time.Second
	)
	return TokenLimiterConfig{
		InitCapacity:     defaultInitCapacity,
		MaxCapacity:      defaultMaxCapacity,
		IncreaseStep:     defaultIncreaseStep,
		IncreaseInterval: defaultIncreaseInterval,
	}
}

func NewConfig(initCapacity, maxCapacity, increaseStep int64, increaseInterval time.Duration) (TokenLimiterConfig, error) {
	var cfg TokenLimiterConfig

	if initCapacity <= 0 {
		return cfg, errors.New("init capacity must be greater than 0")
	}

	if maxCapacity <= 0 {
		return cfg, errors.New("max capacity must be greater than 0")
	}

	if initCapacity > maxCapacity {
		return cfg, errors.New("init capacity must be less than max capacity")
	}

	if increaseStep <= 0 {
		return cfg, errors.New("increase step must be greater than 0")
	}

	if increaseInterval <= 0 {
		return cfg, errors.New("increase interval must be greater than 0")
	}

	cfg = TokenLimiterConfig{
		InitCapacity:     initCapacity,
		MaxCapacity:      maxCapacity,
		IncreaseStep:     increaseStep,
		IncreaseInterval: increaseInterval,
	}

	return cfg, nil
}

// TokenLimiter 令牌桶算法限流器。
type TokenLimiter struct {
	cfg TokenLimiterConfig

	tokens       chan struct{}
	currCapacity atomic.Int64

	ctx        context.Context
	cancelFunc context.CancelFunc
}

// Start 开启一个 goroutine，逐步增加桶的容量。
func (l *TokenLimiter) Start(ctx context.Context) {
	ticker := time.NewTicker(l.cfg.IncreaseInterval)
	defer ticker.Stop()

	slog.Info("[token-limiter] start capacity increasing goroutine")

	for {
		select {
		case <-ctx.Done():
			slog.Info("[token-limiter] outter context canceled, stop capacity increasing goroutine")
			return

		case <-l.ctx.Done():
			slog.Info("[token-limiter] inner context canceled, stop capacity increasing goroutine")
			return

		case <-ticker.C:
			curr := l.currCapacity.Load()
			if curr >= l.cfg.MaxCapacity {
				slog.Info("[token-limiter] capacity reached max, stop capacity increasing goroutine")
				return
			}

			newCap := min(curr+l.cfg.IncreaseStep, l.cfg.MaxCapacity)

			for range newCap - curr {
				l.tokens <- struct{}{}
			}
			l.currCapacity.Store(newCap)

			slog.Info(
				"[token-limiter] capacity increased",
				"from", curr,
				"to", newCap,
				"increase_step", l.cfg.IncreaseStep,
			)
		}
	}
}

// Acquire 尝试获取令牌，成功获取返回 true。
// 注：
//
//	这是一个非阻塞操作，如果当前没有可用令牌会立即返回 false。
func (l *TokenLimiter) Acquire() bool {
	select {
	case <-l.tokens:
		return true
	default:
		return false
	}
}

// Release 释放（归还）令牌，同样是一个非阻塞操作。
func (l *TokenLimiter) Release() bool {
	select {
	case l.tokens <- struct{}{}:
		return true
	default:
		// 理论上不会执行到这里。
		// 执行到这里通常意味着 Release 的调用次数超过 Acquire 的调用次数。
		slog.Warn("[token-limiter] failed to release token, bucket is full")
		return false
	}
}

func (l *TokenLimiter) Close() error {
	l.cancelFunc()
	slog.Info("[token-limiter] token limiter closed")
	return nil
}

func (l *TokenLimiter) Cap() int64 {
	return l.currCapacity.Load()
}

func NewTokenLimiter(cfg TokenLimiterConfig) *TokenLimiter {
	ctx, cancel := context.WithCancel(context.Background())
	tl := &TokenLimiter{
		cfg: cfg,

		tokens: make(chan struct{}, cfg.InitCapacity),

		ctx:        ctx,
		cancelFunc: cancel,
	}

	// 初始令牌。
	for i := int64(0); i < cfg.InitCapacity; i++ {
		tl.tokens <- struct{}{}
	}
	tl.currCapacity.Store(cfg.InitCapacity)

	slog.Info(
		"[token-limiter] successfully initialized",
		"init_capacity", cfg.InitCapacity,
		"max_capacity", cfg.MaxCapacity,
	)

	return tl
}
