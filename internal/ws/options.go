package ws

import (
	"time"

	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/synp/internal/pkg/limiter"
)

// ServerWithConnLimiter 设置连接准入限流器。
func ServerWithConnLimiter(connLimiter *limiter.ConnLimiter) option.Opt[Server] {
	return func(s *Server) {
		s.connLimiter = connLimiter
	}
}

// ServerWithAuthTimeout 设置连接建立后等待首帧 AUTH 的超时时间。
func ServerWithAuthTimeout(authTimeout time.Duration) option.Opt[Server] {
	return func(s *Server) {
		if authTimeout > 0 {
			s.authTimeout = authTimeout
		}
	}
}

// ServerWithIdleScan 设置空闲连接扫描参数。
func ServerWithIdleScan(scanInterval, idleTimeout time.Duration) option.Opt[Server] {
	return func(s *Server) {
		if scanInterval > 0 {
			s.idleScanInterval = scanInterval
		}
		if idleTimeout > 0 {
			s.idleTimeout = idleTimeout
		}
	}
}

// ServerWithDrainTimeout 设置优雅关闭时等待存量连接排空的超时时间。
func ServerWithDrainTimeout(drainTimeout time.Duration) option.Opt[Server] {
	return func(s *Server) {
		if drainTimeout > 0 {
			s.drainTimeout = drainTimeout
		}
	}
}
