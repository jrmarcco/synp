package ws

import (
	"time"

	"github.com/jrmarcco/jit/xbean/option"
	"github.com/jrmarcco/synp/internal/pkg/limiter"
)

func ServerWithConnLimiter(connLimiter *limiter.TokenLimiter) option.Opt[Server] {
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
