package xprocessor

import (
	"context"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
)

// MsgHandler 是处理消息的函数类型。
type MsgHandler func(ctx context.Context, msg *xmq.Message) error

// Processor 是消息处理器的通用接口。
type Processor interface {
	// Start 启动处理器。
	// ctx 用于控制启动过程的超时和取消。
	Start(ctx context.Context) error
	// Stop 停止处理器。
	// ctx 用于控制停止过程的超时，实现优雅关闭。
	Stop(ctx context.Context) error
}
