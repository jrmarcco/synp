package message

import (
	"context"
	"errors"
	"fmt"

	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	cc "github.com/jrmarcco/synp/internal/pkg/codec"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"google.golang.org/protobuf/proto"
)

var ErrMarshalMessage = errors.New("failed to marshal message")

// MarshalBody 统一序列化 *_ACK / NOTIFY 帧 body。
// 零值字段照常输出 ( 见 codec.JSONMarshalOptions )。
func MarshalBody(msg proto.Message) ([]byte, error) {
	return cc.JSONMarshalOptions.Marshal(msg)
}

// PushFunc 将结构化消息编码后经连接发送。
// ctx 控制发送入队等待 ( 慢客户端保护 )。
type PushFunc func(ctx context.Context, conn synp.Conn, msg *messagev1.Message) error

// DefaultPushFunc 创建默认推送消息到前端 ( 业务客户端 ) 的函数的默认实现，用于将结构化消息通过连接发送。
// 该函数将消息编码后通过 Conn.Send 发送。
func DefaultPushFunc(codec cc.Codec) PushFunc {
	return func(ctx context.Context, conn synp.Conn, msg *messagev1.Message) error {
		payload, err := codec.Marshal(msg)
		if err != nil {
			logging.For("message").Error(
				"[message] failed to marshal message",
				"codec_name", codec.Name(),
				"message", msg.String(),
				"error", err,
			)
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		metrics.MsgOutTotal.WithLabelValues(msg.GetCommandType().String()).Inc()

		if err = conn.Send(ctx, payload); err != nil {
			logging.For("message").Error(
				"[message] failed to send message",
				"conn_id", conn.ID(),
				"error", err,
			)
			return fmt.Errorf("failed to send message: %w", err)
		}
		return nil
	}
}
