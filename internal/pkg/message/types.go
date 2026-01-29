package message

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/codec"
)

var ErrMarshalMessage = errors.New("failed to marshal message")

type PushFunc func(conn synp.Conn, msg *messagev1.Message) error

// DefaultPushFunc 创建默认推送消息到前端 ( 业务客户端 ) 的函数的默认实现，用于将结构化消息通过连接发送。
// 该函数将消息编码后通过 Conn.Send 发送，适用于 retransmit.Manager 的 taskFunc 参数。
//
// 参数：
//   - codec: 消息编解码器
//
// 返回：
//   - retransmit.TaskFunc: 可用于发送消息和重传的函数
func DefaultPushFunc(codec codec.Codec) PushFunc {
	return func(conn synp.Conn, msg *messagev1.Message) error {
		payload, err := codec.Marshal(msg)
		if err != nil {
			slog.Error(
				"[message] failed to marshal message",
				"codec_name", codec.Name(),
				"message", msg.String(),
				"error", err,
			)
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		if err = conn.Send(payload); err != nil {
			slog.Error(
				"[message] failed to send message",
				"conn_id", conn.ID(),
				"error", err,
			)
			return fmt.Errorf("failed to send message: %w", err)
		}
		return nil
	}
}
