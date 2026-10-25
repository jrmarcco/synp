package service

import (
	"context"
	"errors"

	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/message/notify"
	"github.com/jrmarcco/synp/internal/pkg/session"
)

var ErrUserNotOnline = errors.New("user not online")

// NotifyService 向本节点的在线连接下发 NOTIFY 帧。
type NotifyService interface {
	// Notify 通知单个用户，网关会向其所有在线连接下发 NOTIFY 帧。
	Notify(ctx context.Context, msg *messagev1.NotifyMessage) error
}

var _ NotifyService = (*DefaultNotifyService)(nil)

// DefaultNotifyService 是 NotifyService 的默认实现。
//
// 注：初期实现只查找本节点连接。广播语义由 Kafka 通知 topic 承担——
// 每个网关节点独立消费全量后按本地连接过滤下发；
// NotifyService 是 Kafka 之外的即时通道，用户不在本节点在线时返回 USER_OFFLINE，
// 通知失败无需重试 ( 只影响实时性 )。
type DefaultNotifyService struct {
	nMsgHandler notify.NMsgHandler

	connManager synp.ConnManager
}

func NewDefaultNotifyService(
	nMsgHandler notify.NMsgHandler,
	connManager synp.ConnManager,
) *DefaultNotifyService {
	return &DefaultNotifyService{
		nMsgHandler: nMsgHandler,
		connManager: connManager,
	}
}

func (s *DefaultNotifyService) Notify(ctx context.Context, msg *messagev1.NotifyMessage) error {
	conns, ok := s.connManager.FindUserConn(session.User{
		BID: msg.GetBusinessId(),
		UID: msg.GetReceiverId(),
	})
	if !ok {
		logging.For("notify-service").Debug(
			"user not online on this node",
			"business_id", msg.GetBusinessId(),
			"receiver_id", msg.GetReceiverId(),
			"message_id", msg.GetMessageId(),
		)
		return ErrUserNotOnline
	}

	if err := s.nMsgHandler.Handle(ctx, conns, msg); err != nil {
		logging.For("notify-service").Error(
			"failed to notify user",
			"business_id", msg.GetBusinessId(),
			"receiver_id", msg.GetReceiverId(),
			"message_id", msg.GetMessageId(),
			"error", err,
		)
		return err
	}

	return nil
}
