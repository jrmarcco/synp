package service

import (
	"context"
	"errors"

	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message/notify"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"go.uber.org/zap"
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

	logger *zap.Logger
}

func NewDefaultNotifyService(
	nMsgHandler notify.NMsgHandler,
	connManager synp.ConnManager,
	logger *zap.Logger,
) *DefaultNotifyService {
	return &DefaultNotifyService{
		nMsgHandler: nMsgHandler,
		connManager: connManager,
		logger:      logger,
	}
}

func (s *DefaultNotifyService) Notify(_ context.Context, msg *messagev1.NotifyMessage) error {
	conns, ok := s.connManager.FindUserConn(session.User{
		BID: msg.GetBusinessId(),
		UID: msg.GetReceiverId(),
	})
	if !ok {
		s.logger.Debug(
			"[notify-service] user not online on this node",
			zap.Uint64("business_id", msg.GetBusinessId()),
			zap.Uint64("receiver_id", msg.GetReceiverId()),
			zap.String("message_id", msg.GetMessageId()),
		)
		return ErrUserNotOnline
	}

	if err := s.nMsgHandler.Handle(conns, msg); err != nil {
		s.logger.Error(
			"[notify-service] failed to notify user",
			zap.Uint64("business_id", msg.GetBusinessId()),
			zap.Uint64("receiver_id", msg.GetReceiverId()),
			zap.String("message_id", msg.GetMessageId()),
			zap.Error(err),
		)
		return err
	}

	return nil
}
