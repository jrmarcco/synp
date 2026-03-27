package service

import (
	"context"
	"errors"

	"github.com/jrmarcco/synp"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/retransmit"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"go.uber.org/zap"
)

var ErrUserNotOnline = errors.New("user not online")

type DownstreamMsg struct {
	MessageID string

	BizID      uint64
	ReceiverID uint64

	Body []byte
}

type PushService interface {
	Push(ctx context.Context, msg *DownstreamMsg, needRetry bool) error
}

var _ PushService = (*DefaultPushService)(nil)

type DefaultPushService struct {
	pushFunc message.PushFunc

	connMamanger      synp.ConnManager
	retransmitManager *retransmit.Manager

	logger *zap.Logger
}

func NewDefaultPushService() *DefaultPushService {
	return &DefaultPushService{}
}

func (s *DefaultPushService) Push(_ context.Context, msg *DownstreamMsg, needRetry bool) error {
	user := session.User{
		BID: msg.BizID,
		UID: msg.ReceiverID,
	}

	// 查找用户所有在线连接 ( 支持多设备 )。
	conns, ok := s.connMamanger.FindUserConn(user)
	if !ok {
		s.logger.Warn(
			"[push-service] user not online",
			zap.Uint64("bid", msg.BizID),
			zap.Uint64("uid", msg.ReceiverID),
			zap.String("message_id", msg.MessageID),
		)
		return ErrUserNotOnline
	}

	downstreamMsg := &messagev1.Message{
		Cmd:       commonv1.CommandType_COMMAND_TYPE_DOWNSTREAM,
		MessageId: msg.MessageID,
		Body:      msg.Body,
	}

	var pushErr error
	for _, conn := range conns {
		if err := s.pushFunc(conn, downstreamMsg); err != nil {
			s.logger.Error(
				"[push-service] failed to push message",
				zap.String("conn_id", conn.ID()),
				zap.String("message_id", msg.MessageID),
				zap.Error(err),
			)
			pushErr = err
			continue
		}
		conn.UpdateActivityTime()
	}

	if needRetry && pushErr != nil {
		s.retransmitManager.Start(conns, downstreamMsg)
	}

	return pushErr
}
