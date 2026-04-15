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
	MessageID  string
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

func NewDefaultPushService(
	pushFunc message.PushFunc,
	connMamanger synp.ConnManager,
	retransmitManager *retransmit.Manager,
	logger *zap.Logger,
) *DefaultPushService {
	return &DefaultPushService{
		pushFunc:          pushFunc,
		connMamanger:      connMamanger,
		retransmitManager: retransmitManager,
		logger:            logger,
	}
}

func (s *DefaultPushService) Push(_ context.Context, msg *DownstreamMsg, needRetry bool) error {
	user := session.User{
		UID: msg.ReceiverID,
	}

	// 查找用户所有在线连接 ( 支持多设备 )。
	conns, ok := s.connMamanger.FindUserConn(user)
	if !ok {
		s.logger.Warn(
			"[push-service] user not online",
			zap.Uint64("uid", msg.ReceiverID),
			zap.String("message_id", msg.MessageID),
		)
		return ErrUserNotOnline
	}

	downstreamMsg := &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_DOWNSTREAM,
		MessageId:   msg.MessageID,
		Body:        msg.Body,
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

			// 启用重传任务。
			s.logger.Debug(
				"[push-service] failed to push message, starting retransmit task",
				zap.String("conn_id", conn.ID()),
				zap.String("message_id", msg.MessageID),
				zap.Error(err),
			)

			s.retransmitManager.Start(conns, downstreamMsg)
			continue
		}

		// 成功发送消息到前端，更新连接活跃时间。
		conn.UpdateActivityTime()

		// 启用重传任务。
		// 推送成功也要启用重传任务，因为前端可能没有收到消息。
		// 当收到前端返回的 ack 消息后会停止重传任务。
		s.logger.Debug(
			"[push-service] successfully pushed message, starting retransmit task",
			zap.String("conn_id", conn.ID()),
			zap.String("message_id", msg.MessageID),
		)
		s.retransmitManager.Start(conns, downstreamMsg)
	}

	if needRetry && pushErr != nil {
		s.retransmitManager.Start(conns, downstreamMsg)
	}

	return pushErr
}
