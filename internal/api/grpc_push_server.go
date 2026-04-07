package api

import (
	"context"
	"errors"

	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/service"
)

var _ messagev1.PushServiceServer = (*GrpcPushServer)(nil)

type GrpcPushServer struct {
	pushService service.PushService
}

func NewGrpcPushServer(pushService service.PushService) *GrpcPushServer {
	return &GrpcPushServer{
		pushService: pushService,
	}
}

func (s *GrpcPushServer) Push(ctx context.Context, req *messagev1.PushRequest) (*messagev1.PushResponse, error) {
	msg := req.GetMessage()
	if msg == nil {
		return &messagev1.PushResponse{
			Status:       messagev1.PushStatus_PUSH_STATUS_INVALID_MESSAGE,
			ErrorMessage: "message is required",
		}, nil
	}

	if msg.GetMessageId() == "" || msg.GetReceiverId() == 0 {
		return &messagev1.PushResponse{
			Status:       messagev1.PushStatus_PUSH_STATUS_INVALID_MESSAGE,
			ErrorMessage: "message id and receiver id are required",
		}, nil
	}

	if len(msg.GetBody()) == 0 {
		return &messagev1.PushResponse{
			Status:       messagev1.PushStatus_PUSH_STATUS_INVALID_MESSAGE,
			ErrorMessage: "message body is required",
		}, nil
	}

	downstreamMsg := &service.DownstreamMsg{
		MessageID:  msg.GetMessageId(),
		ReceiverID: msg.GetReceiverId(),
		Body:       msg.GetBody(),
	}
	err := s.pushService.Push(ctx, downstreamMsg, req.ShouldRetry)
	if err != nil {
		// 用户离线，返回用户离线状态。
		if errors.Is(err, service.ErrUserNotOnline) {
			return &messagev1.PushResponse{
				Status: messagev1.PushStatus_PUSH_STATUS_USER_OFFLINE,
			}, nil
		}

		// 其他错误，返回内部错误状态。
		return &messagev1.PushResponse{
			Status:       messagev1.PushStatus_PUSH_STATUS_INTERNAL_ERROR,
			ErrorMessage: err.Error(),
		}, nil
	}

	// 推送成功。
	return &messagev1.PushResponse{
		Status: messagev1.PushStatus_PUSH_STATUS_SUCCESS,
	}, nil
}

func (s *GrpcPushServer) BatchPush(_ context.Context, _ *messagev1.BatchPushRequest) (*messagev1.BatchPushResponse, error) {
	// TODO: not implemented
	panic("not implemented")
}
