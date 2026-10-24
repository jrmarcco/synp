package api

import (
	"context"
	"errors"

	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/service"
)

var _ messagev1.NotifyServiceServer = (*GrpcNotifyServer)(nil)

// GrpcNotifyServer 是 NotifyService 的 gRPC 服务端实现。
// NotifyService 由网关提供给后端 ( 业务服务端 ) 调用，
// 是 Kafka 通知链路之外的即时通知通道。
type GrpcNotifyServer struct {
	notifyService service.NotifyService
}

func NewGrpcNotifyServer(notifyService service.NotifyService) *GrpcNotifyServer {
	return &GrpcNotifyServer{
		notifyService: notifyService,
	}
}

// Notify 通知单个用户，网关会向其所有在线连接下发 NOTIFY 帧。
// 用户不在线时返回 USER_OFFLINE。通知失败无需重试。
func (s *GrpcNotifyServer) Notify(ctx context.Context, req *messagev1.NotifyRequest) (*messagev1.NotifyResponse, error) {
	msg := req.GetMessage()
	if msg == nil {
		return &messagev1.NotifyResponse{
			Status:       messagev1.NotifyStatus_NOTIFY_STATUS_INTERNAL_ERROR,
			ErrorMessage: "message is required",
		}, nil
	}

	if err := s.notifyService.Notify(ctx, msg); err != nil {
		if errors.Is(err, service.ErrUserNotOnline) {
			return &messagev1.NotifyResponse{
				Status: messagev1.NotifyStatus_NOTIFY_STATUS_USER_OFFLINE,
			}, nil
		}

		return &messagev1.NotifyResponse{
			Status:       messagev1.NotifyStatus_NOTIFY_STATUS_INTERNAL_ERROR,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &messagev1.NotifyResponse{
		Status: messagev1.NotifyStatus_NOTIFY_STATUS_SUCCESS,
	}, nil
}

// BatchNotify 批量通知，results 与 messages 下标一一对应。
func (s *GrpcNotifyServer) BatchNotify(ctx context.Context, req *messagev1.BatchNotifyRequest) (*messagev1.BatchNotifyResponse, error) {
	messages := req.GetMessages()

	results := make([]*messagev1.NotifyResponse, 0, len(messages))
	for _, msg := range messages {
		resp, err := s.Notify(ctx, &messagev1.NotifyRequest{Message: msg})
		if err != nil {
			resp = &messagev1.NotifyResponse{
				Status:       messagev1.NotifyStatus_NOTIFY_STATUS_INTERNAL_ERROR,
				ErrorMessage: err.Error(),
			}
		}
		results = append(results, resp)
	}

	return &messagev1.BatchNotifyResponse{
		Results: results,
	}, nil
}
