package api

import (
	"context"

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

func (s *GrpcPushServer) Push(_ context.Context, _ *messagev1.PushRequest) (*messagev1.PushResponse, error) {
	// TODO: not implemented
	panic("not implemented")
}

func (s *GrpcPushServer) BatchPush(_ context.Context, _ *messagev1.BatchPushRequest) (*messagev1.BatchPushResponse, error) {
	// TODO: not implemented
	panic("not implemented")
}
