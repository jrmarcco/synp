package service

import (
	"context"

	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message/upstream"
	"google.golang.org/grpc"
)

var _ upstream.SyncCaller = (*GrpcSyncCaller)(nil)

// GrpcSyncCaller 是后端 SyncService ( proto/message/v1/sync.proto ) 的 gRPC 调用实现。
// 后端服务地址经服务发现解析 ( synp:///<service_name> )。
type GrpcSyncCaller struct {
	client messagev1.SyncServiceClient
}

func NewGrpcSyncCaller(cc *grpc.ClientConn) *GrpcSyncCaller {
	return &GrpcSyncCaller{
		client: messagev1.NewSyncServiceClient(cc),
	}
}

func (c *GrpcSyncCaller) Sync(ctx context.Context, req *messagev1.SyncRequest) (*messagev1.SyncResponse, error) {
	return c.client.Sync(ctx, req)
}

func (c *GrpcSyncCaller) ConversationSync(ctx context.Context, req *messagev1.ConversationSyncRequest) (*messagev1.ConversationSyncResponse, error) {
	return c.client.ConversationSync(ctx, req)
}
