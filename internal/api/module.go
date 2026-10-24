package api

import (
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var GrpcApiFxModule = fx.Module(
	"grpc-api",
	fx.Provide(
		NewGrpcNotifyServer,
		newGrpcServer,
	),
)

// newGrpcServer 创建 gRPC 服务器。
func newGrpcServer(notifyServer *GrpcNotifyServer) *grpc.Server {
	grpcServer := grpc.NewServer()

	// 注册 notify service。
	messagev1.RegisterNotifyServiceServer(grpcServer, notifyServer)
	return grpcServer
}
