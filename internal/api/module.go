package api

import (
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	nodev1 "github.com/jrmarcco/synp-api/api/go/node/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var GrpcApiFxModule = fx.Module(
	"grpc-api",
	fx.Provide(
		NewGrpcPushServer,
		NewGrpcLocateServer,
		newGrpcServer,
	),
)

// newGrpcServer 创建 gRPC 服务器。
func newGrpcServer(pushServer *GrpcPushServer, locateServer *GrpcLocateServer) *grpc.Server {
	grpcServer := grpc.NewServer()

	// 注册 message service。
	messagev1.RegisterPushServiceServer(grpcServer, pushServer)
	// 注册 locate service。
	nodev1.RegisterLocateServiceServer(grpcServer, locateServer)
	return grpcServer
}
