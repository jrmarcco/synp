package api

import (
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var GrpcApiFxModule = fx.Module(
	"grpc-api",
	fx.Provide(
		NewGrpcPushServer,
		newGrpcServer,
	),
)

func newGrpcServer(pushServer *GrpcPushServer) *grpc.Server {
	grpcServer := grpc.NewServer()

	// 注册 message service。
	messagev1.RegisterPushServiceServer(grpcServer, pushServer)

	return grpcServer
}
