package provider

import (
	"context"
	"fmt"

	"github.com/jrmarcco/synp/internal/pkg/resolver"
	"github.com/jrmarcco/xgrpc/register"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SyncClientFxModule 提供到后端 SyncService 的 gRPC 连接。
// 后端服务地址经共享 registry 服务发现解析 ( synp:///<service_name> )。
var SyncClientFxModule = fx.Module(
	"sync-client",
	fx.Provide(newSyncServiceConn),
)

// newSyncServiceConn 创建到后端 SyncService 的 gRPC 连接。
func newSyncServiceConn(registry register.Registry, lifecycle fx.Lifecycle, logger *zap.Logger) (*grpc.ClientConn, error) {
	type config struct {
		ServiceName string `mapstructure:"service_name"`
		Insecure    bool   `mapstructure:"insecure"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.sync_service", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sync service config: %w", err)
	}

	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("synp.sync_service.service_name is required")
	}

	resolverBuilder := resolver.NewRegistryResolverBuilder(registry, logger)

	dialOpts := []grpc.DialOption{
		grpc.WithResolvers(resolverBuilder),
		grpc.WithNoProxy(),
	}
	if cfg.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// 异步建连：后端未就绪时 SYNC 帧调用会失败并以 *_ACK 错误应答，
	// 不阻塞网关启动。
	cc, err := grpc.NewClient(
		fmt.Sprintf("%s:///%s", resolver.Scheme, cfg.ServiceName),
		dialOpts...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync service client conn: %w", err)
	}

	lifecycle.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			return cc.Close()
		},
	})

	logger.Info("[sync-client] sync service client conn created", zap.String("service_name", cfg.ServiceName))
	return cc, nil
}
