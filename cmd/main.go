package main

import (
	"fmt"

	"github.com/jrmarcco/synp/internal/api"
	"github.com/jrmarcco/synp/internal/app"
	"github.com/jrmarcco/synp/internal/pkg/provider"
	"github.com/jrmarcco/synp/internal/service"
	"github.com/jrmarcco/synp/internal/ws"
	"github.com/jrmarcco/synp/internal/ws/conn"
	"github.com/jrmarcco/synp/internal/ws/conn/lifecycle"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
)

func main() {
	if err := loadConfig(); err != nil {
		panic(err)
	}

	fx.New(
		fx.WithLogger(func(logger *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: logger}
		}),

		// 初始化 zap.Logger。
		provider.ZapLoggerFxModule,

		// 初始化 etcd。
		provider.EtcdFxModule,

		// 初始化 redis.Cmdable。
		provider.RedisFxModule,

		// 初始化 kafka。
		provider.KafkaFxModule,
		provider.KafkaConsumerFxModule,
		provider.KafkaProducerFxModule,

		// 初始化 gRPC 服务注册器 ( 自身注册与 SyncService 服务发现共享 )。
		provider.GrpcRegistryFxModule,

		// 初始化 token validator ( RS256 + JWKS )。
		provider.ValidatorFxModule,

		// 初始化 session builder。
		provider.SessionBuilderFxModule,

		// 初始化 codec。
		provider.CodecFxModule,

		// 初始化 message push func。
		provider.MessagePushFuncFxModule,

		// 初始化到后端 SyncService 的 gRPC 连接。
		provider.SyncClientFxModule,

		// 初始化 message handler。
		provider.MessageHandlerFxModule,

		// 初始化 conn lifecycle handler 。
		lifecycle.ConnLifecycleFxModule,

		// 初始化 conn。
		conn.ConnFxModule,

		// 初始化 upgrader。
		ws.WsFxModule,

		// 初始化 service。
		service.ServiceFxModule,

		// 初始化 grpc api。
		api.GrpcApiFxModule,

		// 初始化 app。
		app.AppFxModule,
	).Run()
}

func loadConfig() error {
	configFile := pflag.String("config", "etc/config.yaml", "config file (default: etc/config.yaml)")
	pflag.Parse()

	viper.SetConfigFile(*configFile)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	return nil
}
