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

		// 初始化 go cache。
		provider.GoCacheFxModule,

		// 初始化 kafka。
		provider.KafkaFxModule,
		provider.KafkaConsumerFxModule,
		provider.KafkaProducerFxModule,

		// 初始化 token validator。
		provider.ValidatorFxModule,

		// 初始化 codec。
		provider.CodecFxModule,

		// 初始化 message push func。
		provider.MessagePushFuncFxModule,

		// 初始化 retransmit manager。
		provider.RetransmitFxModule,

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
	configPath := pflag.String("config-dir", "etc", "config directory")
	pflag.Parse()

	viper.AddConfigPath(*configPath)
	viper.SetConfigType("yaml")

	// 读取基础配置。
	viper.SetConfigName("base")
	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read base config: %w", err)
	}

	// 合并其他配置文件。
	subConfigNames := []string{"etcd", "redis", "kafka"}
	for _, subConfigName := range subConfigNames {
		viper.SetConfigName(subConfigName)
		if err := viper.MergeInConfig(); err != nil {
			return fmt.Errorf("failed to merge %s config: %w", subConfigName, err)
		}
	}

	return nil
}
