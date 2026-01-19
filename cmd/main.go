package main

import (
	"fmt"

	"github.com/jrmarcco/synp/internal/app"
	"github.com/jrmarcco/synp/internal/pkg/providers"
	"github.com/jrmarcco/synp/internal/ws"
	"github.com/jrmarcco/synp/internal/ws/conn"
	"github.com/jrmarcco/synp/internal/ws/conn/lifecycle"
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
		providers.ZapLoggerFxModule,

		// 初始化 redis.Cmdable。
		providers.RedisFxModule,

		// 初始化 kafka。
		providers.KafkaFxModule,
		providers.KafkaConsumerFxModule,
		providers.KafkaProducerFxModule,

		// 初始化 token validator。
		providers.ValidatorFxModule,

		// 初始化 codec。
		providers.CodecFxModule,

		// 初始化 message push func。
		providers.MessagePushFuncFxModule,

		// 初始化 retransmit manager。
		providers.RetransmitFxModule,

		// 初始化 message handler。
		providers.MessageHandlerFxModule,

		// 初始化 upgrader。
		ws.UpgraderFxModule,

		// 初始化 conn lifecycle handler 。
		lifecycle.ConnLcHandlerFxModule,

		// 初始化 conn manager。
		conn.ConnManagerFxModule,

		// 初始化 app。
		app.AppFxModule,
	).Run()
}

func loadConfig() error {
	viper.AddConfigPath("config")
	viper.SetConfigType("yaml")

	// 读取基础配置。
	viper.SetConfigName("base")
	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read base config: %w", err)
	}

	subConfigNames := []string{"redis", "kafka"}
	for _, subConfigName := range subConfigNames {
		viper.SetConfigName(subConfigName)
		if err := viper.MergeInConfig(); err != nil {
			return fmt.Errorf("failed to merge %s config: %w", subConfigName, err)
		}
	}

	return nil
}
