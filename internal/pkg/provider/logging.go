package provider

import (
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/spf13/viper"
)

// SetupLogging 从应用配置装配全局日志 ( slog 唯一管道 )，返回 cleanup。
//
// 必须在 fx.New 之前调用：fx 自身的事件日志 ( fxevent.SlogLogger )
// 在装配最早期求值，若日志装配晚于 fx 启动，早期事件会落到未装配的默认 handler。
//
// 配置 ( synp.log.* ):
//   - mode:  dev ( 默认，单行紧凑格式 ) | prod ( JSON，面向采集 )
//   - level: debug | info ( 默认 ) | warn | error
//   - color: auto ( 默认 ) | always | never ( 仅 dev 生效 )
//   - file:  可选落盘段 ( 路径 + 轮转参数 ):
//     path:         日志文件路径 ( 空串不落盘 )
//     max_size_mb:  单文件大小上限，超过切割 ( 默认 100 )
//     max_backups:  保留旧文件份数 ( 0 保留全部 )
//     max_age_days: 旧文件保留天数 ( 0 不限 )
//     compress:     旧文件 gzip 压缩 ( 默认 false )
func SetupLogging() func() {
	type fileConfig struct {
		Path       string `mapstructure:"path"`
		MaxSizeMB  int    `mapstructure:"max_size_mb"`
		MaxBackups int    `mapstructure:"max_backups"`
		MaxAgeDays int    `mapstructure:"max_age_days"`
		Compress   bool   `mapstructure:"compress"`
	}
	type config struct {
		Mode  string     `mapstructure:"mode"`
		Level string     `mapstructure:"level"`
		Color string     `mapstructure:"color"`
		File  fileConfig `mapstructure:"file"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.log", &cfg); err != nil {
		// 配置解析失败时以默认参数装配，不阻断启动。
		cfg = config{}
	}

	return logging.Setup(logging.Options{
		Mode:  logging.Mode(cfg.Mode),
		Level: logging.ParseLevel(cfg.Level),
		Color: cfg.Color,

		File:       cfg.File.Path,
		MaxSizeMB:  cfg.File.MaxSizeMB,
		MaxBackups: cfg.File.MaxBackups,
		MaxAgeDays: cfg.File.MaxAgeDays,
		Compress:   cfg.File.Compress,
	})
}
