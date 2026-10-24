// Package logging 提供项目统一的日志输出。
//
// 两种模式:
//   - dev:  单行紧凑格式 ( 日期时间 级别 组件 消息 k=v )、TTY 着色，便于开发观察。
//   - prod: zap core + JSON encoder 承载的 slog handler，面向日志采集。
//
// 两种模式均支持可选双写落盘 ( dev 落盘为同格式纯文本，prod 落盘为 JSON )。
//
// 装配参数由调用方 ( provider ) 从应用配置传入，不直接读取环境变量。
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zapcore"
)

// logFilePerm 日志文件权限。
const logFilePerm os.FileMode = 0o644

// componentKey 组件属性键: For 注入、ConsoleHandler 据此渲染组件列。
const componentKey = "component"

// Mode 为日志输出模式。
type Mode string

const (
	ModeDev  Mode = "dev"
	ModeProd Mode = "prod"
)

// Options 为日志装配参数。
type Options struct {
	// Mode 为输出模式，空串或未知值回落 dev。
	Mode Mode

	// Level 为级别阈值。
	Level slog.Level

	// Color 控制终端着色: auto | always | never ( 仅 dev 模式生效，缺省 auto )。
	Color string

	// File 为可选的追加写日志文件路径，空串不落盘。
	File string
}

// For 返回携带组件标识的日志器 ( 组件即日志的功能域标签，如 ws.server、conn.registry )。
// 每次即时解析 slog.Default()，测试期替换全局 handler 后新调用即刻生效。
func For(component string) *slog.Logger {
	return slog.Default().With(componentKey, component)
}

// Truncate 按 rune 安全截断字符串，超限时以省略号加剩余字符数标注。
// 用于热路径日志输出前裁剪大体积字段 ( 如消息 body )。
func Truncate(s string, limit int) string {
	if limit <= 0 {
		return s
	}

	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit]) + fmt.Sprintf("…(+%d)", len(runes)-limit)
}

// Setup 按参数装配全局日志 ( dev 终端单行 / prod JSON，可选文件双写 )，
// 返回的 cleanup 需在进程退出前调用以冲刷并关闭文件句柄。
func Setup(opts Options) func() {
	var handler slog.Handler
	closers := make([]func(), 0, 2)

	if opts.Mode == ModeProd {
		handler = newProdHandler(os.Stdout, opts.Level)
		if path := opts.File; path != "" {
			if fileHandler, closer, err := newProdFileHandler(path, opts.Level); err == nil {
				handler = multiHandler{handlers: []slog.Handler{handler, fileHandler}}
				closers = append(closers, closer)
			} else {
				// 装配失败降级为仅终端输出，不阻断启动。
				slog.Error("日志文件打开失败", "path", path, "error", err)
			}
		}
	} else {
		color := shouldColor(opts.Color)
		handler = NewConsoleHandler(os.Stdout, opts.Level, color)
		if path := opts.File; path != "" {
			if file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, logFilePerm); err == nil { //nolint:gosec // 路径来自运维显式配置，非不可信输入
				handler = multiHandler{handlers: []slog.Handler{
					handler,
					NewConsoleHandler(file, opts.Level, false),
				}}
				closers = append(closers, func() { _ = file.Close() })
			} else {
				slog.Error("日志文件打开失败", "path", path, "error", err)
			}
		}
	}

	slog.SetDefault(slog.New(handler))

	return func() {
		for _, closer := range closers {
			closer()
		}
	}
}

// newProdHandler 创建生产模式 handler: zap core + JSON encoder。
// zap 的编码器、级别过滤与 Sync 能力成熟，作为 slog 的后端承载。
func newProdHandler(w io.Writer, level slog.Level) slog.Handler {
	return zapslog.NewHandler(newZapCore(w, level))
}

// newProdFileHandler 创建生产模式的落盘 handler。
func newProdFileHandler(path string, level slog.Level) (slog.Handler, func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, logFilePerm) //nolint:gosec // 路径来自运维显式配置，非不可信输入
	if err != nil {
		return nil, nil, err
	}
	return newProdHandler(file, level), func() { _ = file.Close() }, nil
}

// newZapCore 构建 JSON 编码的 zap core，级别阈值与 slog 对齐。
func newZapCore(w io.Writer, level slog.Level) zapcore.Core {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "time"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	return zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(w),
		zap.NewAtomicLevelAt(zapLevel(level)),
	)
}

// zapLevel 将 slog 级别映射为 zap 级别。
func zapLevel(level slog.Level) zapcore.Level {
	switch {
	case level >= slog.LevelError:
		return zapcore.ErrorLevel
	case level >= slog.LevelWarn:
		return zapcore.WarnLevel
	case level >= slog.LevelInfo:
		return zapcore.InfoLevel
	default:
		return zapcore.DebugLevel
	}
}

// shouldColor 判定终端输出是否着色 ( mode: auto | always | never )。
func shouldColor(mode string) bool {
	switch mode {
	case "always":
		return true
	case "never":
		return false
	default:
		return isTerminal(os.Stdout)
	}
}

// isTerminal 判定文件描述符是否为终端 ( 字符设备 )。
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// ParseLevel 解析级别名 ( 大小写不敏感，空串与非法值回落 info )。
func ParseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
