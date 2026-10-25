package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// 测试常量 ( 避免魔数直书 )。
const (
	truncateLimit  = 5
	timeLineRegexp = `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3} `
	errKey         = "error"
)

// newTestLogger 构造携带组件标识的测试日志器。
func newTestLogger(buf *bytes.Buffer, level slog.Leveler, color bool) *slog.Logger {
	return slog.New(NewConsoleHandler(buf, level, color)).With(componentKey, "engine")
}

func TestConsoleHandlerSingleLine(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo, false)

	logger.Info("运行开始", "session", "s1", "workspace", "/ws")
	logger.Debug("不应出现")

	line := buf.String()
	if matched, err := regexp.MatchString(timeLineRegexp, line); err != nil || !matched {
		t.Fatalf("行首应为毫秒时间戳，实际 %q", line)
	}
	if want := "INFO  engine     运行开始 session=s1 workspace=/ws"; !strings.Contains(line, want) {
		t.Fatalf("应包含紧凑单行字段，实际 %q", line)
	}
	if strings.Count(line, "\n") != 1 {
		t.Fatalf("应为单行输出，实际 %d 行", strings.Count(line, "\n"))
	}
	if strings.Contains(line, "不应出现") {
		t.Fatal("低于级别阈值的记录不应输出")
	}
	if strings.Contains(line, "\x1b[") {
		t.Fatal("未开颜色时不应包含 ANSI 转义码")
	}
}

func TestConsoleHandlerColor(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo, true)
	logger.Info("运行开始")

	line := buf.String()
	if !strings.Contains(line, ansiBlue+"INFO ") {
		t.Fatalf("INFO 徽应着蓝色，实际 %q", line)
	}
	if !strings.Contains(line, ansiCyan+"engine") {
		t.Fatalf("组件列应着青色，实际 %q", line)
	}
	if !strings.Contains(line, ansiReset) {
		t.Fatalf("着色后应重置颜色，实际 %q", line)
	}
}

func TestConsoleHandlerValueQuoting(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo, false)
	logger.Error("失败", "error", errors.New("boom"), "reason", "包含 空格")

	line := buf.String()
	if !strings.Contains(line, "error=boom") {
		t.Fatalf("error 值应渲染错误信息，实际 %q", line)
	}
	if !strings.Contains(line, `reason="包含 空格"`) {
		t.Fatalf("含空白的值应加引号，实际 %q", line)
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "未超限原样返回", in: "abc", want: "abc"},
		{name: "超限截断并标注剩余", in: "abcdefghij", want: "abcde…(+5)"},
		{name: "中文按 rune 计数", in: "中文字符测试", want: "中文字符测…(+1)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := Truncate(tc.in, truncateLimit); got != tc.want {
				t.Fatalf("Truncate(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTruncateNonPositiveLimit(t *testing.T) {
	t.Parallel()

	if got := Truncate("abcdef", 0); got != "abcdef" {
		t.Fatalf("limit<=0 应原样返回，实际 %q", got)
	}
}

func TestParseLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want slog.Level
	}{
		{name: "空串回落 info", in: "", want: slog.LevelInfo},
		{name: "debug", in: "debug", want: slog.LevelDebug},
		{name: "大小写不敏感", in: " WARN ", want: slog.LevelWarn},
		{name: "error", in: "error", want: slog.LevelError},
		{name: "非法值回落 info", in: "bogus", want: slog.LevelInfo},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := ParseLevel(tc.in); got != tc.want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestForAttachesComponent(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	previous := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})
	slog.SetDefault(slog.New(NewConsoleHandler(&buf, slog.LevelInfo, false)))

	For("approval").Warn("审核拒绝", "tool", "bash")

	if line := buf.String(); !strings.Contains(line, "approval   审核拒绝 tool=bash") {
		t.Fatalf("For 应注入组件列，实际 %q", line)
	}
}

func TestProdHandlerJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(newProdHandler(&buf, slog.LevelInfo)).With(componentKey, "ws.server")

	logger.Warn("配置降级", "topic", "synp.message.notify", errKey, errors.New("kafka unreachable"))

	line := strings.TrimSpace(buf.String())
	if !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
		t.Fatalf("prod 模式应为 JSON 单行输出，实际 %q", line)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("prod 输出应为合法 JSON: %v", err)
	}
	for _, key := range []string{"time", "level", "msg", "component", "topic", errKey} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("prod 输出应包含字段 %s，实际 %q", key, line)
		}
	}
}

func TestSetupDevAndProdModes(t *testing.T) {
	t.Parallel()

	previous := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	cleanup := Setup(Options{Mode: ModeDev, Level: slog.LevelError, Color: "never"})
	defer cleanup()

	logging := For("setup-check")
	if !logging.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("Setup 后全局日志器应使用配置级别")
	}
	if logging.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("低于配置级别的日志不应启用")
	}
}

func TestFileRotation(t *testing.T) {
	t.Parallel()

	previous := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	dir := t.TempDir()
	logPath := filepath.Join(dir, "hermet.log")

	// 1MB 切割 + 保留 2 份 + 压缩，写入超过 1MB 必然触发轮转。
	cleanup := Setup(Options{
		Mode:       ModeProd,
		Level:      slog.LevelDebug,
		File:       logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Compress:   true,
	})
	defer cleanup()

	logger := For("rotation-test")
	const rounds = 6000
	const payload = "rotation payload for exceeding one megabyte across writes 0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz"
	for i := range rounds {
		logger.Info(payload, "round", i)
	}
	cleanup()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取日志目录失败: %v", err)
	}

	var rotated int
	for _, e := range entries {
		if e.Name() != "hermet.log" {
			rotated++
		}
	}
	if rotated == 0 {
		t.Fatalf("写入超过 MaxSizeMB 应产生轮转备份文件，目录内容: %v", entries)
	}

	// 主日志文件仍应存在且非空。
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("主日志文件应存在: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("主日志文件不应为空")
	}
}

func TestFileParentDirCreation(t *testing.T) {
	t.Parallel()

	previous := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	dir := t.TempDir()
	// 深层父目录不存在，Setup 应自动创建。
	logPath := filepath.Join(dir, "nested", "logs", "hermet.log")

	cleanup := Setup(Options{Mode: ModeDev, File: logPath, Level: slog.LevelInfo})
	defer cleanup()

	For("dir-check").Info("写入深层目录")

	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("父目录应被自动创建且文件可写: %v", err)
	}
}
