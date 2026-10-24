package ws

import (
	"bytes"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"strings"

	"github.com/gobwas/httphead"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/compression"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/session"
)

var ErrInvalidDeviceType = errors.New("invalid device type")

var _ synp.Upgrader = (*Upgrader)(nil)

// Upgrader 将 HTTP 连接升级为 WebSocket 连接。
//
// 注意：鉴权不在升级阶段完成——新协议要求连接建立后的第一帧必须为 AUTH，
// 携带后端签发的 JWT ( 契约见 api/proto/auth/v1/auth.proto )。
// 升级阶段只协商压缩扩展并解析设备类型等非敏感参数。
type Upgrader struct {
	compressionConfig compression.Config

	logger *slog.Logger
}

func (u *Upgrader) Name() string {
	return "synp.upgrader"
}

func (u *Upgrader) Upgrade(conn net.Conn) (session.Session, *compression.State, error) {
	ext := u.newCompressionExtension()
	ctx := &upgradeContext{}
	upgrader := u.newWebsocketUpgrader(ext, ctx)
	state := compression.State{Enabled: false}

	if _, err := upgrader.Upgrade(conn); err != nil {
		return nil, nil, err
	}

	u.finalizeCompressionState(ext, ctx.clientRequestedCompression, &state)

	// 连接建立时身份未知，返回匿名 Session。
	ctx.sess = session.NewAnonymousSession(session.User{
		Device:    ctx.device,
		AutoClose: ctx.autoClose,
	})
	return ctx.sess, &state, nil
}

type upgradeContext struct {
	sess session.Session

	reqURI []byte

	device                     session.Device
	autoClose                  bool
	clientRequestedCompression bool
}

func (u *Upgrader) newCompressionExtension() *wsflate.Extension {
	if !u.compressionConfig.Enabled {
		return nil
	}

	// 启用压缩时，创建压缩扩展。
	params := u.compressionConfig.ToParamters()
	ext := &wsflate.Extension{Parameters: params}
	u.logger.Info("[upgrader] compression enabled", "params", params)
	return ext
}

func (u *Upgrader) newWebsocketUpgrader(ext *wsflate.Extension, ctx *upgradeContext) ws.Upgrader {
	return ws.Upgrader{
		// 协商扩展。
		ExtensionCustom: func(value []byte, accepted []httphead.Option) ([]httphead.Option, bool) {
			return u.negotiateExtensions(ext, ctx, value, accepted)
		},
		OnRequest: func(uri []byte) error {
			// 保存请求 URI，后续在 OnBeforeUpgrade 中解析 device。
			ctx.reqURI = append(ctx.reqURI[:0], uri...)
			return nil
		},
		OnHeader: func(key, value []byte) error {
			return u.parseHeader(ctx, key, value)
		},
		OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
			return u.beforeUpgrade(ctx)
		},
	}
}

// negotiateExtensions 协商压缩扩展。
//
// 客户端推荐连接参数：
//
//   - 无压缩：
//     websocat -v "wss://gateway.example.com/ws?device=pc"
//
//   - 启用压缩：
//     websocat -v \
//     "wss://gateway.example.com/ws?device=pc" \
//     --header "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=15"
//
// 鉴权在连接建立后通过 AUTH 帧完成，连接 URL 不再携带任何凭证。
func (u *Upgrader) negotiateExtensions(
	ext *wsflate.Extension,
	ctx *upgradeContext,
	val []byte,
	accepted []httphead.Option,
) ([]httphead.Option, bool) {
	trimmed, ok := u.validateExtensionHeader(val)
	if !ok {
		return nil, false
	}
	ctx.clientRequestedCompression = true

	// 未启用压缩时忽略扩展协商结果。
	if ext == nil {
		return accepted, true
	}

	return u.scanAndNegotiateOptions(ext, trimmed, accepted)
}

// validateExtensionHeader 验证 Sec-WebSocket-Extensions 头。
// 禁止空的 Sec-WebSocket-Extensions。
func (u *Upgrader) validateExtensionHeader(val []byte) ([]byte, bool) {
	trimmed := bytes.TrimSpace(val)
	if len(trimmed) == 0 {
		u.logger.Warn("[upgrader] empty Sec-WebSocket-Extensions is forbidden")
		return nil, false
	}
	return trimmed, true
}

// scanAndNegotiateOptions 扫描并协商扩展选项。
func (u *Upgrader) scanAndNegotiateOptions(
	ext *wsflate.Extension,
	val []byte,
	accepted []httphead.Option,
) ([]httphead.Option, bool) {
	idx := -1
	var current httphead.Option
	var malformed bool

	appendNegotiated := func(in httphead.Option) ([]httphead.Option, bool) {
		if in.Size() == 0 {
			return accepted, true
		}
		next, ok := u.negotiateOption(ext, accepted, in)
		if !ok {
			malformed = true
			return accepted, false
		}
		accepted = next
		return accepted, true
	}

	ok := httphead.ScanOptions(val, func(i int, name, attr, val []byte) httphead.Control {
		if i != idx {
			var appendOK bool
			accepted, appendOK = appendNegotiated(current)
			if !appendOK {
				return httphead.ControlBreak
			}
			idx = i
			current = httphead.Option{Name: name}
		}
		if attr != nil {
			current.Parameters.Set(attr, val)
		}
		return httphead.ControlContinue
	})
	if !ok || malformed {
		return nil, false
	}
	var appendOK bool
	accepted, appendOK = appendNegotiated(current)
	if !appendOK {
		return nil, false
	}
	return accepted, true
}

// negotiateOption 协商扩展选项。
func (u *Upgrader) negotiateOption(
	ext *wsflate.Extension,
	accepted []httphead.Option,
	opt httphead.Option,
) ([]httphead.Option, bool) {
	negotiated, err := ext.Negotiate(opt)
	if err != nil {
		return nil, false
	}
	if negotiated.Size() > 0 {
		accepted = append(accepted, negotiated)
	}
	return accepted, true
}

func (u *Upgrader) parseHeader(ctx *upgradeContext, key, val []byte) error {
	// 解析 auto close 参数。
	if strings.EqualFold(string(key), "x-auto-close") {
		ctx.autoClose = string(val) == "true"
		u.logger.Debug("[upgrader] auto close parameter parsed")
	}
	return nil
}

// finalizeCompressionState 完成压缩状态的协商。
func (u *Upgrader) finalizeCompressionState(ext *wsflate.Extension, clientRequestedCompression bool, state *compression.State) {
	// 检查协商压缩的结果。
	if ext == nil {
		return
	}

	if params, accepted := ext.Accepted(); accepted {
		state.Enabled = true
		state.Ext = ext
		state.Params = params

		// 压缩级别与最小压缩阈值来自配置，
		// 经 State 传递到连接的出站压缩器。
		state.Level = u.compressionConfig.Level
		state.MinCompressSize = u.compressionConfig.MinCompressSize
		if state.MinCompressSize <= 0 {
			state.MinCompressSize = compression.DefaultMinCompressSize
		}

		u.logger.Info(
			"[upgrader] successfully negotiated compression",
			"negotiated_params", params,
		)
		return
	}

	// 客户端未请求压缩时，降级为无压缩是预期行为，不需要告警。
	if clientRequestedCompression {
		u.logger.Warn("[upgrader] failed to negotiate compression, downgrade to no compression")
	}
}

// beforeUpgrade 在升级前解析设备类型等连接参数。
func (u *Upgrader) beforeUpgrade(ctx *upgradeContext) (ws.HandshakeHeader, error) {
	ctx.device = u.extractDevice(ctx.reqURI)
	return ws.HandshakeHeaderString(""), nil
}

// extractDevice 从 URI 中提取设备类型。
func (u *Upgrader) extractDevice(uri []byte) session.Device {
	parsedURL, err := url.Parse(string(uri))
	if err != nil {
		return session.DeviceUnknown
	}

	queryParam := parsedURL.Query().Get("device")
	if queryParam == "" {
		return session.DeviceUnknown
	}

	// 校验并转换设备类型。
	device := session.Device(queryParam)
	switch device {
	case session.DeviceMobile, session.DeviceTablet, session.DevicePC:
		return device
	default:
		u.logger.Warn(
			"[upgrader] invalid device type, using unknown",
			"device", string(device),
		)
		return session.DeviceUnknown
	}
}

func NewUpgrader(compressionConfig compression.Config) *Upgrader {
	return &Upgrader{
		compressionConfig: compressionConfig,
		logger:            logging.For("ws.upgrader"),
	}
}
