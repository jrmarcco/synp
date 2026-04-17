package ws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/gobwas/httphead"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/auth"
	"github.com/jrmarcco/synp/internal/pkg/compression"
	"github.com/jrmarcco/synp/internal/pkg/session"
	sr "github.com/jrmarcco/synp/internal/pkg/session/redis"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	AuthorizationHeader = "authorization"
	AuthorizationPrefix = "bearer"
)

var (
	ErrTokenRequired             = errors.New("token is required")
	ErrInvalidToken              = errors.New("invalid token")
	ErrInvalidAuthorizationToken = errors.New("invalid Authorization header token")
)

var _ synp.Upgrader = (*Upgrader)(nil)

type Upgrader struct {
	rdb redis.Cmdable

	validator         auth.Validator
	compressionConfig compression.Config

	logger *zap.Logger
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
	return ctx.sess, &state, nil
}

type upgradeContext struct {
	sess session.Session

	reqURI []byte

	token                      string
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
	u.logger.Info("[upgrader] compression enabled", zap.Any("params", params))
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
		OnBeforeUpgrade: func() (header ws.HandshakeHeader, err error) {
			return u.beforeUpgrade(ctx)
		},
	}
}

// negotiateExtensions 协商压缩扩展。
//
// 客户端推荐连接参数：
//
//   - 无压缩：
//     websocat -v \
//     "wss://gateway.example.com/ws?device=pc" \
//     --header "Authorization: Bearer <JWT>" \
//     --header "X-Request-Id: $(uuidgen)"
//
//   - 启用压缩：
//     websocat -v \
//     "wss://gateway.example.com/ws?device=pc" \
//     --header "Authorization: Bearer <JWT>" \
//     --header "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=15" \
//     --header "X-Request-Id: $(uuidgen)"
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
	// 解析 Authorization 头并提取 token。
	if strings.EqualFold(string(key), AuthorizationHeader) {
		ctx.token = string(val)
	}

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

		u.logger.Info(
			"[upgrader] successfully negotiated compression",
			zap.Any("negotiated_params", params),
		)
		return
	}

	// 客户端未请求压缩时，降级为无压缩是预期行为，不需要告警。
	if clientRequestedCompression {
		u.logger.Warn("[upgrader] failed to negotiate compression, downgrade to no compression")
	}
}

// beforeUpgrade 在升级前准备用户信息。
func (u *Upgrader) beforeUpgrade(ctx *upgradeContext) (ws.HandshakeHeader, error) {
	user, err := u.prepareUpgradeUser(ctx)
	if err != nil {
		return nil, err
	}

	// 构建 session。
	createdSession, err := u.buildSession(user)
	if err != nil {
		return nil, err
	}

	ctx.sess = createdSession
	return ws.HandshakeHeaderString(""), nil
}

func (u *Upgrader) prepareUpgradeUser(ctx *upgradeContext) (session.User, error) {
	// 校验 token 并提取用户信息。
	user, err := u.extractUser(ctx.reqURI, ctx.token)
	if err != nil {
		return session.User{}, err
	}

	// 设置 auto close 参数。
	user.AutoClose = ctx.autoClose
	return user, nil
}

// getUserInfo 从 Header + URI 中获取用户信息。
func (u *Upgrader) extractUser(uri []byte, authorization string) (session.User, error) {
	token, err := u.extractToken(authorization)
	if err != nil {
		u.logger.Error("[upgrader] failed to extract token from authorization header", zap.Error(err))
		return session.User{}, err
	}

	var user session.User
	user, err = u.validator.Validate(context.Background(), token)
	if err != nil {
		u.logger.Error("[upgrader] failed to validate token", zap.Error(err))
		return session.User{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	// 从 URI 中提取设备类型。
	device := u.extractDevice(uri)
	user.Device = device

	return user, nil
}

// extractToken 从 Header 中提取 token。
func (u *Upgrader) extractToken(authorization string) (string, error) {
	parts := strings.Fields(authorization)
	if len(parts) != 2 { //nolint:mnd // 期望的 Authorization 格式为 "Bearer <token>"
		return "", ErrTokenRequired
	}

	if !strings.EqualFold(parts[0], AuthorizationPrefix) {
		return "", ErrInvalidAuthorizationToken
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", ErrTokenRequired
	}
	return token, nil
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
			zap.String("device", string(device)),
		)
		return session.DeviceUnknown
	}
}

func (u *Upgrader) buildSession(user session.User) (session.Session, error) {
	// 初始化 session。
	sessionBuilder := sr.NewSessionBuilder(u.rdb)
	createdSession, isNew, err := sessionBuilder.Build(context.Background(), user)
	if err != nil {
		return nil, err
	}

	if !isNew {
		u.logger.Warn("[upgrader] session already exists", zap.Any("user", user))
	}

	return createdSession, nil
}

func NewUpgrader(rdb redis.Cmdable, validator auth.Validator, compressionConfig compression.Config, logger *zap.Logger) *Upgrader {
	return &Upgrader{
		rdb:               rdb,
		validator:         validator,
		compressionConfig: compressionConfig,
		logger:            logger,
	}
}
