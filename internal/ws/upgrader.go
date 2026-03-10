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
	var ext *wsflate.Extension
	if u.compressionConfig.Enabled {
		// 启用压缩时，创建压缩扩展。
		params := u.compressionConfig.ToParamters()
		ext = &wsflate.Extension{Parameters: params}

		u.logger.Info("[upgrader] compression enabled", zap.Any("params", params))
	}

	var user session.User
	var sess session.Session
	var autoClose bool
	var requestURI []byte
	var authorization string
	var clientRequestedCompression bool
	upgrader := ws.Upgrader{
		// 协商过程 ( 主要是协商是否启用压缩算法及压缩参数 )。
		//
		// 客户端推荐连接参数：
		// 	- 无压缩：
		//		websocat -v \
		// 		  "wss://gateway.example.com/ws?device=pc" \
		//   	  --header "Authorization: Bearer <JWT>" \
		//   	  --header "X-Request-Id: $(uuidgen)"
		//
		//  - 启用压缩：
		// websocat -v \
		//   	"wss://gateway.example.com/ws?device=pc" \
		//   	  --header "Authorization: Bearer <JWT>" \
		//   	  --header "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=15" \
		//   	  --header "X-Request-Id: $(uuidgen)"
		ExtensionCustom: func(value []byte, accepted []httphead.Option) ([]httphead.Option, bool) {
			// 禁止空的 Sec-WebSocket-Extensions。
			trimmed := bytes.TrimSpace(value)
			if len(trimmed) == 0 {
				u.logger.Warn("[upgrader] empty Sec-WebSocket-Extensions is forbidden")
				return nil, false
			}
			clientRequestedCompression = true

			// 未启用压缩时忽略扩展协商结果。
			if ext == nil {
				return accepted, true
			}

			idx := -1
			var current httphead.Option
			var malformed bool

			appendNegotiated := func(in httphead.Option) bool {
				if in.Size() == 0 {
					return true
				}
				opt, err := ext.Negotiate(in)
				if err != nil {
					malformed = true
					return false
				}
				if opt.Size() > 0 {
					accepted = append(accepted, opt)
				}
				return true
			}

			ok := httphead.ScanOptions(trimmed, func(i int, name, attr, val []byte) httphead.Control {
				if i != idx {
					if !appendNegotiated(current) {
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
			if !appendNegotiated(current) {
				return nil, false
			}
			return accepted, true
		},
		OnRequest: func(uri []byte) error {
			// 保存请求 URI，后续在 OnBeforeUpgrade 中解析 device。
			requestURI = append(requestURI[:0], uri...)
			return nil
		},
		OnHeader: func(key, value []byte) error {
			// 解析 Authorization 头并提取 token。
			if strings.EqualFold(string(key), AuthorizationHeader) {
				authorization = string(value)
			}

			// 解析 auto close 参数。
			if strings.EqualFold(string(key), "x-auto-close") {
				autoClose = string(value) == "true"
				u.logger.Warn(
					"[upgrader] auto close parameter parsed",
					zap.String("header_key", string(key)),
					zap.String("header_value", string(value)),
					zap.Bool("auto_close", autoClose),
				)
			}
			return nil
		},
		OnBeforeUpgrade: func() (header ws.HandshakeHeader, err error) {
			// 校验 token 并提取用户信息。
			if user, err = u.extractUserInfo(requestURI, authorization); err != nil {
				return nil, err
			}

			// 设置 auto close 参数。
			user.AutoClose = autoClose

			// 初始化 session。
			sessionBuilder := sr.NewSessionBuilder(u.rdb)
			createdSession, isNew, err := sessionBuilder.Build(context.Background(), user)
			if err != nil {
				return nil, err
			}

			if !isNew {
				u.logger.Warn("[upgrader] session already exists", zap.Any("user", user))
			}

			sess = createdSession
			return ws.HandshakeHeaderString(""), nil
		},
	}

	state := compression.State{
		Enabled: false,
	}

	if _, err := upgrader.Upgrade(conn); err != nil {
		return nil, nil, err
	}

	// 检查协商压缩的结果。
	if ext != nil {
		if params, accepted := ext.Accepted(); accepted {
			state.Enabled = true
			state.Ext = ext
			state.Params = params

			u.logger.Info(
				"[upgrader] successfully negotiated compression",
				zap.Any("negotiated_params", params),
			)
			return sess, &state, nil
		}

		// 客户端未请求压缩时，降级为无压缩是预期行为，不需要告警。
		if clientRequestedCompression {
			u.logger.Warn("[upgrader] failed to negotiate compression, downgrade to no compression")
		}
	}

	return sess, &state, nil
}

// extractTokenFromHeader 从 Header 中提取 token。
func (u *Upgrader) extractTokenFromHeader(authorization string) (string, error) {
	parts := strings.Fields(authorization)
	if len(parts) != 2 {
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

// getUserInfo 从 Header + URI 中获取用户信息。
func (u *Upgrader) extractUserInfo(uri []byte, authorization string) (session.User, error) {
	token, err := u.extractTokenFromHeader(authorization)
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

func NewUpgrader(rdb redis.Cmdable, validator auth.Validator, compressionConfig compression.Config, logger *zap.Logger) *Upgrader {
	return &Upgrader{
		rdb:               rdb,
		validator:         validator,
		compressionConfig: compressionConfig,
		logger:            logger,
	}
}
