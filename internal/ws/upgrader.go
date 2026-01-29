package ws

import (
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

var (
	ErrTokenRequired = errors.New("token is required")
	ErrInvalidURI    = errors.New("invalid uri")
	ErrInvalidToken  = errors.New("invalid token")
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
	upgrader := ws.Upgrader{
		// 协商过程，这里主要是压缩相关的协商（是否启用以及压缩算法）。
		Negotiate: func(opt httphead.Option) (httphead.Option, error) {
			if ext != nil {
				return ext.Negotiate(opt)
			}
			return httphead.Option{}, nil
		},
		OnRequest: func(uri []byte) error {
			// 验证 token 并提取用户信息。
			var err error
			if user, err = u.extractUserInfo(uri); err != nil {
				return err
			}
			return nil
		},
		OnHeader: func(key, value []byte) error {
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

		u.logger.Warn("[upgrader] failed to negotiate compression, downgrade to no compression")
	}

	return sess, &state, nil
}

// extractToken 从 URI 中提取 token。
func (u *Upgrader) extractToken(uri []byte) (string, error) {
	parsedURL, err := url.Parse(string(uri))
	if err != nil {
		return "", ErrInvalidURI
	}

	token := parsedURL.Query().Get("token")
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

// getUserInfo 从 URI 中获取用户信息。
func (u *Upgrader) extractUserInfo(uri []byte) (session.User, error) {
	token, err := u.extractToken(uri)
	if err != nil {
		u.logger.Error("[upgrader] failed to extract token from uri", zap.Error(err))
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
