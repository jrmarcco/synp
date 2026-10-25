package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jrmarcco/synp/internal/pkg/session"
)

// defaultClockLeeway 为 exp 判断保留的时钟容差，
// 各节点需 NTP 对时 ( 见 proto/auth/v1/auth.proto 第 4 条 )。
const defaultClockLeeway = 60

var _ Validator = (*JwtValidator)(nil)

// jwtClaims 是网关连接鉴权 JWT 的 claims 契约 ( proto/auth/v1/auth.proto )。
// 业务 claims 字段名与 token 中的 JSON key 一致 ( snake_case )，
// 注册 claims ( iss/aud/iat/exp/jti ) 由 jwt.RegisteredClaims 按标准名解析。
//
// business_id / user_id 以十进制字符串写入 token ( int64 超出 JS 安全整数，
// 客户端需无损读取 )；解析侧同时兼容数字字面量 ( 升级前签发的存量 token )。
type jwtClaims struct {
	jwt.RegisteredClaims

	BusinessID uint64 `json:"business_id"`
	UserID     uint64 `json:"user_id"`
	SessionID  string `json:"session_id"`
}

// UnmarshalJSON 兼容字符串与数字字面量两种 ID 形态。
func (c *jwtClaims) UnmarshalJSON(data []byte) error {
	type alias jwtClaims

	aux := struct {
		*alias

		BusinessID *flexibleU64 `json:"business_id"`
		UserID     *flexibleU64 `json:"user_id"`
	}{alias: (*alias)(c)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.BusinessID != nil {
		c.BusinessID = uint64(*aux.BusinessID)
	}
	if aux.UserID != nil {
		c.UserID = uint64(*aux.UserID)
	}
	return nil
}

// flexibleU64 兼容十进制字符串与数字字面量的 uint64。
type flexibleU64 uint64

func (u *flexibleU64) UnmarshalJSON(data []byte) error {
	s := string(data)
	if s == "null" {
		*u = 0
		return nil
	}
	if len(s) >= 2 && s[0] == '"' {
		s = s[1 : len(s)-1]
	}

	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}
	*u = flexibleU64(val)
	return nil
}

// JwtValidator 使用 RS256 算法对连接鉴权 JWT 做本地验签。
//
// 公钥经 JWKS 端点拉取并缓存，token header 的 kid 用于密钥轮换。
// 校验项: 签名、exp、aud。全程不回调后端。
type JwtValidator struct {
	jwks *JWKSClient

	// audience 为网关服务标识 ( 如 "synp-gateway" )，
	// 防止为其它用途签发的 token 被重放到网关连接上。
	audience string
}

func NewJwtValidator(jwks *JWKSClient, audience string) *JwtValidator {
	return &JwtValidator{
		jwks:     jwks,
		audience: audience,
	}
}

func (v *JwtValidator) Validate(ctx context.Context, token string) (session.User, error) {
	claims := &jwtClaims{}

	_, err := jwt.ParseWithClaims(
		token,
		claims,
		func(token *jwt.Token) (any, error) {
			kid, ok := token.Header["kid"].(string)
			if !ok || kid == "" {
				return nil, fmt.Errorf("jwt token header missing kid")
			}
			return v.jwks.KeyFunc(ctx, kid)
		},
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithAudience(v.audience),
		jwt.WithLeeway(defaultClockLeeway),
	)
	if err != nil {
		return session.User{}, fmt.Errorf("failed to verify jwt token: %w", err)
	}

	return session.User{
		BID:       claims.BusinessID,
		UID:       claims.UserID,
		SessionID: claims.SessionID,
	}, nil
}
