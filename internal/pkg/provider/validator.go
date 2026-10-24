package provider

import (
	"context"
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/auth"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

// newValidator 创建连接鉴权用的 JWT 验签器。
// 公钥经 JWKS 端点 ( /.well-known/jwks.json ) 定期拉取并缓存，
// token header 的 kid 用于密钥轮换，签名算法固定 RS256。
func newValidator(lifecycle fx.Lifecycle) (*auth.JwtValidator, error) {
	type config struct {
		Issuer          string        `mapstructure:"issuer"`
		Audience        string        `mapstructure:"audience"`
		JWKSEndpoint    string        `mapstructure:"jwks_endpoint"`
		RefreshInterval time.Duration `mapstructure:"refresh_interval"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("jwt", &cfg); err != nil {
		return nil, err
	}

	if cfg.JWKSEndpoint == "" {
		return nil, fmt.Errorf("jwt.jwks_endpoint is required")
	}
	if cfg.Audience == "" {
		return nil, fmt.Errorf("jwt.audience is required")
	}

	jwksClient := auth.NewJWKSClient(cfg.JWKSEndpoint, cfg.RefreshInterval)

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			jwksClient.Start(ctx)
			return nil
		},
		OnStop: func(_ context.Context) error {
			jwksClient.Close()
			return nil
		},
	})

	return auth.NewJwtValidator(jwksClient, cfg.Audience), nil
}
