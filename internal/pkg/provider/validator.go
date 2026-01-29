package provider

import (
	"github.com/jrmarcco/jit/xjwt"
	authv1 "github.com/jrmarcco/synp-api/api/go/auth/v1"
	"github.com/jrmarcco/synp/internal/pkg/auth"
	"github.com/spf13/viper"
)

func newValidator() (*auth.JwtValidator, error) {
	type config struct {
		Issuer  string `mapstructure:"issuer"`
		Private string `mapstructure:"private"`
		Public  string `mapstructure:"public"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("jwt", &cfg); err != nil {
		return nil, err
	}

	claimsCfg := xjwt.NewClaimsConfig(xjwt.WithIssuer(cfg.Issuer))
	// 这里只使用 public key 进行验证，不需要加密。
	jwtManager, err := xjwt.NewEd25519VerifierBuilder[authv1.JwtPayload](cfg.Public).
		ClaimsConfig(claimsCfg).
		Build()
	if err != nil {
		return nil, err
	}

	return auth.NewJwtValidator(jwtManager), nil
}
