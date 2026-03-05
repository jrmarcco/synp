package auth

import (
	"context"

	"github.com/jrmarcco/jit/xjwt"
	authv1 "github.com/jrmarcco/synp-api/api/go/auth/v1"
	"github.com/jrmarcco/synp/internal/pkg/session"
)

var _ Validator = (*JwtValidator)(nil)

type JwtValidator struct {
	jwtManager xjwt.Manager[authv1.JwtPayload]
}

func (v *JwtValidator) Validate(_ context.Context, token string) (session.User, error) {
	claims, err := v.jwtManager.Decrypt(token)
	if err != nil {
		return session.User{}, err
	}

	return session.User{
		BID: claims.Data.BizId,
		UID: claims.Data.UserId,
	}, nil
}

func NewJwtValidator(jwtManager xjwt.Manager[authv1.JwtPayload]) *JwtValidator {
	return &JwtValidator{jwtManager: jwtManager}
}
