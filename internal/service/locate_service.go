package service

import (
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type LocalService interface{}

var _ LocalService = (*DefaultLocalService)(nil)

type DefaultLocalService struct {
	rdb    redis.Cmdable
	logger *zap.Logger
}

func NewDefaultLocalService(rdb redis.Cmdable, logger *zap.Logger) *DefaultLocalService {
	return &DefaultLocalService{
		rdb:    rdb,
		logger: logger,
	}
}
