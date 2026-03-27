package service

import (
	"context"

	"github.com/jrmarcco/synp"
	"go.uber.org/zap"
)

type UserLocation struct {
	ServiceName string
	ServiceAddr string
}

type LocalService interface {
	LocateUser(ctx context.Context, bid, uid uint64) ([]UserLocation, error)
}

var _ LocalService = (*DefaultLocalService)(nil)

type DefaultLocalService struct {
	connRegistry synp.ConnRegistry
	logger       *zap.Logger
}

func NewDefaultLocalService(
	connRegistry synp.ConnRegistry,
	logger *zap.Logger,
) *DefaultLocalService {
	return &DefaultLocalService{
		connRegistry: connRegistry,
		logger:       logger,
	}
}

func (s *DefaultLocalService) LocateUser(ctx context.Context, bid, uid uint64) ([]UserLocation, error) {
	nodes, err := s.connRegistry.FindNodes(ctx, bid, uid)
	if err != nil {
		return nil, err
	}

	userLocations := make([]UserLocation, 0, len(nodes))
	for _, connNode := range nodes {
		userLocations = append(userLocations, UserLocation{
			ServiceName: connNode.ServiceName,
			ServiceAddr: connNode.ServiceAddr,
		})
	}

	return userLocations, nil
}
