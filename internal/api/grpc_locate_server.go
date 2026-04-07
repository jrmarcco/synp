package api

import (
	"context"

	nodev1 "github.com/jrmarcco/synp-api/api/go/node/v1"
	"github.com/jrmarcco/synp/internal/service"
)

var _ nodev1.LocateServiceServer = (*GrpcLocateServer)(nil)

type GrpcLocateServer struct {
	locateService service.LocalService
}

func NewGrpcLocateServer(locateService service.LocalService) *GrpcLocateServer {
	return &GrpcLocateServer{
		locateService: locateService,
	}
}

// LocateUser 定位用户，用于获取用户所在的网关节点。
func (s *GrpcLocateServer) LocateUser(ctx context.Context, req *nodev1.LocateUserRequest) (*nodev1.LocateUserResponse, error) {
	locations, err := s.locateService.LocateUser(ctx, req.BusinessId, req.UserId)
	if err != nil {
		return nil, err
	}

	pbLocations := make([]*nodev1.UserLocation, 0, len(locations))
	for _, location := range locations {
		pbLocations = append(pbLocations, &nodev1.UserLocation{
			ServiceName: location.ServiceName,
			ServiceAddr: location.ServiceAddr,
		})
	}

	return &nodev1.LocateUserResponse{
		Locations: pbLocations,
	}, nil
}
