package provider

import (
	"fmt"
	"time"

	"github.com/jrmarcco/xgrpc/register/etcd"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func newGrpcRegistry(etcdClient *clientv3.Client) (*etcd.Registry, error) {
	type config struct {
		KeyPrefix string        `mapstructure:"key_prefix"`
		LeaseTTL  time.Duration `mapstructure:"lease_ttl"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("grpc.registry", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal grpc registry config: %w", err)
	}

	return etcd.NewBuilder(etcdClient).
		KeyPrefix(cfg.KeyPrefix).
		LeaseTTL(int(cfg.LeaseTTL.Seconds())).
		Build()
}
