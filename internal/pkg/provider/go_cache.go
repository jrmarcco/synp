package provider

import (
	"fmt"
	"time"

	gocache "github.com/patrickmn/go-cache"
	"github.com/spf13/viper"
)

func newGoCache() (*gocache.Cache, error) {
	type config struct {
		DefaultExpiration time.Duration `mapstructure:"default_expiration"`
		CleanupInterval   time.Duration `mapstructure:"cleanup_interval"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("go_cache", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal go cache config: %w", err)
	}

	return gocache.New(cfg.DefaultExpiration, cfg.CleanupInterval), nil
}
