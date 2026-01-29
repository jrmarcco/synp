package provider

import (
	"fmt"

	"github.com/jrmarcco/synp/internal/pkg/codec"
	"github.com/spf13/viper"
)

func newCodec() (codec.Codec, error) {
	type config struct {
		Type string `mapstructure:"type"` // "json" or "proto"
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.codec", &cfg); err != nil {
		return nil, err
	}

	switch cfg.Type {
	case "json":
		return codec.NewJSONCodec(), nil
	case "proto":
		return codec.NewProtoCodec(), nil
	default:
		return nil, fmt.Errorf("unsupported codec type: %s, expected 'json' or 'proto'", cfg.Type)
	}
}
