package xproducer

import (
	"context"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
)

//go:generate mockgen -source=types.go -destination=mock/xproducer.mock.go -package=xproducermock -typed Producer

type Producer interface {
	Produce(ctx context.Context, msg *xmq.Message) error
}
