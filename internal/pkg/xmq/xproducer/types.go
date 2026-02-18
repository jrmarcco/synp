package xproducer

import (
	"context"

	"github.com/jrmarcco/synp/internal/pkg/xmq"
)

type Producer interface {
	Produce(ctx context.Context, msg *xmq.Message) error
}
