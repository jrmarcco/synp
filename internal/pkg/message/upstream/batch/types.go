package batch

import (
	"context"
	"sync"
	"time"

	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
)

type Processor interface {
	Process(ctx context.Context, bizID int64, msgs []*messagev1.Message) error
}

type Batch struct {
	mu sync.Mutex

	bizID  int64
	msgs   []*messagev1.Message
	timer  *time.Timer
	closed bool
}
