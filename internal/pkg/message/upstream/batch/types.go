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
	reqs   []*Req
	timer  *time.Timer
	closed bool
}

type Req struct {
	Msg     *messagev1.Message
	ConnID  string
	ResChan chan *Res
}

// Res 批量处理结果。
// 批次内每个消息都返回一个结果。
type Res struct {
	// 注意：
	//  我这里和后端使用 kafka 通信，所以只有一个 Err 字段记录错误信息即可。
	//  如果是使用 grpc 调用，那么可以增加一个 grpc 服务端的响应字段 （ 如 Response ）。
	Err error
}
