package batch

import (
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Coordinator 批量消息处理器。
type Coordinator struct {
	mu sync.RWMutex

	batches   map[int64]*Batch
	batchSize int

	timeout   time.Duration
	processor Processor

	stopChan chan struct{}
	stopped  bool
}

func NewCoordinator(batchSize int, timeout time.Duration, processor Processor) *Coordinator {
	return &Coordinator{
		batches:   make(map[int64]*Batch),
		batchSize: batchSize,

		timeout:   timeout,
		processor: processor,

		stopChan: make(chan struct{}),
	}
}

func (c *Coordinator) processBatch(bizID int64) error {
	// TODO: not implemented.
	panic("not implemented")
}

// Stop 停止协处理器。
// 先停止接收新请求，处理完当前批次后关闭并通知等待者。
func (c *Coordinator) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}

	c.stopped = true

	pendingBizIDs := make([]int64, 0, len(c.batches))
	for bizID := range c.batches {
		pendingBizIDs = append(pendingBizIDs, bizID)
	}

	slog.Info(
		"stopping coordinator",
		"pending_batches", pendingBizIDs,
	)
	c.mu.Unlock()

	if len(pendingBizIDs) > 0 {
		var eg errgroup.Group
		for _, bizID := range pendingBizIDs {
			eg.Go(func() error {
				return c.processBatch(bizID)
			})
		}

		if err := eg.Wait(); err != nil {
			slog.Error(
				"error waiting for pending batches",
				"error", err.Error(),
			)
		}
	}

	c.mu.Lock()

	close(c.stopChan)
	// 只是一个保险措施，确保 batches 被清空。
	c.batches = make(map[int64]*Batch)

	c.mu.Unlock()

	slog.Info("coordinator successfully stopped")
}
