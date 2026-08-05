package batch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"golang.org/x/sync/errgroup"
)

var ErrCoordinatorStopped = errors.New("coordinator stopped")

const (
	batchProcessingTimeout = time.Second
)

// Coordinator 批量消息处理器。
type Coordinator struct {
	mu sync.RWMutex

	batches   map[int64]*Batch // bizID -> batch
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

// OnReceive 处理接收到的消息。
// 将请求添加到批次并等待处理结果。
func (c *Coordinator) OnReceive(bizID int64, connID string, msg *messagev1.Message) error {
	c.mu.RLock()
	if c.stopped {
		//　协调器已经停止。
		c.mu.RUnlock()
		return nil
	}
	// 别忘了释放读锁。
	c.mu.RUnlock()

	req := &Req{
		Msg:     msg,
		ConnID:  connID,
		ResChan: make(chan *Res, 1),
	}

	// 添加到批次。
	c.addToBatch(bizID, req)

	// 等待处理结果。
	select {
	case res := <-req.ResChan:
		return res.Err
	case <-c.stopChan:
		return ErrCoordinatorStopped
	}
}

func (c *Coordinator) addToBatch(bizID int64, req *Req) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		// 协调器已经停止，直接返回错误。
		select {
		case req.ResChan <- &Res{Err: ErrCoordinatorStopped}:
		default:
		}
		return
	}

	batch, ok := c.batches[bizID]
	if !ok {
		batch = &Batch{
			bizID: bizID,
			reqs:  make([]*Req, 0, c.batchSize),
		}

		// 设置超时定时器。
		batch.timer = time.AfterFunc(c.timeout, func() {
			// 超时自动处理批次。
			slog.Debug(
				"[batch-coordinator] batch timeout",
				"biz_id", bizID,
				"timeout", c.timeout,
			)
			c.processBatch(bizID)
		})

		c.batches[bizID] = batch
		slog.Debug(
			"[batch-coordinator] new batch created",
			"biz_id", bizID,
			"timeout", c.timeout,
		)
	}

	// 将请求添加到批次。
	batch.reqs = append(batch.reqs, req)

	slog.Debug(
		"[batch-coordinator] request added to batch",
		"biz_id", bizID,
		"conn_id", req.ConnID,
		"req_message_id", req.Msg.GetMessageId(),
		"req_count", len(batch.reqs),
		"threshold", c.batchSize,
	)

	// 检查批次大小是否达到阈值。
	if len(batch.reqs) >= c.batchSize {
		go c.processBatch(bizID)
	}
}

func (c *Coordinator) processBatch(bizID int64) {
	c.mu.Lock()
	batch, ok := c.batches[bizID]
	if !ok {
		c.mu.Unlock()
		return
	}

	delete(c.batches, bizID)
	c.mu.Unlock()

	// 锁定并处理批次。
	batch.mu.Lock()
	defer batch.mu.Unlock()

	if batch.closed || len(batch.reqs) == 0 {
		return
	}

	// 停止计时器。
	if batch.timer != nil {
		_ = batch.timer.Stop()
	}
	batch.closed = true
	slog.Info(
		"[batch-coordinator] processing batch",
		"biz_id", bizID,
		"req_count", len(batch.reqs),
	)

	msgs := make([]*messagev1.Message, 0, len(batch.reqs))
	for i, req := range batch.reqs {
		msgs[i] = req.Msg
	}

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), batchProcessingTimeout)
	err := c.processor.Process(ctx, bizID, msgs)
	cancel()

	duration := time.Since(startTime)

	switch {
	case err != nil:
		slog.Error(
			"[batch-coordinator] failed to process batch",
			"biz_id", bizID,
			"req_count", len(batch.reqs),
			"duration", duration,
			"error", err,
		)
		c.distributeErr(batch.reqs, err)

	default:
		slog.Info(
			"[batch-coordinator] batch processed successfully",
			"biz_id", bizID,
			"req_count", len(batch.reqs),
			"duration", duration,
		)
		c.distributeResult(batch.reqs)
	}
}

func (c *Coordinator) distributeErr(reqs []*Req, err error) {
	for _, req := range reqs {
		select {
		case req.ResChan <- &Res{Err: err}:
		default:
			slog.Warn(
				"[batch-coordinator] failed to distribute error, result channel is blocked",
				"req_message_id", req.Msg.GetMessageId(),
				"req_conn_id", req.ConnID,
				"error", err,
			)
		}
	}
}

func (c *Coordinator) distributeResult(reqs []*Req) {
	for _, req := range reqs {
		select {
		case req.ResChan <- &Res{Err: nil}:
		default:
			slog.Warn(
				"[batch-coordinator] failed to distribute result, result channel is blocked",
				"req_message_id", req.Msg.GetMessageId(),
				"req_conn_id", req.ConnID,
			)
		}
	}
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
				c.processBatch(bizID)
				return nil
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
