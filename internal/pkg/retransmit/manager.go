package retransmit

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jrmarcco/jit/xsync"

	"github.com/jrmarcco/synp"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp/internal/pkg/message"
)

const (
	DefaultRetryInterval = 8 * time.Second
	DefaultMaxRetryCnt   = 3
)

// Task 为重传任务。
// 负责对 downstream 消息的失败重传。
// 每个消息需要一个重传任务，一个重传任务只能对应一个消息。
type Task struct {
	key  string
	conn synp.Conn

	msg *messagev1.Message

	timerPtr      atomic.Pointer[time.Timer] // 重传定时器
	retransmitCnt atomic.Int32               // 重传次数

	manager *Manager
}

func NewManager(retryInterval time.Duration, maxRetryCnt int32, taskFunc message.PushFunc) *Manager {
	if retryInterval <= 0 {
		retryInterval = DefaultRetryInterval
	}
	if maxRetryCnt <= 0 {
		maxRetryCnt = DefaultMaxRetryCnt
	}

	if taskFunc == nil {
		taskFunc = func(_ synp.Conn, _ *messagev1.Message) error {
			return nil
		}
	}

	return &Manager{
		tasks:         &xsync.Map[string, *Task]{},
		retryInterval: retryInterval,
		maxRetryCnt:   maxRetryCnt,
		taskFunc:      taskFunc,
	}
}

func (t *Task) run() {
	// 尝试加载任务，如果不存在说明已被停止。
	if _, ok := t.manager.tasks.Load(t.key); !ok {
		return
	}

	t.retransmitCnt.Add(1)

	// 检查重传次数。
	if t.retransmitCnt.Load() >= t.manager.maxRetryCnt {
		slog.Warn(
			"[retransmit-manager] retransmit task reach max retry cnt",
			"conn_id", t.conn.ID(),
			"message_id", t.msg.MessageId,
			"retransmit_count", t.retransmitCnt.Load(),
		)
		_ = t.manager.stopAndDelete(t.key)
		return
	}

	// 重传。
	err := t.manager.taskFunc(t.conn, t.msg)
	if err != nil {
		slog.Error(
			"[retransmit-manager] failed to retransmit message",
			"conn_id", t.conn.ID(),
			"message_id", t.msg.MessageId,
			"retransmit_count", t.retransmitCnt.Load(),
			"error", err.Error(),
		)

		// 重传失败，直接停止重传任务。
		_ = t.manager.stopAndDelete(t.key)
		return
	}

	t.conn.UpdateActivityTime()
	slog.Debug(
		"[retransmit-manager] successfully retransmit message",
		"conn_id", t.conn.ID(),
		"message_id", t.msg.MessageId,
		"retransmit_count", t.retransmitCnt.Load(),
	)

	// 更新定时器。
	t.timerPtr.Store(time.AfterFunc(t.manager.retryInterval, t.run))
}

func (t *Task) stop() {
	if timer := t.timerPtr.Load(); timer != nil {
		// Stop() 返回 false 表示 timer 已经过期或被停止。
		// 此时需要排空 channel 防止 goroutine 泄露。
		if !timer.Stop() {
			// 尝试排空 channel。
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

// Manager 为重传管理器，负责管理重传任务。
// 重传使用固定间隔重试，直到成功或达到最大重传次数。
type Manager struct {
	tasks *xsync.Map[string, *Task] // key (connId:messageId) -> retransmit.Task

	totalTaskCnt  atomic.Int64
	retryInterval time.Duration // 重传间隔
	maxRetryCnt   int32         // 最大重传次数

	taskFunc message.PushFunc
	closed   atomic.Bool
}

func (m *Manager) Start(conns []synp.Conn, msg *messagev1.Message) {
	for _, conn := range conns {
		m.start(conn, msg)
	}
}

func (m *Manager) start(conn synp.Conn, msg *messagev1.Message) {
	if m.closed.Load() {
		return
	}

	task := &Task{
		key:     m.taskKey(conn.ID(), msg.MessageId),
		conn:    conn,
		msg:     msg,
		manager: m,
	}

	if _, ok := m.tasks.LoadOrStore(task.key, task); ok {
		return
	}

	task.timerPtr.Store(time.AfterFunc(m.retryInterval, task.run))
	m.totalTaskCnt.Add(1)

	slog.Debug(
		"[retransmit-manager] successfully start retransmit task",
		"conn_id", conn.ID(),
		"message_id", msg.MessageId,
		"retry_interval", m.retryInterval,
		"max_retry_cnt", m.maxRetryCnt,
	)
}

// Stop 停止指定消息的重传任务。
func (m *Manager) Stop(connID, messageID string) {
	key := m.taskKey(connID, messageID)
	if task, ok := m.tasks.Load(key); ok {
		_ = m.stopAndDelete(key)

		slog.Debug(
			"[retransmit-manager] successfully stop retransmit task",
			"conn_id", connID,
			"message_id", messageID,
			"retransmit_count", task.retransmitCnt.Load(),
		)
	}
}

func (m *Manager) taskKey(connID, messageID string) string {
	return fmt.Sprintf("%s:%s", connID, messageID)
}

func (m *Manager) TotalTaskCnt() int64 {
	return m.totalTaskCnt.Load()
}

// ClearByConn 清除指定连接的重传任务。
func (m *Manager) ClearByConn(connID string) {
	var cnt int
	m.tasks.Range(func(key string, task *Task) bool {
		if task.conn.ID() == connID {
			if m.stopAndDelete(key) {
				cnt++
			}
		}
		return true
	})

	if cnt > 0 {
		slog.Info(
			"[retransmit-manager] successfully clear retransmit tasks by connection",
			"conn_id", connID,
			"task_cleared_cnt", cnt,
		)
	}
}

func (m *Manager) stopAndDelete(key string) bool {
	if task, ok := m.tasks.LoadAndDelete(key); ok {
		task.stop()
		m.totalTaskCnt.Add(-1)
		return true
	}
	return false
}

// Close 关闭重传管理器。
func (m *Manager) Close() {
	if !m.closed.CompareAndSwap(false, true) {
		return
	}

	var cnt int
	m.tasks.Range(func(key string, _ *Task) bool {
		if m.stopAndDelete(key) {
			cnt++
		}
		return true
	})

	slog.Info(
		"[retransmit-manager] retransmit manager closed",
		"task_cleared_cnt", cnt,
	)
}
