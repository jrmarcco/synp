package conn

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/stretchr/testify/require"
)

// drainPipe 持续读空 pipe 对端，
// 避免连接关闭时写 close frame 阻塞到写超时拖慢测试。
func drainPipe(conn net.Conn) {
	go func() {
		buf := make([]byte, 1024)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
}

// newTestConnPair 创建注册进 ConnManager 的测试连接 ( 基于 net.Pipe )。
func newTestConn(t *testing.T, m *ConnManager) synp.Conn {
	t.Helper()

	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { _ = clientSide.Close() })
	drainPipe(clientSide)

	sess := session.NewAnonymousSession(session.User{Device: session.DevicePC})
	conn, err := m.NewConn(context.Background(), serverSide, sess, nil)
	require.NoError(t, err)

	return conn
}

func TestConnManagerLifecycle(t *testing.T) {
	t.Parallel()

	m := NewConnManager()

	conn := newTestConn(t, m)
	require.EqualValues(t, 1, m.ConnCount(), "创建连接后计数应为 1 ( 含未鉴权连接 )")

	user := session.User{BID: 1, UID: 100, Device: session.DevicePC}
	require.NoError(t, m.BindUser(conn, user))

	// BindUser 只移动归属，不新增计数。
	require.EqualValues(t, 1, m.ConnCount())
	require.EqualValues(t, 1, m.UserCount())

	conns, ok := m.FindUserConn(user)
	require.True(t, ok)
	require.Len(t, conns, 1)

	found, ok := m.FindConn(user)
	require.True(t, ok)
	require.Same(t, conn, found)

	require.True(t, m.RemoveConnByID(conn.ID()))
	require.EqualValues(t, 0, m.ConnCount(), "断开后计数应归零")
	require.EqualValues(t, 0, m.UserCount(), "用户连接清空后用户数应归零")
}

func TestConnManagerConnCountNoLeakAcrossCycles(t *testing.T) {
	t.Parallel()

	m := NewConnManager()

	// 多轮 创建 -> 绑定 -> 断开，验证计数无净泄漏。
	const cycles = 100
	for range cycles {
		conn := newTestConn(t, m)
		require.NoError(t, m.BindUser(conn, session.User{BID: 1, UID: 100, Device: session.DevicePC}))
		require.True(t, m.RemoveConnByID(conn.ID()))
	}

	require.EqualValues(t, 0, m.ConnCount())
	require.EqualValues(t, 0, m.UserCount())
}

func TestConnManagerKickOldConnOnRebind(t *testing.T) {
	t.Parallel()

	m := NewConnManager()
	user := session.User{BID: 1, UID: 100, Device: session.DevicePC}

	oldConn := newTestConn(t, m)
	require.NoError(t, m.BindUser(oldConn, user))

	// 同用户同设备重连: 顶号关闭旧连接。
	newConn := newTestConn(t, m)
	require.NoError(t, m.BindUser(newConn, user))

	conns, ok := m.FindUserConn(user)
	require.True(t, ok)
	require.Len(t, conns, 1)
	require.Same(t, newConn, conns[0], "重连后应保留新连接")

	// 旧连接已关闭。
	select {
	case <-oldConn.Closed():
	case <-func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch)
		return ch
	}():
	}

	require.EqualValues(t, 1, m.ConnCount())
	require.True(t, m.RemoveConnByID(newConn.ID()))
	require.EqualValues(t, 0, m.ConnCount())
}

func TestConnManagerMultiDeviceUnbind(t *testing.T) {
	t.Parallel()

	m := NewConnManager()
	user := session.User{BID: 1, UID: 100}

	pc := newTestConn(t, m)
	require.NoError(t, m.BindUser(pc, session.User{BID: user.BID, UID: user.UID, Device: session.DevicePC}))

	mobile := newTestConn(t, m)
	require.NoError(t, m.BindUser(mobile, session.User{BID: user.BID, UID: user.UID, Device: session.DeviceMobile}))

	require.EqualValues(t, 2, m.ConnCount())
	require.EqualValues(t, 1, m.UserCount())

	// 断开一个设备，另一个设备仍在。
	require.True(t, m.RemoveConnByID(pc.ID()))
	require.EqualValues(t, 1, m.ConnCount())
	require.EqualValues(t, 1, m.UserCount())

	conns, ok := m.FindUserConn(user)
	require.True(t, ok)
	require.Len(t, conns, 1)

	require.True(t, m.RemoveConnByID(mobile.ID()))
	require.EqualValues(t, 0, m.ConnCount())
	require.EqualValues(t, 0, m.UserCount())
}

// TestConnManagerConcurrentStorm 并发连接风暴:
// 多 goroutine 同时创建、绑定、断开同用户多设备连接，
// 配合 go test -race 验证无数据竞争、计数最终一致。
func TestConnManagerConcurrentStorm(t *testing.T) {
	t.Parallel()

	m := NewConnManager()

	const (
		goroutines = 32
		rounds     = 20
	)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			for r := range rounds {
				// 同一用户交替使用 PC / Mobile 设备，制造顶号与多设备并发窗口。
				device := session.DevicePC
				if (g+r)%2 == 0 {
					device = session.DeviceMobile
				}

				clientSide, serverSide := net.Pipe()
				drainPipe(clientSide)

				sess := session.NewAnonymousSession(session.User{Device: device})
				conn, err := m.NewConn(context.Background(), serverSide, sess, nil)
				if err != nil {
					t.Errorf("NewConn: %v", err)
					_ = clientSide.Close()
					return
				}

				if err = m.BindUser(conn, session.User{BID: 1, UID: 100, Device: device}); err != nil {
					t.Errorf("BindUser: %v", err)
				}

				m.RemoveConnByID(conn.ID())
				_ = clientSide.Close()

				// 匿名断开路径 ( 未绑定即断开 )。
				anonClient, anonServer := net.Pipe()
				drainPipe(anonClient)
				anonSess := session.NewAnonymousSession(session.User{Device: device})
				anonConn, err := m.NewConn(context.Background(), anonServer, anonSess, nil)
				if err != nil {
					t.Errorf("NewConn anon: %v", err)
					_ = anonClient.Close()
					return
				}
				m.RemoveConnByID(anonConn.ID())
				_ = anonClient.Close()
			}
		}(g)
	}

	wg.Wait()

	require.EqualValues(t, 0, m.ConnCount(), fmt.Sprintf("并发风暴后连接计数应归零, 实际 %d", m.ConnCount()))
	require.EqualValues(t, 0, m.UserCount(), fmt.Sprintf("并发风暴后用户计数应归零, 实际 %d", m.UserCount()))
}
