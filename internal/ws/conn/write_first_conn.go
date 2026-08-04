package conn

// WriteFirstConn 代表一个 WebSocket 连接，封装了 net.Conn。
// 使用单个 goroutine 同时负责读写操作。
// 与 ReadWriteConn 不同的是，WriteFirstConn 会优先保证写操作。
type WriteFirstConn struct{}
