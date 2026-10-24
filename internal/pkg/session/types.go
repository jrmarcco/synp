package session

import (
	"context"
	"fmt"
)

//go:generate mockgen -source=types.go -destination=mock/session.mock.go -package=sessionmock -typed Session

type Session interface {
	User() User

	Set(ctx context.Context, key, val string) error
	Get(ctx context.Context, key string) (string, error)

	Destroy(ctx context.Context) error
}

// Builder 为 Session 的构建器。
type Builder interface {
	// Build 新建一个 Session 或 返回一个已存在的 Session。
	// bool 参数表示返回的 Session 是否是新建的。
	Build(ctx context.Context, user User) (Session, bool, error)
}

// Device 设备类型
type Device string

const (
	DeviceMobile  Device = "mobile"
	DeviceTablet  Device = "tablet"
	DevicePC      Device = "pc"
	DeviceUnknown Device = "unknown"
)

// User 为 Session 的用户信息。
type User struct {
	BID uint64 `json:"bid"`
	UID uint64 `json:"uid"`

	// SessionID 为业务系统内的会话 ID ( 来自 JWT claim session_id，
	// 注意不是网关连接 ID )。
	SessionID string `json:"session_id"`

	Device    Device `json:"device"`    // 设备类型：mobile/tablet/pc
	AutoClose bool   `json:"autoClose"` // 空闲时是否自动关闭连接
}

func (u *User) ConnKey() string {
	return fmt.Sprintf("%d:%d", u.BID, u.UID)
}

func (u *User) SessionKey() string {
	return fmt.Sprintf("synp:session:%d:%d", u.BID, u.UID)
}

// AnonymousSession 是未完成 AUTH 鉴权的连接使用的占位 Session。
// 连接建立时身份未知，只有完成首帧鉴权后才会被替换为真实 Session。
type AnonymousSession struct {
	user User
}

var _ Session = (*AnonymousSession)(nil)

func NewAnonymousSession(user User) *AnonymousSession {
	return &AnonymousSession{user: user}
}

func (s *AnonymousSession) User() User {
	return s.user
}

func (s *AnonymousSession) Set(_ context.Context, _, _ string) error {
	return nil
}

func (s *AnonymousSession) Get(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (s *AnonymousSession) Destroy(_ context.Context) error {
	return nil
}
