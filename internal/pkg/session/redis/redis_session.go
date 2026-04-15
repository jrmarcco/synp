package redis

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/session_create.lua
var sessionCreateLua string

var (
	ErrSessionExists  = errors.New("session exists")
	ErrSessionCreate  = errors.New("failed to create session")
	ErrSessionDestroy = errors.New("failed to destroy session")
)

var _ session.Session = (*Session)(nil)

// Session 为 Session 的 Redis 实现。
type Session struct {
	rdb redis.Cmdable

	key  string
	user session.User
}

func (s *Session) User() session.User {
	return s.user
}

func (s *Session) Set(ctx context.Context, key, val string) error {
	// HSet 的 value 参数可以是 any 类型，go-redis 会自动转换为 string。
	//
	// 注：
	//  传入结构体的时，value 会被 go-redis 序列化成一种默认的字符串格式，反序列化时会出现问题。
	//  最好自己序列化成 string 再传入。
	return s.rdb.HSet(ctx, s.key, key, val).Err()
}

func (s *Session) Get(ctx context.Context, key string) (string, error) {
	str, err := s.rdb.HGet(ctx, s.key, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return str, nil
}

func (s *Session) Destroy(ctx context.Context) error {
	if err := s.rdb.Del(ctx, s.key).Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrSessionDestroy, err)
	}
	return nil
}

// saveToRedis 将 Session 保存到 redis 中。
// 如果 Session 已存在，则返回 ErrSessionExists。
func (s *Session) saveToRedis(ctx context.Context) error {
	args := []any{
		"sign_in_time",
		time.Now().Format(time.RFC3339Nano),
	}

	// 使用 lua 脚本保证原子性。
	res, err := s.rdb.Eval(
		ctx,
		sessionCreateLua,
		[]string{s.key},
		args...,
	).Result()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSessionCreate, err)
	}

	if res == "ok" {
		return nil
	}

	return ErrSessionExists
}

func newSession(rdb redis.Cmdable, user session.User) *Session {
	return &Session{
		rdb:  rdb,
		key:  user.SessionKey(),
		user: user,
	}
}

var _ session.Builder = (*SessionBuilder)(nil)

type SessionBuilder struct {
	rdb redis.Cmdable
}

func (b *SessionBuilder) Build(ctx context.Context, user session.User) (session.Session, bool, error) {
	sess := newSession(b.rdb, user)

	var err error
	if err = sess.saveToRedis(ctx); err == nil {
		return sess, true, nil
	}

	if errors.Is(err, ErrSessionExists) {
		return sess, false, nil
	}

	return nil, false, err
}

func NewSessionBuilder(rdb redis.Cmdable) *SessionBuilder {
	return &SessionBuilder{rdb: rdb}
}
