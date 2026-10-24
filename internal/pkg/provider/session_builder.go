package provider

import (
	"github.com/jrmarcco/synp/internal/pkg/session"
	sr "github.com/jrmarcco/synp/internal/pkg/session/redis"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
)

// SessionBuilderFxModule 提供 session 构建器。
// 连接完成首帧 AUTH 鉴权后由 AuthMsgHandler 构建 session。
var SessionBuilderFxModule = fx.Module(
	"session-builder",
	fx.Provide(
		fx.Annotate(
			newSessionBuilder,
			fx.As(new(session.Builder)),
		),
	),
)

func newSessionBuilder(rdb redis.Cmdable) session.Builder {
	return sr.NewSessionBuilder(rdb)
}
