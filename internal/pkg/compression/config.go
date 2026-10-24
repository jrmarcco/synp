package compression

import "github.com/gobwas/ws/wsflate"

// Config 为压缩配置。
type Config struct {
	Enabled                 bool
	ServerMaxWindowBits     int  // 服务端压缩时使用的滑动窗口大小 取值范围: 8-15 = 2^8 - 2^15 = 256B - 32KB
	ServerNoContextTakeover bool // 服务器不保留压缩上下文 ( 默认 false )
	ClientMaxWindowBits     int  // 客户端压缩时使用的滑动窗口大小 取值范围: 8-15 = 2^8 - 2^15 = 256B - 32KB
	ClientNoContextTakeover bool // 客户端不保留压缩上下文 ( 默认 false )
	Level                   int  // 压缩级别 取值范围: 1-9 = 快速压缩 - 最大压缩 ( 建议取中位数 4~6 )

	// MinCompressSize 为启用压缩的最小 payload 大小 ( 默认 256B )，
	// 更小的消息走未压缩路径。
	MinCompressSize int
}

// ToParamters 将 Config 转换为 wsflate 参数。
func (cfg *Config) ToParamters() wsflate.Parameters {
	return wsflate.Parameters{
		ServerMaxWindowBits:     wsflate.WindowBits(cfg.ServerMaxWindowBits),
		ServerNoContextTakeover: cfg.ServerNoContextTakeover,
		ClientMaxWindowBits:     wsflate.WindowBits(cfg.ClientMaxWindowBits),
		ClientNoContextTakeover: cfg.ClientNoContextTakeover,
	}
}

// State 为压缩状态，包含协商后的扩展信息和压缩参数。
type State struct {
	Enabled bool

	Ext    *wsflate.Extension
	Params wsflate.Parameters

	// Level 为压缩级别 ( 1~9 )，传递给出站压缩器。
	Level int

	// MinCompressSize 为启用压缩的最小 payload 大小，
	// 更小的消息走未压缩路径 ( 压缩收益低于开销 )。
	MinCompressSize int
}

// DefaultMinCompressSize 为小消息压缩阈值的默认值 ( 256B )。
const DefaultMinCompressSize = 256
