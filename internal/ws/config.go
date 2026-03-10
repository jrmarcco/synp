package ws

import (
	"flag"
	"fmt"
)

const (
	defaultHost = "0.0.0.0"
	defaultPort = 10071
)

// Config 为 WebSocket 的相关配置。
type Config struct {
	Host            string // IP 地址，默认 0.0.0.0
	Port            int    // 端口号，默认 10071
	Network         string // 网络协议，默认 tcp4
	EnableLocalMode bool   // 是自动获取 IP 地
}

func DefaultConfig() *Config {
	var host string
	var port int

	flag.StringVar(&host, "host", defaultHost, "WebSocket gateway host address")
	flag.IntVar(&port, "port", defaultPort, "WebSocket gateway port")

	return &Config{
		Host:    host,
		Port:    port,
		Network: "tcp4",
	}
}

func (cfg Config) Address() string {
	if cfg.Network == "unix" {
		// 如果是 unix，
		// 那么启动方式为 unix domain socket，
		// Host 为 file。
		return cfg.Host
	}

	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}
