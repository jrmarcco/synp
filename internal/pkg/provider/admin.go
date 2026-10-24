package provider

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

// defaultAdminAddr 为管理端口默认地址。
const defaultAdminAddr = ":9090"

// AdminFxModule 提供运维管理端口:
//
//	GET /metrics       Prometheus 指标
//	GET /debug/pprof/*  Go pprof
//
// 管理端口与业务端口隔离，避免 pprof 与指标抓取影响数据面。
var AdminFxModule = fx.Module(
	"admin",
	fx.Invoke(startAdminServer),
)

func startAdminServer(connManager synp.ConnManager, lifecycle fx.Lifecycle) error {
	type config struct {
		Addr string `mapstructure:"addr"`
	}

	cfg := config{}
	if err := viper.UnmarshalKey("synp.admin", &cfg); err != nil {
		return err
	}

	addr := cfg.Addr
	if addr == "" {
		addr = defaultAdminAddr
	}

	// 连接统计指标在抓取时向 ConnManager 即时求值。
	metrics.InitConnStats(connManager.ConnCount, connManager.UserCount)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// pprof 端点 ( 标准库默认路径 )。
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger := logging.For("admin")

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			lc := net.ListenConfig{}
			ln, err := lc.Listen(ctx, "tcp", addr)
			if err != nil {
				return fmt.Errorf("failed to listen admin server: %w", err)
			}

			go func() {
				if serveErr := server.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
					logger.Error("[admin] admin server exited with error", "error", serveErr)
				}
			}()

			logger.Info("[admin] admin server started", "addr", addr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			if err := server.Shutdown(shutdownCtx); err != nil {
				logger.Warn("[admin] failed to shutdown admin server", "error", err)
			}
			return nil
		},
	})
	return nil
}
