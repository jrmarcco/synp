package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/buildinfo"
	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/synp/internal/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"google.golang.org/grpc/connectivity"
)

// defaultAdminAddr 为管理端口默认地址。
const defaultAdminAddr = ":9090"

// readinessCacheTTL 为就绪检查结果的缓存时长，
// 防止探针高频打爆依赖 ( 结果最多延迟 5s )。
const readinessCacheTTL = 5 * time.Second

// 就绪/存活状态值。
const (
	statusOK       = "ok"
	statusDegraded = "degraded"
)

// AdminFxModule 提供运维管理端口:
//
//	GET /healthz          存活探针 ( 进程可用 )
//	GET /readyz           就绪探针 ( 依赖健康，结果缓存 5s )
//	GET /version          构建版本信息
//	GET /metrics          Prometheus 指标
//	GET /debug/pprof/*    Go pprof
//
// 管理端口与业务端口隔离，避免 pprof 与指标抓取影响数据面。
var AdminFxModule = fx.Module(
	"admin",
	fx.Invoke(startAdminServer),
)

func startAdminServer(connManager synp.ConnManager, rdb redis.Cmdable, etcdClient *clientv3.Client, lifecycle fx.Lifecycle) error {
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
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", newReadinessHandler(rdb, etcdClient))
	mux.HandleFunc("/version", handleVersion)
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

// handleHealthz 存活探针: 进程与 admin 端口可用即返回 200。
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": statusOK})
}

// handleVersion 返回构建版本信息。
func handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version": buildinfo.Version,
		"commit":  buildinfo.Commit,
		"date":    buildinfo.Date,
	})
}

// newReadinessHandler 创建就绪探针。
// 检查 Redis 与 etcd 连通性，结果缓存 readinessCacheTTL，
// 任一依赖不可用返回 503 与分项状态。
func newReadinessHandler(rdb redis.Cmdable, etcdClient *clientv3.Client) http.HandlerFunc {
	var (
		mu        sync.Mutex
		cachedAt  time.Time
		cachedRes readinessResult
	)

	check := func() readinessResult {
		res := readinessResult{
			Status:  statusOK,
			Checks:  map[string]string{"redis": statusOK, "etcd": statusOK},
			Checked: time.Now().Format(time.RFC3339),
		}

		// Redis: 1s 超时 PING ( 未配置时直接标记降级 )。
		if rdb == nil {
			res.Status = statusDegraded
			res.Checks["redis"] = "not configured"
		} else {
			pingCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := rdb.Ping(pingCtx).Err(); err != nil {
				res.Status = statusDegraded
				res.Checks["redis"] = err.Error()
			}
		}

		// etcd: gRPC 连接状态 ( Ready/Idle 视为健康，惰性连接 Idle 是正常态 )。
		if etcdClient == nil {
			res.Status = statusDegraded
			res.Checks["etcd"] = "not configured"
		} else if cc := etcdClient.ActiveConnection(); cc == nil {
			res.Status = statusDegraded
			res.Checks["etcd"] = "no active connection"
		} else if state := cc.GetState(); state != connectivity.Ready && state != connectivity.Idle {
			res.Status = statusDegraded
			res.Checks["etcd"] = state.String()
		}

		return res
	}

	return func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		cached := !cachedAt.IsZero() && time.Since(cachedAt) < readinessCacheTTL
		result := cachedRes
		mu.Unlock()

		if !cached {
			result = check()
			mu.Lock()
			cachedRes, cachedAt = result, time.Now()
			mu.Unlock()
		}

		status := http.StatusOK
		if result.Status != statusOK {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, result)
	}
}

type readinessResult struct {
	Status  string            `json:"status"`
	Checks  map[string]string `json:"checks"`
	Checked string            `json:"checked"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
