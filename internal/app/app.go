package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/xgrpc/register"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type app struct {
	mu sync.Mutex

	grpcServer          *grpc.Server
	grpcListener        net.Listener
	grpcServiceInstance register.ServiceInstance

	grpcRegistry        register.Registry
	grpcRegisterTimeout time.Duration

	webSocketServer synp.Server

	logger *zap.Logger
}

func (app *app) Start(ctx context.Context) (err error) {
	grpcStarted := false
	grpcRegistered := false
	webSocketStarted := false

	defer func() {
		if err == nil {
			return
		}
		if webSocketStarted {
			if stopErr := app.webSocketServer.GracefulShutdown(); stopErr != nil {
				app.logger.Warn("[app] failed to rollback ws server", zap.Error(stopErr))
			}
		}
		if grpcRegistered {
			if unregisterErr := app.unregisterGrpc(ctx); unregisterErr != nil {
				app.logger.Warn("[app] failed to rollback grpc registration", zap.Error(unregisterErr))
			}
		}
		if grpcStarted {
			if stopErr := app.stopGrpc(); stopErr != nil {
				app.logger.Warn("[app] failed to rollback grpc server", zap.Error(stopErr))
			}
		}
	}()

	if err := app.startGrpc(ctx); err != nil {
		return err
	}
	grpcStarted = true

	if err := app.registerGrpc(ctx); err != nil {
		return err
	}
	grpcRegistered = true

	if err := app.startWebSocket(); err != nil {
		return err
	}
	webSocketStarted = true
	return nil
}

func (app *app) Stop(ctx context.Context) error {
	var errs []error

	if err := app.webSocketServer.GracefulShutdown(); err != nil {
		errs = append(errs, fmt.Errorf("[app] failed to stop ws server: %w", err))
	}

	if err := app.unregisterGrpc(ctx); err != nil {
		errs = append(errs, fmt.Errorf("[app] failed to unregister grpc service: %w", err))
	}

	if err := app.stopGrpc(); err != nil {
		errs = append(errs, fmt.Errorf("[app] failed to stop grpc server: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (app *app) startGrpc(ctx context.Context) error {
	if app.grpcServer == nil {
		return errors.New("[app] grpc server is nil")
	}

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:5051")
	if err != nil {
		return fmt.Errorf("[app] failed to listen grpc: %w", err)
	}

	app.mu.Lock()
	app.grpcListener = ln
	app.mu.Unlock()

	go func() {
		if serveErr := app.grpcServer.Serve(ln); serveErr != nil && !isExpectedGRPCServeErr(serveErr) {
			app.logger.Error("[app] grpc serve exited with error", zap.Error(serveErr))
		}
	}()

	app.logger.Info("[app] grpc server started")
	return nil
}

func (app *app) stopGrpc() error {
	if app.grpcServer == nil {
		return nil
	}

	app.grpcServer.GracefulStop()

	app.mu.Lock()
	defer app.mu.Unlock()

	if app.grpcListener != nil {
		if err := app.grpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
		app.grpcListener = nil
	}
	return nil
}

func (app *app) registerGrpc(ctx context.Context) error {
	if app.grpcRegistry == nil {
		return nil
	}

	registerCtx, cancel := withOptionalTimeout(ctx, app.grpcRegisterTimeout)
	defer cancel()

	if err := app.grpcRegistry.Register(registerCtx, app.grpcServiceInstance); err != nil {
		return err
	}

	app.logger.Info("[app] grpc service registered", zap.Any("service_instance", app.grpcServiceInstance))
	return nil
}

func (app *app) unregisterGrpc(ctx context.Context) error {
	if app.grpcRegistry == nil {
		return nil
	}

	unregisterCtx, cancel := withOptionalTimeout(ctx, app.grpcRegisterTimeout)
	defer cancel()

	return app.grpcRegistry.Unregister(unregisterCtx, app.grpcServiceInstance)
}

func (app *app) startWebSocket() error {
	if err := app.webSocketServer.Start(); err != nil {
		return fmt.Errorf("[app] failed to start ws server: %w", err)
	}
	app.logger.Info("[app] ws server started")
	return nil
}

func withOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func isExpectedGRPCServeErr(err error) bool {
	if errors.Is(err, grpc.ErrServerStopped) {
		return true
	}
	// grpc 服务停止时，底层 listener 关闭会返回这一类错误。
	return strings.Contains(err.Error(), "use of closed network connection")
}
