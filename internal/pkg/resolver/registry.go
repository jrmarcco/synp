// Package resolver 提供基于 register.Registry 的 gRPC 客户端名称解析器。
//
// xgrpc 只提供服务注册端 ( register.Registry )，
// 客户端服务发现通过本包的 resolver 桥接：
// target 为服务名，地址列表来自 registry.ListServices，
// 变更由 registry.Subscribe 通知式订阅驱动。
package resolver

import (
	"context"

	"github.com/jrmarcco/synp/internal/pkg/logging"
	"github.com/jrmarcco/xgrpc/register"
	"google.golang.org/grpc/resolver"
)

const Scheme = "synp"

var (
	_ resolver.Builder  = (*RegistryResolverBuilder)(nil)
	_ resolver.Resolver = (*registryResolver)(nil)
)

// RegistryResolverBuilder 基于 register.Registry 的 resolver builder。
type RegistryResolverBuilder struct {
	registry register.Registry
}

func NewRegistryResolverBuilder(registry register.Registry) *RegistryResolverBuilder {
	return &RegistryResolverBuilder{
		registry: registry,
	}
}

func (b *RegistryResolverBuilder) Scheme() string {
	return Scheme
}

// Build 返回 target.Endpoint() 对应服务的 resolver。
// 连接地址: synp:///<service_name>。
func (b *RegistryResolverBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	_ resolver.BuildOptions,
) (resolver.Resolver, error) {
	serviceName := target.Endpoint()

	ctx, cancel := context.WithCancel(context.Background())
	r := &registryResolver{
		registry:    b.registry,
		clientConn:  cc,
		serviceName: serviceName,

		ctx:    ctx,
		cancel: cancel,
	}

	// 解析一次初始快照后再进入订阅循环。
	r.resolve()

	go r.watch()

	return r, nil
}

type registryResolver struct {
	registry    register.Registry
	clientConn  resolver.ClientConn
	serviceName string

	ctx    context.Context
	cancel context.CancelFunc
}

// watch 订阅服务实例变更，每次通知后重新拉取全量快照。
func (r *registryResolver) watch() {
	// Subscribe 返回通知式 channel，channel 关闭表示订阅不可用。
	for range r.registry.Subscribe(r.serviceName) {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		r.resolve()
	}
}

// resolve 拉取服务实例全量快照并更新连接状态。
func (r *registryResolver) resolve() {
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	instances, err := r.registry.ListServices(ctx, r.serviceName)
	if err != nil {
		logging.For("resolver").Error(
			"failed to list services",
			"service_name", r.serviceName,
			"error", err,
		)
		return
	}

	addresses := make([]resolver.Address, 0, len(instances))
	for _, instance := range instances {
		if instance.Addr == "" {
			continue
		}
		addresses = append(addresses, resolver.Address{Addr: instance.Addr})
	}

	if err = r.clientConn.UpdateState(resolver.State{Addresses: addresses}); err != nil {
		logging.For("resolver").Error(
			"failed to update state",
			"service_name", r.serviceName,
			"instance_cnt", len(addresses),
			"error", err,
		)
	}
}

func (r *registryResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	r.resolve()
}

func (r *registryResolver) Close() {
	r.cancel()
}
