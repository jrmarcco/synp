# 迁移指南：从双Kafka到单Kafka+gRPC

## 迁移步骤

### 第一阶段：准备工作

#### 1. 添加网关节点标识配置

在网关配置中添加节点ID：

```yaml
# config/config.yaml
gateway:
  node_id: "${GATEWAY_NODE_ID}"  # 环境变量，如 gateway-1, gateway-2
  grpc:
    port: 50051
    enable: true
```

#### 2. 初始化连接注册表

在依赖注入中添加 `ConnRegistry`：

```go
// internal/pkg/provider/registry.go
package provider

import (
	"github.com/jrmarcco/synp/internal/pkg/registry"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func newConnRegistry(rdb redis.Cmdable) registry.ConnRegistry {
	gatewayNode := viper.GetString("gateway.node_id")
	if gatewayNode == "" {
		panic("gateway.node_id is required")
	}
	return registry.NewRedisConnRegistry(rdb, gatewayNode)
}

var RegistryModule = fx.Module(
	"registry",
	fx.Provide(newConnRegistry),
)
```

#### 3. 更新 Lifecycle Handler 依赖

```go
// internal/pkg/provider/lifecycle.go
func newLifecycleHandler(
	rdb redis.Cmdable,
	codec codec.Codec,
	dMsgHandler downstream.DMsgHandler,
	uMsgHandlers []upstream.UMsgHandler,
	connRegistry registry.ConnRegistry, // 新增
	logger *zap.Logger,
) *lifecycle.Handler {
	return lifecycle.NewHandler(
		rdb,
		viper.GetDuration("cache.request_timeout")*time.Millisecond,
		viper.GetDuration("cache.expiration")*time.Millisecond,
		codec,
		dMsgHandler,
		uMsgHandlers,
		connRegistry, // 新增
		logger,
	)
}
```

### 第二阶段：添加gRPC服务

#### 1. 定义 Protobuf

创建 `api/proto/gateway/v1/gateway.proto`：

```protobuf
syntax = "proto3";

package gateway.v1;

option go_package = "github.com/jrmarcco/synp-api/api/go/gateway/v1;gatewayv1";

service GatewayService {
  rpc PushMessage(PushMessageRequest) returns (PushMessageResponse);
  rpc BatchPushMessage(BatchPushMessageRequest) returns (BatchPushMessageResponse);
}

message PushMessageRequest {
  uint64 bid = 1;
  uint64 uid = 2;
  string message_id = 3;
  bytes body = 4;
  bool need_retry = 5;
}

message PushMessageResponse {
  bool success = 1;
  string reason = 2;
  int32 online_count = 3;
}

message BatchPushMessageRequest {
  repeated PushMessageRequest messages = 1;
}

message BatchPushMessageResponse {
  repeated PushMessageResponse results = 1;
}
```

生成代码：

```bash
buf generate
```

#### 2. 实现 gRPC 服务

使用已创建的 `internal/grpc/gateway_service.go`，更新为使用生成的代码：

```go
// internal/grpc/gateway_service.go
package grpc

import (
	"context"
	"log/slog"

	gatewayv1 "github.com/jrmarcco/synp-api/api/go/gateway/v1"
	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
	"github.com/jrmarcco/synp"
	"github.com/jrmarcco/synp/internal/pkg/message"
	"github.com/jrmarcco/synp/internal/pkg/retransmit"
	"github.com/jrmarcco/synp/internal/pkg/session"
)

var _ gatewayv1.GatewayServiceServer = (*GatewayService)(nil)

type GatewayService struct {
	gatewayv1.UnimplementedGatewayServiceServer
	
	connManager       synp.ConnManager
	pushFunc          message.PushFunc
	retransmitManager *retransmit.Manager
}

// ... 实现 PushMessage 和 BatchPushMessage 方法 ...
```

#### 3. 启动 gRPC 服务器

```go
// internal/grpc/server.go
package grpc

import (
	"fmt"
	"net"

	gatewayv1 "github.com/jrmarcco/synp-api/api/go/gateway/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	port           int
	gatewayService *GatewayService
	grpcServer     *grpc.Server
	logger         *zap.Logger
}

func NewServer(
	port int,
	gatewayService *GatewayService,
	logger *zap.Logger,
) *Server {
	return &Server{
		port:           port,
		gatewayService: gatewayService,
		logger:         logger,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.grpcServer = grpc.NewServer()
	gatewayv1.RegisterGatewayServiceServer(s.grpcServer, s.gatewayService)

	s.logger.Info(
		"[grpc] gRPC server started",
		zap.Int("port", s.port),
	)

	return s.grpcServer.Serve(listener)
}

func (s *Server) Stop() error {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	return nil
}
```

### 第三阶段：删除旧的Kafka消费逻辑

#### 1. 删除 backend Kafka 消费者

在 `internal/ws/server.go` 中删除：

```go
// 删除这个函数
func (s *Server) consumeFromBackend(ctx context.Context, msg *xmq.Message) error {
	// ...
}

// 删除 Kafka 消费者初始化逻辑
```

#### 2. 清理不需要的配置

删除或注释掉配置文件中的：

```yaml
# 不再需要
kafka:
  consumer:
    backend_topic: "synp.backend.push"
```

### 第四阶段：后端集成

#### 1. 后端添加依赖

```go
import (
	gatewayv1 "github.com/jrmarcco/synp-api/api/go/gateway/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)
```

#### 2. 后端实现推送逻辑

参考 `docs/backend_integration_example.go`

#### 3. 配置网关节点列表

```yaml
# backend config
gateways:
  - name: "gateway-1"
    addr: "gateway-1:50051"
  - name: "gateway-2"
    addr: "gateway-2:50051"
```

## 测试计划

### 单元测试

```go
func TestConnRegistry(t *testing.T) {
	// 测试注册、注销、查询功能
}

func TestGatewayService(t *testing.T) {
	// 测试 gRPC 推送功能
}
```

### 集成测试

1. **单网关测试**：启动单个网关，测试消息推送
2. **多网关测试**：启动多个网关，测试分布式路由
3. **故障测试**：模拟网关crash，测试连接迁移

### 性能测试

对比新旧架构的延迟：

```bash
# 测试消息端到端延迟
# 旧架构：200-600ms
# 新架构：105-320ms
```

## 回滚方案

如果遇到问题，可以快速回滚：

1. 重新启用 Kafka 消费者
2. 临时关闭 gRPC 服务
3. 恢复旧版本配置

## 监控告警

添加新的监控指标：

1. `synp_conn_registry_ops_total{op="register|unregister|find"}`
2. `synp_grpc_push_duration_seconds`
3. `synp_grpc_push_errors_total`

## 常见问题

### Q1: Redis连接注册失败怎么办？

A: 连接注册失败不影响WebSocket连接建立，只会导致分布式路由失效。可以通过监控告警及时发现。

### Q2: 网关节点ID如何生成？

A: 可以使用：
- Kubernetes pod name
- 服务器hostname
- IP地址
- 手动配置的唯一标识

### Q3: 如何处理网关扩缩容？

A: 
- 扩容：新网关启动后自动注册，后端自动发现
- 缩容：网关下线前优雅关闭连接，Redis中的注册会自动过期

### Q4: 后端如何发现新的网关节点？

A: 可以使用：
- 静态配置（简单）
- 服务发现（etcd/consul）
- K8s Service/Endpoint
