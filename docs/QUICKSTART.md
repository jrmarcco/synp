# 快速开始：启用分布式网关路由

## ⚡ 5分钟上手

### 步骤1：添加配置

复制配置文件并修改：

```bash
cp config/gateway.yaml.example config/gateway.yaml
```

编辑 `config/gateway.yaml`：

```yaml
gateway:
  node_id: "gateway-1"  # 修改为你的网关节点标识
  grpc:
    enable: true
    port: 50051
```

在 `cmd/main.go` 的 `loadConfig` 函数中添加：

```go
func loadConfig() error {
	viper.AddConfigPath("config")
	viper.SetConfigType("yaml")

	viper.SetConfigName("base")
	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read base config: %w", err)
	}

	// 添加 gateway 到配置列表
	subConfigNames := []string{"redis", "kafka", "gateway"}  // 添加 "gateway"
	for _, subConfigName := range subConfigNames {
		viper.SetConfigName(subConfigName)
		if err := viper.MergeInConfig(); err != nil {
			return fmt.Errorf("failed to merge %s config: %w", subConfigName, err)
		}
	}

	return nil
}
```

### 步骤2：启动网关

```bash
# 启动网关 1
GATEWAY_NODE_ID=gateway-1 go run cmd/main.go

# 启动网关 2（另一个终端）
GATEWAY_NODE_ID=gateway-2 go run cmd/main.go
```

### 步骤3：验证功能

连接网关后，检查 Redis 中的连接注册：

```bash
redis-cli
> SMEMBERS synp:conn_registry:1:123  # bid=1, uid=123
1) "gateway-1"
```

## 📝 详细说明

### 配置说明

| 配置项 | 说明 | 必填 | 示例 |
|--------|------|------|------|
| `gateway.node_id` | 网关节点唯一标识 | 是 | `gateway-1` |
| `gateway.grpc.enable` | 是否启用gRPC服务 | 否 | `true` |
| `gateway.grpc.port` | gRPC监听端口 | 否 | `50051` |

### 网关节点标识选择

#### 开发环境
```yaml
gateway:
  node_id: "gateway-1"  # 手动指定
```

#### Kubernetes部署
```yaml
gateway:
  node_id: "${POD_NAME}"  # 使用 pod name
```

Deployment 配置：
```yaml
env:
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: GATEWAY_NODE_ID
  value: "$(POD_NAME)"
```

#### Docker部署
```bash
docker run \
  -e GATEWAY_NODE_ID=$(hostname) \
  your-gateway-image
```

### Redis数据结构

网关会在 Redis 中维护以下数据：

```
Key: synp:conn_registry:{bid}:{uid}
Type: Set
Value: ["gateway-1", "gateway-2"]
TTL: 2小时
```

示例：
```bash
# 用户上线
SADD synp:conn_registry:1:123 "gateway-1"

# 用户下线
SREM synp:conn_registry:1:123 "gateway-1"

# 查询用户所在网关
SMEMBERS synp:conn_registry:1:123
```

## 🧪 测试

### 测试连接注册

```go
package registry_test

import (
	"context"
	"testing"
	
	"github.com/jrmarcco/synp/internal/pkg/registry"
	"github.com/jrmarcco/synp/internal/pkg/session"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestConnRegistry(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	reg := registry.NewRedisConnRegistry(rdb, "gateway-test")
	ctx := context.Background()
	
	user := session.User{
		BID: 1,
		UID: 123,
	}
	
	// 测试注册
	err := reg.Register(ctx, user)
	assert.NoError(t, err)
	
	// 测试查询
	nodes, err := reg.FindGatewayNodes(ctx, user.BID, user.UID)
	assert.NoError(t, err)
	assert.Contains(t, nodes, "gateway-test")
	
	// 测试注销
	err = reg.Unregister(ctx, user)
	assert.NoError(t, err)
	
	nodes, err = reg.FindGatewayNodes(ctx, user.BID, user.UID)
	assert.NoError(t, err)
	assert.Empty(t, nodes)
}
```

### 测试多网关场景

启动两个网关实例：

```bash
# Terminal 1
GATEWAY_NODE_ID=gateway-1 go run cmd/main.go

# Terminal 2
GATEWAY_NODE_ID=gateway-2 go run cmd/main.go
```

使用不同设备连接：

```bash
# 手机连接到 gateway-1
# PC 连接到 gateway-2

# 查询 Redis
redis-cli SMEMBERS synp:conn_registry:1:123
# 应该看到：
# 1) "gateway-1"
# 2) "gateway-2"
```

## 🚨 常见问题

### Q: 启动时报错 "gateway.node_id is required"

**A:** 检查配置文件是否正确加载：

1. 确认 `config/gateway.yaml` 存在
2. 确认 `loadConfig` 函数中包含了 `"gateway"`
3. 使用环境变量：`GATEWAY_NODE_ID=gateway-1 go run cmd/main.go`

### Q: Redis 中看不到连接注册

**A:** 检查以下几点：

1. lifecycle handler 是否正确注入了 connRegistry
2. OnConnect 回调是否被调用（查看日志）
3. Redis 连接是否正常

### Q: 如何查看当前在线用户？

**A:** 使用 Redis 命令：

```bash
# 查看所有连接注册 key
redis-cli KEYS "synp:conn_registry:*"

# 查看特定用户
redis-cli SMEMBERS synp:conn_registry:1:123
```

### Q: 网关重启后，Redis 中还有旧的注册信息？

**A:** 这是正常的，Redis 中的连接注册有 2 小时的 TTL，会自动过期。如果需要立即清理：

```bash
# 手动清理特定用户
redis-cli DEL synp:conn_registry:1:123

# 清理所有（慎用）
redis-cli KEYS "synp:conn_registry:*" | xargs redis-cli DEL
```

## 📚 下一步

现在你已经启用了分布式网关路由，接下来可以：

1. **添加 gRPC 服务**：参考 [迁移指南](./migration_guide.md)
2. **后端集成**：参考 [后端集成示例](./backend_integration_example.go)
3. **监控告警**：添加连接注册成功率等监控指标
4. **性能测试**：测试分布式场景下的消息延迟

详细的架构设计和最佳实践，请查看：
- [架构文档](./distributed_gateway_architecture.md)
- [迁移指南](./migration_guide.md)
- [总结文档](./SUMMARY.md)
