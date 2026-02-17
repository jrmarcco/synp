# 分布式网关文档中心

欢迎！这里是 synp 分布式网关的完整文档。

## 🚀 快速导航

### 新手入门
- **[快速开始](./QUICKSTART.md)** ⭐ - 5分钟快速上手，启用分布式网关路由
- **[总结文档](./SUMMARY.md)** - 了解架构优化的背景和方案

### 核心文档
- **[架构设计](./distributed_gateway_architecture.md)** - 完整的架构设计和数据流说明
- **[迁移指南](./migration_guide.md)** - 详细的迁移步骤和测试计划
- **[实施检查清单](./CHECKLIST.md)** - 任务清单和进度追踪

### 代码示例
- **[后端集成示例](./backend_integration_example.go)** - 后端如何调用网关 gRPC

## 📚 文档概览

### 1. [快速开始 (QUICKSTART.md)](./QUICKSTART.md)

**适合人群**：想要快速体验功能的开发者

**包含内容**：
- ⚡ 5分钟上手步骤
- 📝 配置说明
- 🧪 测试方法
- 🚨 常见问题

**快速链接**：
```bash
# 启动网关
cp config/gateway.yaml.example config/gateway.yaml
GATEWAY_NODE_ID=gateway-1 go run cmd/main.go

# 验证连接注册
redis-cli SMEMBERS synp:conn_registry:1:123
```

---

### 2. [总结文档 (SUMMARY.md)](./SUMMARY.md)

**适合人群**：想要了解全貌的技术决策者

**包含内容**：
- 📌 问题背景和挑战
- 🎯 方案对比和选择
- 🏗️ 核心架构设计
- 📦 已完成的工作
- 🔧 待完成的工作

**关键要点**：
- ✅ 性能提升 60%（延迟从 200-600ms → 105-320ms）
- ✅ 架构更简单（去掉第二个 Kafka）
- ✅ 符合 IM 行业标准

---

### 3. [架构设计 (distributed_gateway_architecture.md)](./distributed_gateway_architecture.md)

**适合人群**：想要深入理解架构的开发者

**包含内容**：
- 🎨 完整的架构图
- 🔄 消息流程说明
- 🗄️ Redis 数据结构
- 📊 性能对比分析
- 🔧 容错处理策略
- 📈 监控指标设计

**核心概念**：
```
用户上线 → Redis 注册 (user → gateway 映射)
消息推送 → 查询 Redis → 精准 gRPC 调用
用户下线 → Redis 注销
```

---

### 4. [迁移指南 (migration_guide.md)](./migration_guide.md)

**适合人群**：负责实施迁移的开发者

**包含内容**：
- 📋 分阶段迁移步骤
- 💻 完整的代码示例
- 🧪 测试计划
- 🔄 回滚方案
- ⚠️ 常见问题

**迁移阶段**：
1. **准备阶段**：配置和依赖注入
2. **gRPC 阶段**：定义和实现 gRPC 服务
3. **清理阶段**：删除旧的 Kafka 消费
4. **集成阶段**：后端对接

---

### 5. [实施检查清单 (CHECKLIST.md)](./CHECKLIST.md)

**适合人群**：项目管理者和实施团队

**包含内容**：
- ✅ 已完成的工作
- 📋 待完成的任务
- 🎯 优先级划分
- 📊 进度追踪
- 💡 验收标准

**当前进度**：
- 基础架构：100% ✅
- 功能实现：35% 🚧
- 后端集成：0% ⏳

---

### 6. [后端集成示例 (backend_integration_example.go)](./backend_integration_example.go)

**适合人群**：后端服务开发者

**包含内容**：
- 完整的 Go 代码示例
- Redis 查询实现
- gRPC 客户端调用
- 错误处理示例

**核心代码**：
```go
// 1. 查询用户所在网关
nodes := findGatewayNodes(ctx, bid, uid)

// 2. 调用网关 gRPC
for _, node := range nodes {
    client.PushMessage(ctx, &PushMessageRequest{...})
}
```

---

## 🎯 按角色查看

### 我是新手开发者
1. 先看 [快速开始](./QUICKSTART.md) 快速体验
2. 再看 [总结文档](./SUMMARY.md) 了解全貌
3. 遇到问题查看快速开始中的常见问题

### 我是架构师
1. 先看 [总结文档](./SUMMARY.md) 了解方案对比
2. 再看 [架构设计](./distributed_gateway_architecture.md) 了解细节
3. 评估性能提升和技术风险

### 我是实施开发者
1. 先看 [迁移指南](./migration_guide.md) 了解步骤
2. 参考 [检查清单](./CHECKLIST.md) 追踪进度
3. 使用 [后端集成示例](./backend_integration_example.go) 编写代码

### 我是测试工程师
1. 查看 [迁移指南](./migration_guide.md) 的测试计划
2. 查看 [检查清单](./CHECKLIST.md) 的验收标准
3. 参考 [快速开始](./QUICKSTART.md) 的测试方法

---

## 🔗 外部资源

### 配置文件
- `config/gateway.yaml.example` - 网关配置示例

### 核心代码
- `internal/pkg/registry/conn_registry.go` - 连接注册表
- `internal/grpc/gateway_service.go` - gRPC 服务
- `internal/ws/conn/lifecycle/handler.go` - 生命周期处理

### 依赖注入
- `internal/pkg/provider/registry.go` - Registry 模块
- `internal/ws/conn/lifecycle/module.go` - Handler 模块
- `cmd/main.go` - 主入口

---

## 📞 获取帮助

### 常见问题
大部分问题可以在以下文档中找到答案：
- [快速开始 - 常见问题](./QUICKSTART.md#-常见问题)
- [迁移指南 - 常见问题](./migration_guide.md#常见问题)

### 问题排查
1. 检查配置文件是否正确
2. 检查 Redis 连接是否正常
3. 查看应用日志
4. 使用 Redis CLI 验证数据

### Debug 技巧
```bash
# 查看连接注册
redis-cli KEYS "synp:conn_registry:*"

# 查看特定用户
redis-cli SMEMBERS synp:conn_registry:1:123

# 查看 TTL
redis-cli TTL synp:conn_registry:1:123
```

---

## 🎉 开始使用

立即开始使用分布式网关：

```bash
# 1. 复制配置
cp config/gateway.yaml.example config/gateway.yaml

# 2. 修改配置
vim config/gateway.yaml

# 3. 启动网关
GATEWAY_NODE_ID=gateway-1 go run cmd/main.go

# 4. 验证功能
redis-cli KEYS "synp:conn_registry:*"
```

祝你使用愉快！🚀

---

<p align="center">
  Made with ❤️ for distributed WebSocket gateway
</p>
