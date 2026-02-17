# 分布式网关实施检查清单

## ✅ 已完成的工作

### 核心功能实现

- [x] **连接注册表** (`internal/pkg/registry/conn_registry.go`)
  - [x] Redis 存储用户→网关节点映射
  - [x] 支持 Register/Unregister/FindGatewayNodes
  - [x] 自动 TTL 过期（2小时）
  - [x] 支持多设备场景

- [x] **Lifecycle Handler 集成** (`internal/ws/conn/lifecycle/`)
  - [x] OnConnect 时注册连接到 Redis
  - [x] OnDisconnect 时从 Redis 注销
  - [x] 依赖注入配置完成

- [x] **依赖注入配置**
  - [x] 创建 `internal/pkg/provider/registry.go`
  - [x] 添加 RegistryFxModule
  - [x] 在 main.go 中注册模块
  - [x] 更新 lifecycle/module.go 依赖

- [x] **gRPC 服务定义** (`internal/grpc/gateway_service.go`)
  - [x] PushMessage 接口
  - [x] BatchPushMessage 接口
  - [x] 集成 ConnManager 和 RetransmitManager

### 文档

- [x] **架构设计文档** (`docs/distributed_gateway_architecture.md`)
  - [x] 完整的架构图
  - [x] 数据流说明
  - [x] 性能对比
  - [x] 监控指标

- [x] **迁移指南** (`docs/migration_guide.md`)
  - [x] 详细的迁移步骤
  - [x] 代码示例
  - [x] 测试计划
  - [x] 回滚方案

- [x] **后端集成示例** (`docs/backend_integration_example.go`)
  - [x] 完整的后端代码示例
  - [x] Redis 查询示例
  - [x] gRPC 调用示例

- [x] **快速开始指南** (`docs/QUICKSTART.md`)
  - [x] 5分钟上手步骤
  - [x] 配置说明
  - [x] 常见问题

- [x] **总结文档** (`docs/SUMMARY.md`)
  - [x] 问题背景
  - [x] 方案对比
  - [x] 实施清单

### 配置文件

- [x] **配置示例** (`config/gateway.yaml.example`)
  - [x] 网关节点 ID 配置
  - [x] gRPC 服务配置
  - [x] 环境变量示例

---

## 📋 待完成的工作

### 必须完成（核心功能）

#### 1. 配置文件
- [ ] 创建 `config/gateway.yaml`（从 example 复制）
- [ ] 在 `cmd/main.go` 的 `loadConfig` 中添加 `"gateway"` 到配置列表
- [ ] 设置环境变量或配置文件中的 `gateway.node_id`

#### 2. gRPC Protobuf 定义
- [ ] 创建 `api/proto/gateway/v1/gateway.proto`
- [ ] 定义 PushMessage 相关消息
- [ ] 运行 `buf generate` 生成代码
- [ ] 更新 `internal/grpc/gateway_service.go` 使用生成的代码

#### 3. gRPC 服务器
- [ ] 创建 `internal/grpc/server.go`
- [ ] 实现 Start/Stop 方法
- [ ] 在 main.go 中启动 gRPC server
- [ ] 添加 gRPC server 到 fx lifecycle

#### 4. 删除旧的 Kafka 消费逻辑
- [ ] 删除 `internal/ws/server.go` 中的 `consumeFromBackend` 函数
- [ ] 删除 backend Kafka consumer 初始化代码
- [ ] 清理配置文件中不需要的 backend topic 配置

#### 5. 后端服务改造
- [ ] 后端添加 Redis 连接
- [ ] 实现 `findGatewayNodes` 查询逻辑
- [ ] 为每个网关节点创建 gRPC 客户端
- [ ] 修改消息处理逻辑：落库后调用网关 gRPC

---

### 可选完成（增强功能）

#### 监控和可观测性
- [ ] 添加 Prometheus 指标
  - [ ] `synp_conn_registry_ops_total{op="register|unregister|find"}`
  - [ ] `synp_conn_registry_errors_total`
  - [ ] `synp_grpc_push_duration_seconds`
  - [ ] `synp_grpc_push_errors_total`
  - [ ] `synp_online_users_count`
  - [ ] `synp_gateway_nodes_count`

#### 心跳机制
- [ ] 实现定期心跳，延长 Redis TTL
- [ ] 避免长连接因 TTL 过期被清理
- [ ] 建议间隔：30秒或1分钟

#### 服务发现
- [ ] 集成 etcd/consul 进行网关节点注册
- [ ] 后端自动发现网关节点
- [ ] 避免静态配置网关列表

#### 降级方案
- [ ] Redis 故障时的降级策略
  - [ ] 广播到所有网关节点
  - [ ] 或者只推送失败，依赖离线消息
- [ ] 网关节点故障检测和自动剔除

#### 性能优化
- [ ] gRPC 连接池
- [ ] Redis pipeline 批量查询
- [ ] 本地缓存网关节点信息（减少 Redis 查询）

#### 测试
- [ ] 单元测试
  - [ ] ConnRegistry 测试
  - [ ] GatewayService 测试
- [ ] 集成测试
  - [ ] 单网关场景测试
  - [ ] 多网关场景测试
  - [ ] 网关故障恢复测试
- [ ] 性能测试
  - [ ] 消息端到端延迟测试
  - [ ] 对比新旧架构性能
  - [ ] 压力测试

#### 文档
- [ ] API 文档（gRPC 接口）
- [ ] 运维文档（部署、监控、故障处理）
- [ ] 开发文档（如何添加新功能）

---

## 🎯 实施优先级

### P0（必须完成）- 基础功能
1. 配置文件和环境变量
2. 验证连接注册功能
3. 测试单网关场景

### P1（核心功能）- gRPC 推送
1. 定义和生成 Protobuf
2. 实现 gRPC 服务器
3. 后端集成 gRPC 调用
4. 测试多网关场景

### P2（清理优化）
1. 删除旧的 Kafka 消费逻辑
2. 添加基本监控指标
3. 性能对比测试

### P3（增强功能）
1. 心跳机制
2. 服务发现
3. 降级方案

---

## 📊 进度追踪

| 阶段 | 任务数 | 已完成 | 进度 |
|------|--------|--------|------|
| 核心功能实现 | 4 | 4 | 100% ✅ |
| 文档编写 | 5 | 5 | 100% ✅ |
| 配置文件 | 3 | 1 | 33% 🚧 |
| gRPC 服务 | 4 | 1 | 25% 🚧 |
| 后端集成 | 4 | 0 | 0% ⏳ |
| 清理工作 | 3 | 0 | 0% ⏳ |
| 增强功能 | 10+ | 0 | 0% ⏳ |

**总体进度：基础架构 100% ✅ | 功能实现 35% 🚧**

---

## 🚀 下一步行动

### 今天（立即可做）
1. ✅ 复制 `config/gateway.yaml.example` → `config/gateway.yaml`
2. ✅ 修改 `loadConfig` 添加 "gateway" 配置
3. ✅ 设置 `GATEWAY_NODE_ID` 环境变量
4. ✅ 启动网关并验证连接注册

### 本周
1. 📝 定义 gRPC Protobuf
2. 🔨 实现 gRPC 服务器
3. 🔗 后端集成 gRPC 调用
4. ✅ 端到端测试

### 下周
1. 🧹 清理旧代码
2. 📊 添加监控指标
3. 🚀 部署到测试环境
4. 📈 性能测试和对比

---

## 💡 验收标准

### 功能验收
- [ ] 用户连接时，Redis 中正确记录网关节点
- [ ] 用户断开连接时，Redis 中正确清理
- [ ] 后端能够查询 Redis 找到网关节点
- [ ] 后端能够通过 gRPC 推送消息
- [ ] 支持多设备场景（一个用户在多个网关）

### 性能验收
- [ ] 消息端到端延迟 < 320ms（旧架构 < 600ms）
- [ ] Redis 查询延迟 < 5ms
- [ ] gRPC 调用延迟 < 20ms
- [ ] 连接注册成功率 > 99.9%

### 可靠性验收
- [ ] 网关重启后连接能够正确恢复
- [ ] Redis 故障不影响 WebSocket 连接建立
- [ ] 推送失败后能够通过重传或离线消息补偿

---

## 📞 需要帮助？

如果在实施过程中遇到问题：

1. 查看 [快速开始指南](./QUICKSTART.md) 的常见问题
2. 查看 [迁移指南](./migration_guide.md) 的详细步骤
3. 查看 [架构文档](./distributed_gateway_architecture.md) 的设计细节
4. 参考 [后端集成示例](./backend_integration_example.go) 的代码实现

祝你实施顺利！🎉
