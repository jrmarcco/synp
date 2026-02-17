# 分布式网关架构方案

## 架构概览

```
┌─────────────┐
│   前端用户   │
└──────┬──────┘
       │ WebSocket
       ↓
┌─────────────────────────────────────────────────┐
│              网关集群 (分布式部署)                │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐ │
│  │ Gateway-1  │  │ Gateway-2  │  │ Gateway-3  │ │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘ │
│        │                │                │       │
│        └────────────────┼────────────────┘       │
│                         ↓                        │
│              ┌──────────────────┐                │
│              │  连接注册表(Redis) │                │
│              │  user → gateway  │                │
│              └──────────────────┘                │
└─────────────────────────────────────────────────┘
       │                         ↑
       │ Kafka                   │ gRPC (查询Redis后精准路由)
       ↓                         │
┌─────────────────────────────────────────────────┐
│                  后端服务                         │
│  1. 从Kafka消费消息                               │
│  2. 落库保证可靠性                                │
│  3. 查询Redis获取网关节点                         │
│  4. 调用对应网关的gRPC推送                        │
└─────────────────────────────────────────────────┘
```

## 核心组件

### 1. ConnRegistry (连接注册表)

**职责**：在Redis中维护用户连接到网关节点的映射关系

**数据结构**：
```
Key: synp:conn_registry:{bid}:{uid}
Type: Set
Value: ["gateway-1", "gateway-2", "gateway-3"]  # 用户的多设备可能在不同节点
TTL: 2小时（防止网关crash后没清理）
```

**接口**：
- `Register(user)` - 用户上线时注册
- `Unregister(user)` - 用户下线时注销
- `FindGatewayNodes(bid, uid)` - 查找用户所在的网关节点
- `Heartbeat(user)` - 定期心跳，延长TTL

### 2. Lifecycle Handler (生命周期处理器)

**职责**：在连接建立和断开时自动注册/注销连接

```go
OnConnect(conn):
    1. 调用 connRegistry.Register(user)
    2. 记录日志
    
OnDisconnect(conn):
    1. 调用 connRegistry.Unregister(user)
    2. 关闭连接
```

### 3. Gateway gRPC Service

**职责**：提供gRPC接口供后端调用推送消息

**接口**：
```protobuf
service GatewayService {
  rpc PushMessage(PushMessageRequest) returns (PushMessageResponse);
  rpc BatchPushMessage(BatchPushMessageRequest) returns (BatchPushMessageResponse);
}
```

## 消息流程

### 前端发送消息

```
1. 前端 → 网关 (WebSocket)
2. 网关 → Kafka → 后端
3. 后端落库
4. 后端查询Redis获取接收方所在网关节点
5. 后端 → 网关 (gRPC) → 接收方前端
```

### 关键点

1. **消息先落库再推送**：保证可靠性，即使推送失败用户也能通过离线消息获取
2. **精准路由**：后端查询Redis后直接调用目标网关，减少无效RPC
3. **多设备支持**：一个用户可能在多个网关节点有连接（手机+PC）
4. **自动清理**：Redis设置TTL，即使网关crash也能自动清理过期数据

## 网关节点标识

网关启动时需要配置唯一标识：

```yaml
# config.yaml
gateway:
  node_id: "gateway-1"  # 唯一标识，可以用hostname或IP
  grpc_port: 50051      # gRPC服务端口
```

后端需要维护网关节点列表：

```yaml
# backend config
gateways:
  - name: "gateway-1"
    addr: "gateway-1.example.com:50051"
  - name: "gateway-2"
    addr: "gateway-2.example.com:50051"
  - name: "gateway-3"
    addr: "gateway-3.example.com:50051"
```

## 容错处理

### 网关节点crash

1. Redis中的连接注册会在TTL后自动过期
2. 用户重连到其他网关节点
3. 新连接会注册到新的节点

### Redis故障

1. 连接注册失败不影响WebSocket连接建立
2. 后端查询失败时可以广播到所有网关（降级方案）
3. 用户可以通过拉取离线消息获取漏掉的消息

### 后端推送失败

1. 用户不在线时跳过推送
2. 用户上线后通过拉取离线消息获取
3. 推送失败不影响消息落库

## 性能优化

### 1. 连接查询优化

- 使用Redis Set，查询时间复杂度O(1)
- 批量查询可以使用pipeline减少往返次数

### 2. gRPC连接池

后端维护到各网关节点的gRPC连接池，避免每次推送都建立连接

### 3. 心跳优化

不需要每次消息都更新Redis TTL，可以定期心跳（如每30秒）

## 监控指标

1. **连接注册成功率**：`conn_registry_success_rate`
2. **推送成功率**：`push_message_success_rate`
3. **Redis查询延迟**：`redis_query_latency`
4. **gRPC调用延迟**：`grpc_push_latency`
5. **在线用户数**：`online_users_count`

## 与旧架构对比

### 旧架构（双Kafka）

```
前端 → 网关 → Kafka1 → 后端落库 → Kafka2 → 网关 → 接收方
延迟：~100-300ms         ~100-300ms
总延迟：200-600ms
```

### 新架构（单Kafka + gRPC）

```
前端 → 网关 → Kafka → 后端落库 → gRPC → 网关 → 接收方
延迟：~100-300ms      ~5-20ms
总延迟：105-320ms
```

**性能提升**：约50-60%的延迟降低

## 下一步优化（可选）

如果需要进一步优化延迟，可以考虑：

1. **写扩散优化**：后端异步落库，落库成功后立即推送
2. **快速路径**：网关转发到Kafka后立即推送临时消息，后端确认后发送确认消息
3. **消息去重**：客户端基于message_id去重，避免双重投递问题

这些优化会增加系统复杂度，建议在有明确性能瓶颈时再考虑。
