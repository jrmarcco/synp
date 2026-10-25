# Synp WebSocket 网关性能测试方案

## 1. 目的与说明

本方案用于评估 Synp WebSocket 网关的：

- 单节点稳定连接容量；
- 连接建立和重连能力；
- 上行、下行消息吞吐与端到端延迟；
- 慢客户端下的队列背压与故障隔离能力；
- Redis、Kafka 等外部依赖对网关性能的影响；
- 长时间运行时的内存、FD、goroutine 和 GC 稳定性。

本文中的容量数据来自代码、Go 运行时结构和机器规格推导，**不是实际压力测试结果**。正式容量必须以独立压测环境中的重复测试结果为准。

## 2. 代码路径概览

### 2.1 连接建立

```text
TCP Accept
  -> WebSocket Upgrade
  -> JWT 验证
  -> Redis Session EVAL
  -> ConnManager.NewConn
  -> Redis ConnRegistry Register
  -> readLoop + writeLoop + handleConn
```

主要代码：

- `internal/ws/server.go`
- `internal/ws/upgrader.go`
- `internal/ws/conn/conn.go`
- `internal/ws/conn/conn_manager.go`
- `internal/ws/conn/conn_registry.go`
- `internal/ws/conn/lifecycle/handler.go`

### 2.2 上行消息

```text
Client
  -> WebSocket readLoop
  -> receiveChan
  -> JSON/Proto 解码
  -> Redis SETNX 去重
  -> Kafka 同步 Produce（RequireAll）
  -> ACK 编码
  -> sendChan
  -> Client
```

每条非心跳上行消息至少包含一次 Redis `SETNX` 和一次 Kafka 同步写入。客户端 ACK 只有在两项操作完成后才会入队，因此 ACK 尾延迟会直接受到 Redis 和 Kafka 尾延迟影响。

### 2.3 下行消息

```text
Kafka
  -> 6 个 Consumer
  -> JSON 解码
  -> ConnManager.FindUserConn
  -> sendChan
  -> writeLoop
  -> Client
  -> Downstream ACK
```

下行默认有 6 个串行消费循环。客户端读取过慢导致 `sendChan` 满后，`Conn.Send` 会阻塞消费处理逻辑，并可能进一步造成 Kafka consumer lag。

## 3. 测试工具

| 工具 | 用途 | 说明 |
| --- | --- | --- |
| 自定义 Go Load Generator | 5 万级以上长连接、JWT、ACK、慢读和断网场景 | 首选；可使用 `gobwas/ws` 或 `coder/websocket`，延迟统计使用 HDR Histogram |
| k6 | 阶梯负载、场景编排、阈值和趋势输出 | 适合协议验证和中等连接数；高连接数前需确认压测机资源 |
| Prometheus + Grafana | 网关、主机、Redis、Kafka 和 loadgen 指标 | 建议 1～5 秒采集间隔 |
| pprof | CPU、heap、alloc、mutex、block 分析 | 每个稳定负载平台期采集 60 秒 CPU profile |
| go tool trace | goroutine 调度、阻塞和网络轮询分析 | 用于解释高连接数下的调度拐点 |
| redis-benchmark / memtier | Redis 基线和 Lua 脚本隔离测试 | Registry Lua 应使用真实 key 分布单独测试 |
| Kafka perf tools | Kafka Producer/Consumer 隔离测试 | 使用与生产一致的 TLS、分区、压缩和 `acks=all` |
| Toxiproxy 或 tc/netem | Redis/Kafka 延迟、抖动、丢包和断连 | 故障不应施加到监控链路 |

当前仓库没有网关 benchmark、Prometheus、pprof 或 CI 性能流水线。现有 codec 测试只验证正确性，etcd benchmark 不能代表网关性能。

## 4. 测试环境

### 4.1 推荐拓扑

```text
Load Generator 1 ─┐
                  ├── Load Balancer（可选）── Synp Gateway
Load Generator 2 ─┘                             ├── Redis
                                                ├── Kafka
                                                └── etcd

Prometheus/Grafana 独立部署
```

要求：

1. 至少使用两台独立压测机，防止客户端先成为瓶颈。
2. 网关、Redis、Kafka 和监控服务分开部署。
3. 所有机器进行 NTP 对时。
4. 固定 CPU、内存、Go 版本、镜像 digest、GOMAXPROCS 和 GC 参数。
5. Redis、Kafka 使用与生产一致的网络 RTT、TLS、ACK、副本数和分区数。
6. 压测机 CPU 不超过 60%，网络不超过 70%。
7. 网关 CPU、RSS 和 FD 建议不超过容器或机器限制的 70%。
8. `go.mod` 要求 Go 1.26，而当前 Dockerfile 使用 Go 1.25，正式测试前应统一版本。

### 4.2 系统参数

Linux 网关节点至少检查：

```bash
ulimit -n
sysctl net.core.somaxconn
sysctl net.ipv4.tcp_max_syn_backlog
sysctl net.netfilter.nf_conntrack_max
sysctl net.ipv4.tcp_keepalive_time
```

目标连接数应控制在 `nofile` 的 70% 以下，并为 Redis、Kafka、etcd、日志和监控连接保留 FD。

压测机还需要扩大本地端口范围。单台压测机连接单一网关地址时会受到源 IP/端口组合限制，可通过多压测机、多源 IP 或多网关地址解决。

## 5. 空闲连接扫描配置

推荐默认值：

```yaml
synp:
  conn:
    idle_scan_interval: 30s
    idle_timeout: 2m
```

依据：

- 假设客户端每 30 秒发送一次心跳；
- `idle_timeout=2m` 可以容忍连续约 3～4 次心跳丢失；
- 30 秒扫描一次时，实际关闭时间约为 2～2.5 分钟；
- `read_timeout=15s` 只是 socket 读 deadline，不应作为 idle timeout；
- 应校验 `0 < idle_scan_interval <= idle_timeout/4`。

当连接数超过 5 万时，不建议每 30 秒一次性扫描全部连接。推荐：

- 将连接分为 30 个分片，每秒扫描一个分片；或
- 使用时间轮，根据下一次过期时间增量清理。

当前 `cleanIdleConn` 尚未遍历 `ConnManager`。仅设置 interval 不能使空闲清理真正生效。

## 6. 理论稳定连接容量

### 6.1 计算假设

以下容量忽略连接限流器，并假设已经修复：

- `time.NewTicker(0)` 启动 panic；
- GracefulShutdown 自锁；
- ConnRegistry 续租；
- 基础指标和 pprof 缺失。

连接模型：

- 每 30 秒一次心跳；
- 平均 payload 为 1KiB；
- send/receive 队列平均占用不超过 5%；
- 没有大规模未 ACK 下行消息；
- 网关和外部依赖位于低延迟网络；
- `GOGC=100`；
- 容量表示低频心跳长连接，不表示每连接 1 msg/s 的业务容量。

### 6.2 单连接内存依据

| 项目 | 依据 | 理论占用 |
| --- | --- | ---: |
| Channel 槽位 | send 256 + receive 256，64 位 slice header 为 24B | 12KiB |
| Goroutine 初始栈 | `handleConn`、`readLoop`、`writeLoop`，每个初始栈约 2KiB | 至少 6KiB |
| Flate hashHead | `1<<17` 个 `uint32` | 512KiB |
| Flate hashPrev | `1<<15` 个 `uint32` | 128KiB |
| Flate window | 两倍 32KiB 窗口 | 64KiB |
| Flate tokens | 约 `1<<14` 个 `uint32` | 约 64KiB |
| Flate 其他结构 | hashMatch、Huffman、reader 等 | 额外若干 KiB |

因此：

- 不压缩的空闲连接建议按 **64～96KiB/连接**规划；
- 协商启用压缩后建议按 **0.85～1.0MiB/连接**规划；
- 以上不包含队列中实际 payload；
- 两个 256 队列都填满时，1KiB 消息最多可额外占用约 0.5MiB/连接；
- Go `GOGC=100` 下，heap goal 通常接近 live heap 的两倍，不能把全部物理内存直接除以单连接 live bytes。

### 6.3 具体机器理论值

| 机器 | 配置 | 压缩开启 | 压缩关闭 | 说明 |
| --- | --- | ---: | ---: | --- |
| 当前开发机 | Apple M3 Max，14 核，36GiB，FD 1,048,575 | 10,000～14,000 | 60,000～100,000 | Darwin 结果不能直接替代生产 Linux |
| 通用 Linux | 16 vCPU，32GiB，nofile≥262k | 9,000～12,000 | 70,000～120,000 | 预留 25% 系统内存 |
| 高容量 Linux | 32 vCPU，64GiB，nofile≥524k | 20,000～26,000 | 160,000～250,000 | 需要分片扫描和 deadline 优化 |

以当前 36GiB 开发机为例：

1. 预留约 6GiB 给系统和内核；
2. 网关进程 RSS 预算约 30GiB；
3. `GOGC=100` 时将 live heap 控制在约 15GiB；
4. 压缩连接按 0.9MiB 计算，内存上限约为 17,000；
5. 为队列波动、栈增长、GC 和内核 socket 留出余量后，建议范围为 10,000～14,000；
6. 不压缩时内存上限明显提高，但调度和 deadline 唤醒会先成为限制。

当前每连接每 15 秒触发一次读 deadline：

```text
timeout wakeups/s = connections / 15
```

对应：

| 连接数 | 超时唤醒次数 |
| ---: | ---: |
| 10,000 | 约 667 次/s |
| 50,000 | 约 3,333 次/s |
| 100,000 | 约 6,667 次/s |
| 150,000 | 约 10,000 次/s |

因此，在不修改当前读 deadline 模型的情况下，将当前开发机的不压缩稳定值收紧为 60,000～100,000。若改为基于心跳更新一次较长读 deadline，可进一步验证 100,000～150,000。

### 6.4 消息吞吐不能由连接内存推导

如果每连接每秒发送一条非心跳业务消息：

| 连接数 | 上行 QPS | Redis SETNX | Kafka 同步写 |
| ---: | ---: | ---: | ---: |
| 10,000 | 10,000/s | 10,000/s | 10,000/s |
| 50,000 | 50,000/s | 50,000/s | 50,000/s |
| 100,000 | 100,000/s | 100,000/s | 100,000/s |

此时 Redis、Kafka、JSON 编解码和日志通常会早于连接内存达到上限。因此必须分别报告：

- 最大稳定在线连接数；
- 指定连接数下的最大消息吞吐；
- 满足 ACK p95/p99 SLO 的安全容量。

## 7. 分阶段测试步骤

### 7.1 P0：正确性和可测性

1. 修复第 10 节中的 P0 缺陷。
2. 增加 Prometheus 指标和受保护的 pprof 管理端口。
3. 使用 100 个连接、1 msg/s 运行 10 分钟。
4. 验证 JWT、连接注册、上行 ACK、下行 ACK、去重、断开和重连。
5. 确认没有 panic、goroutine 泄漏、FD 泄漏或错误路由。

### 7.2 P1：组件隔离基准

分别测试：

1. JSON 与 Proto 编解码；
2. WebSocket 帧读写；
3. 压缩开/关以及 128B、1KiB、8KiB payload；
4. Redis Session Lua；
5. Redis Registry Lua；
6. Redis `SETNX`；
7. Kafka `acks=all` Producer；
8. Kafka 6 分区 Consumer。

每项记录：

- ops/s；
- p50、p95、p99、max；
- alloc/op 和 bytes/op；
- CPU；
- RSS；
- GC 次数和暂停。

Kafka 示例：

```bash
kafka-producer-perf-test.sh \
  --topic synp.message.upstream \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=<brokers> \
    acks=all \
    compression.type=snappy
```

### 7.3 P2：连接容量

按以下阶梯建立连接：

```text
500 -> 1,000 -> 2,000 -> 5,000 -> 10,000
     -> 20,000 -> 50,000 -> 100,000
```

规则：

1. 每档建立时间不少于 2 分钟，避免重连风暴掩盖稳态表现；
2. 每档预热 5 分钟；
3. 稳定采样 10～30 分钟；
4. 记录握手成功率、握手 p95/p99、RSS、heap、FD、goroutine、GC 和网络；
5. 压缩开、关分别执行；
6. 每组执行三次，使用中位数作为结果；
7. 首次违反任一 SLO 的前一档为候选安全容量。

### 7.4 P3：上行消息

测试矩阵：

- 连接数：500、1,000、5,000、10,000；
- payload：128B、1KiB、8KiB；
- 每连接速率：1、5、10 msg/s；
- codec：JSON、Proto；
- 压缩：开启、关闭。

端到端延迟定义：

```text
客户端完成 WebSocket Write
  -> 网关读取和解码
  -> Redis SETNX
  -> Kafka RequireAll
  -> ACK 入队和写出
  -> 客户端收到 ACK
```

不能用 HTTP 或 WebSocket write 调用耗时替代业务 ACK 延迟。

### 7.5 P4：下行消息

从 Kafka Producer 写入携带发送时间戳的消息，测量客户端实际收到消息的时间。

测试：

- ACK 率：100%、99%、90%；
- 慢读客户端：0%、1%、10%；
- 每用户设备数：1、2、3；
- 正常 payload 与 8KiB payload；
- Kafka partition lag；
- send queue depth；
- retransmit task 数量和 timer 数量。

### 7.6 P5：突发与反压

1. 稳态运行 5 分钟；
2. 在 10 秒内提升到基线流量的 10 倍；
3. 持续 30 秒；
4. 恢复到基线流量；
5. 记录队列峰值、Send 阻塞、消息错误率、断连数、Kafka lag 和恢复时间。

### 7.7 P6：稳定性

在目标容量的 70% 下运行 12～24 小时。

通过条件：

- RSS、heap、FD、goroutine 没有持续单调增长；
- 连接注册信息在 30 分钟后仍然有效；
- retransmit task 不持续累积；
- 错误率和 p99 没有随时间恶化；
- GC CPU 占比小于 5%；
- 关闭和重启能在设定超时内完成。

### 7.8 P7：故障注入

依次注入：

- Redis 延迟 20ms、50ms、100ms；
- Redis 断开和恢复；
- Kafka Produce 延迟和 broker 故障；
- Kafka Consumer rebalance；
- 1% 和 5% 网络丢包；
- 网关单实例受控重启；
- 集中重连。

记录错误传播、重试放大、连接恢复时间和消息重复率。

## 8. 必采指标

### 8.1 网关

- 当前连接数和用户数；
- handshake total/success/error；
- handshake p50/p95/p99；
- message in/out；
- 上行 ACK 和下行送达 p50/p95/p99；
- send/receive queue depth；
- queue block/drop；
- retransmit task；
- goroutine；
- heap live、heap goal、RSS；
- allocation rate；
- GC CPU 和 pause；
- FD；
- 网络吞吐和重传。

### 8.2 Redis

- commands/s；
- EVAL、EVALSHA、SETNX、ZREM、DEL 延迟；
- connected_clients；
- blocked_clients；
- CPU、内存和网络；
- evictions；
- slowlog。

### 8.3 Kafka

- Producer request p95/p99；
- records/s 和 bytes/s；
- batch size；
- Consumer lag；
- rebalance；
- request queue；
- under-replicated partitions；
- broker CPU、磁盘和网络。

## 9. 建议初始 SLO

以下是压测起始标准，不是代码保证：

| 维度 | 建议标准 |
| --- | --- |
| 握手成功率 | ≥99.9% |
| 握手延迟 | p95≤300ms，p99≤800ms |
| 非计划断连 | <0.1%/h |
| 上行 ACK | p95≤200ms，p99≤500ms |
| 下行送达 | p95≤200ms，p99≤500ms |
| 消息错误率 | <0.1% |
| CPU | ≤70% |
| RSS | ≤内存限制的 70% |
| FD | ≤nofile 的 70% |
| GC CPU | <5% |
| GC p99 pause | <10ms |

安全容量定义为：**连接数、吞吐、延迟、错误率和资源水位同时满足 SLO 的最大负载档位**。

## 10. 高优先级明显缺陷与修复建议

### P0：正式压测前必须修复

#### 10.1 idleScanInterval 为零导致启动 panic

- 位置：`internal/ws/server.go:43`、`internal/ws/server.go:258-260`
- 问题：`time.NewTicker(0)` 会 panic。
- 修复：
  - 增加 `idle_scan_interval` 和 `idle_timeout` 配置；
  - 默认分别使用 30 秒和 2 分钟；
  - 构造和启动时校验参数；
  - 增加 `Server.Start` 测试。

#### 10.2 GracefulShutdown 自锁

- 位置：`internal/ws/server.go:372-385`
- 问题：在调用 `Shutdown`/`cancelFunc` 前等待 `<-s.ctx.Done()`，可能永久阻塞。
- 修复：
  - 先停止接受连接；
  - 停止 Kafka Consumer；
  - 等待连接排空；
  - 到达超时后关闭剩余连接；
  - 最后调用 `cancelFunc`；
  - 为完整关闭流程设置总超时。

#### 10.3 TokenLimiter 扩容没有启动

- 位置：`internal/ws/server.go:68`、`internal/pkg/limiter/token_limiter.go:114`
- 问题：`Start()` 没有调用，容量固定为初始 2,000。
- 修复：
  - 将限流器接入 fx lifecycle；或
  - 使用显式 `max_connections` 配置和原子连接计数代替渐增令牌模型。

#### 10.4 ConnRegistry 没有续租

- 位置：`internal/ws/conn/conn_registry.go:192-194`
- 问题：`Heartbeat()` 没有调用，默认 30 分钟后活跃连接的路由记录可能失效。
- 修复：
  - 在应用心跳处理时限频调用 Registry Heartbeat；或
  - 网关节点批量续租本地活跃用户；
  - 稳定性测试必须超过 30 分钟并持续调用 Locate 验证。

### P1：高优先级性能与正确性问题

#### 10.5 多设备连接的 Registry 注销语义错误

- 位置：`internal/ws/conn/conn_registry.go:140-153`
- 问题：Registry member 只包含节点，不包含 device/connID。同一节点上的一个设备断开会删除节点 member，即使用户还有其他设备连接。
- 修复：
  - member 增加 device 或 connID；或
  - 在本地维护节点连接引用计数，最后一个连接断开时才注销。

#### 10.6 sendChan 满后无限阻塞

- 位置：`internal/ws/conn/conn.go:337-346`
- 问题：`Send` 没有超时。慢客户端可以阻塞 Kafka 消费 handler。
- 修复：
  - 增加 `Send(ctx, payload)`；
  - 配置 enqueue timeout；
  - 暴露 queue depth 和 queue block 指标；
  - 明确超时后的断连、丢弃或重试策略。

#### 10.7 消费超时 context 未传递

- 位置：`internal/pkg/xmq/xprocessor/consume_processor.go:113`、`internal/ws/server.go:306`
- 问题：Processor 创建 handler timeout context，但 `consumePushMessage` 忽略它。
- 修复：
  - 将 context 传递到发送路径；
  - 超时后终止当前投递并记录 receiver、partition 和 queue depth。

#### 10.8 压缩器常驻内存高

- 位置：`internal/pkg/xws/writer.go:63-70`
- 问题：每个协商压缩的连接创建独立 flate writer，单 compressor 常驻结构约 769KiB 起。
- 修复：
  - 小消息默认不压缩；
  - 按 payload 阈值启用压缩；
  - A/B 测试 level=1 和 level=6；
  - 评估安全的压缩器池；
  - 确保配置中的 compression level 真正传递到 writer。

#### 10.9 消息读取没有大小限制

- 位置：`internal/pkg/xws/reader.go:25-45`
- 问题：使用 `io.ReadAll`，异常大帧可能造成内存放大和 OOM。
- 修复：
  - 增加 `max_message_bytes`；
  - 使用 LimitedReader 或分段读取；
  - 超限后返回 WebSocket `Message Too Big` 并关闭连接。

#### 10.10 上行 ACK 同步依赖 Redis 和 Kafka

- 位置：`internal/ws/conn/lifecycle/handler.go:220-234`、`internal/pkg/message/upstream/frontend_msg_handler.go:82-101`
- 问题：ACK 串行等待 Redis SETNX 和 Kafka `RequireAll`。
- 修复：
  - 分别增加 Redis dedup 和 Kafka produce latency 指标；
  - 评估 Redis pipeline、本地短期去重或 Kafka 批处理；
  - 只有业务允许“已接收但未持久化”的语义时才能异步 ACK。

#### 10.11 连接断开未清理重传任务

- 位置：`internal/pkg/retransmit/manager.go:192-203`
- 问题：`ClearByConn` 没有调用，timer 在重试完成前继续持有连接和消息。
- 修复：
  - 在 `OnDisconnect` 调用 `ClearByConn`；
  - 只对成功入队的连接创建一次重传任务；
  - 暴露 active retransmit task 指标。

#### 10.12 RemoveConn 锁外访问 map

- 位置：`internal/ws/conn/conn_manager.go:247-275`
- 问题：释放 `DeviceConns` 锁后读取 `len(dc.conns)`，存在竞态。
- 修复：
  - 让 `remove` 在锁内返回 remaining count；
  - 使用 `go test -race` 验证连接风暴和同用户多设备重连。

#### 10.13 热路径记录完整消息

- 位置：`internal/ws/conn/lifecycle/handler.go:167-172`
- 问题：每条上行消息使用 Info 级别记录完整内容，增加序列化、锁、磁盘和 GC 开销。
- 修复：
  - 生产环境仅记录 message ID、长度和采样后的元数据；
  - 禁止记录完整业务 body；
  - 单独对比日志开/关性能。

### P2：可观测性和工程一致性

#### 10.14 缺少性能观测

- 位置：全仓库。
- 修复：
  - 增加 Prometheus 指标；
  - 在独立管理端口暴露 pprof；
  - 增加 codec、frame、registry 和 ConnManager benchmark；
  - CI 保存 benchmark 历史并检测显著回退。

#### 10.15 Go 构建版本不一致

- 位置：`scripts/build/Dockerfile:1,13`、`go.mod:3`
- 问题：Docker 使用 Go 1.25，模块要求 Go 1.26。
- 修复：构建和运行镜像固定为同一个 Go 1.26 patch 版本。

## 11. 测试结果模板

每轮测试至少保存：

```text
Git SHA:
Image digest:
Go version:
Machine:
GOMAXPROCS:
GOGC/GOMEMLIMIT:
Compression:
Codec:
Connections:
Payload distribution:
Message rate:
Duration:

Handshake success:
Handshake p50/p95/p99:
Upstream ACK p50/p95/p99:
Downstream delivery p50/p95/p99:
Disconnect rate:
CPU:
RSS/Heap:
Goroutines:
FD:
GC CPU/pause:
Redis p95/p99:
Kafka produce p95/p99:
Kafka consumer lag:
Queue depth/block/drop:
Retransmit tasks:

First bottleneck:
SLO result:
Conclusion:
```

正式报告应同时附带：

- Load Generator 原始 JSON/HDR Histogram；
- Prometheus 时间段快照；
- CPU、heap、alloc、mutex 和 block profile；
- 网关、Redis 和 Kafka 配置；
- 环境与内核参数；
- 三次重复测试结果及其中位数。
