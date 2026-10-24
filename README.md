# synp

一个 WebSocket 网关。
synp 取自 Synapse（ 生物学中的 "突触" 的变体 ）。

API 契约见 [synp-api](../api)（ proto 目录为唯一事实源 ）。

## 运维

### 管理端口

`synp.admin.addr`（ 默认 `:9090` ）暴露:

- `GET /metrics` — Prometheus 指标（ 连接数、握手、各指令帧计数、NOTIFY 丢弃、Redis/Kafka/gRPC 耗时、AUTH 耗时 ）
- `GET /debug/pprof/*` — Go pprof

### 日志

全项目统一 slog 单管道（ `synp.log.*` 配置 ）:

- `mode: dev` — 单行紧凑格式（ 日期时间 级别 组件 消息 k=v ），TTY 着色
- `mode: prod` — JSON 输出（ zap core 承载 ），面向采集
- `level` / `color` / `file`（ 可选落盘 ）

代码中通过 `logging.For("component")` 获取组件日志器；禁止直接使用 zap 或全局 `slog.X`。

### 关键配置

| 配置 | 默认 | 说明 |
|---|---|---|
| `synp.websocket.max_connections` | 50000 | 最大并发连接数（ 原子计数准入，超出拒绝 ） |
| `synp.websocket.auth_timeout_ms` | 15000 | 首帧 AUTH 超时，超时断开 |
| `synp.websocket.idle_scan_interval` / `idle_timeout` | 30s / 2m | 空闲扫描（ 分片，每秒扫 1/30 ） |
| `synp.websocket.shutdown_drain_timeout` | 10s | 优雅关闭排空超时 |
| `synp.conn.manager.max_message_bytes` | 64KiB | 入站帧大小上限，超限断连 |
| `synp.conn.manager.established_read_timeout` | 90s | 鉴权后读超时（ 鉴权前用 `read_timeout` 短超时 ） |
| `synp.message_handler.notify.enqueue_timeout` | 200ms | NOTIFY 入队等待上限，超时丢弃（ 通知可丢 ） |

### 可靠性行为

- **NOTIFY 允许丢**: 入队超时即丢弃并计数（ `synp_notify_drop_total` ），客户端重连/心跳时主动 SYNC 兜底。
- **慢客户端隔离**: 出站入队连续 ~3 个扫描周期超时（ 默认 ~90s ）判定为僵死连接并主动断开。
- **SYNC 不依赖 Redis**: 同步帧不参与网关侧 SETNX 去重（ 游标语义幂等 ），Redis 故障不影响拉取链路。
- **优雅关闭**: 停止接入 → 停 Kafka 消费 → 排空（ 带超时 ）→ 关闭剩余连接。

## 性能备忘

- 每连接内存预算: 不压缩 ~64–96KiB；permessage-deflate 协商后 ~0.85–1.0MiB（ 小于 `min_compress_size`（ 默认 256B ）的消息自动走未压缩路径 ）。
- 上行每消息串行经过 Redis SETNX + Kafka（ acks=all ），单连接吞吐受两者 RTT 之和限制；集群吞吐先受 Redis/Kafka 制约，网关 CPU 通常不是第一瓶颈。
- 压测方案与容量推导见 [doc/websocket-performance-test-plan.md](./doc/websocket-performance-test-plan.md)。

### 评估结论（ 未实施项 ）

- **Redis 去重本地 LRU 预过滤**: 评估后不做。正常流量下重复帧占比极低，预过滤命中率可忽略，无法减少 Redis QPS，反而增加本地开销。若需削减该跳，方向应为去重下沉（ Kafka 幂等 producer + 后端按 message_id 幂等 ）。
- **Kafka 记录 protojson → proto 二进制**: 待与后端协同的变更窗口执行（ 双端需同步切换 ）；WS 帧层已支持 `synp.codec.type: proto`，可先行灰度。
