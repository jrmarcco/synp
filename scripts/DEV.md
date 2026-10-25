# synp-gateway 本地开发/联调启动指南

> 联调经验固化 ( gateway × backend 端到端验证, 2026-10 )。
> 一键脚本: `./scripts/dev-up.sh` ( 子命令见脚本头部注释 )。

## 拓扑与端口

| 组件 | 端口 | 来源 |
|---|---|---|
| kafka ×3 ( mTLS ) + ui | 19092/29092/39092 | 本仓库 `scripts/docker/kafka` |
| joint-redis / joint-etcd ( 无 TLS ) | 16379 / 12379 | `../backend/scripts/docker/joint` |
| synp: ws / grpc / admin | 10071 / 50051 / 9090 | `etc/config-joint.yaml` |

gateway 启动的两个外部依赖链:

1. **JWKS**: `http://127.0.0.1:10081/.well-known/jwks.json` —— backend 签发 token
   的公钥端点，**backend 必须先启动**，否则连接 AUTH 全部失败
   ( JWKS 拉取有 30s 负缓存，backend 后启动需等缓存过期或重连 )。
2. **SyncService**: 经 etcd 服务发现 `synp:///synp-backend` —— SYNC/CONVERSATION_SYNC
   帧透传给 backend，backend 未注册时这些帧以错误应答 ( 不影响 AUTH/UPSTREAM )。

## 快速开始

```bash
# 前置: backend 已就绪 ( 依赖容器 + JWKS + SyncService 都在 backend 侧 )
cd ../backend && SUDO_PASS=<密码> ./scripts/dev-up.sh

cd ../gateway
SUDO_PASS=<密码> ./scripts/dev-up.sh     # check→config→build→start
go run ./cmd/jointtest                   # 端到端验证 ( 单聊; --group-id 群聊 )
./scripts/dev-up.sh stop
```

## 独立启动 ( 不联调 backend )

kafka 用本仓库 mTLS 三节点 ( `scripts/docker/kafka`，记得 `.env` 的
`KAFKA_EXTERNAL_IP=127.0.0.1` )；redis/etcd 无 TLS 联调版在
`../backend/scripts/docker/joint` ( 单文件 compose，跨仓库复用 )。
`dev-up.sh check` 失败时会打印逐项补起命令。

kafka 客户端证书: Go 客户端需要 PEM，`dev-up.sh config` 自动从 JKS
导出 `client.key`/`client.crt` 并内联进 `etc/config-joint.yaml`
( 密码 `kafka-secret` 与 certs_gen.sh 同源，改过则需同步 )。

## 已知坑 ( 联调实录 )

- **kafka TLS 强制**: 本工程 kafka 客户端无证书不启动——不是 bug，联调必走 mTLS 集群。
- **帧 message_id 去重**: 按 ID 幂等丢弃重复帧 ( at-least-once 契约 )，
  测试工具每次运行必须用唯一 ID。
- **protojson 帧**: 枚举序列化为名字 ( `COMMAND_TYPE_*` )、uint64/bytes 为
  字符串/base64；`Message.body` 是 base64 的子消息 JSON。
- **etcd key 前缀**: 联调配置 `key_prefix: synp` ( 与 backend 的
  synp-backend 注册一致 )；backend 未起时 SYNC 帧报错属预期。

## 排查入口

```
curl 127.0.0.1:9090/healthz   # 存活
curl 127.0.0.1:9090/readyz    # redis / etcd
curl 127.0.0.1:9090/version   # 构建版本
tail -f /tmp/synp-dev.log     # 服务日志
```
