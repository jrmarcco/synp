# synp 中间件部署 ( docker compose )

本目录提供网关依赖中间件的本地/内网部署编排,均为 3 节点集群:

- [`etcd/`](./etcd) — gRPC 服务注册与发现 ( TLS + Ed25519 证书 )
- [`kafka/`](./kafka) — 消息上行/通知 topic ( KRaft + mTLS )
- [`observability/`](./observability) — Prometheus 抓取与告警 + Grafana ( 可选 )

所有 compose 共享外部网络 `jrmarcco_net` ( 各目录的 `init.sh` 会自动创建 )。

## 快速开始

```bash
# etcd
cd etcd && ./init.sh && docker compose up -d

# kafka
cd ../kafka && ./init.sh && docker compose up -d

# ( 可选 ) 观测栈
cd ../observability && docker compose up -d
```

## 变量与密钥约定

- **密码/地址不入版本库**:所有凭据经环境变量注入,kafka 侧有 `.env.example` 模板
  ( `init.sh` 会生成一份 `.env`,按需修改后再启动 )。
- **etcd 管理脚本**:`ETCD_ENDPOINTS` / `ETCD_USER` / `ETCD_PASSWORD` 环境变量可覆盖默认值。
- **镜像版本固定**:compose 中镜像 tag 默认固定,可经 `.env` 覆盖 (`KAFKA_IMAGE` 等)。

## 证书

各目录的 `certs_gen.sh` 生成自签证书:

- etcd: Ed25519, SAN 经 `SAN_IPS` / `SAN_DOMAINS` 环境变量定制;
- kafka: RSA mTLS, 密码与 compose 的 `KAFKA_TLS_KEY_PASSWORD` 同源
  ( 不一致会导致 broker TLS 握手失败 );

> 证书仅适用于开发/内网环境。生产环境请替换为受信 CA 签发的证书。

## 注意事项

- 容器间互访走 INTERNAL listener ( 如 kafka-ui 连 `kafka-N:9093` );
  EXTERNAL listener 的 advertised 地址是 `KAFKA_EXTERNAL_IP`,供集群外客户端 ( 网关、压测机 ) 使用。
- **advertised 地址必须从客户端真实可达且与其它 Kafka 集群不冲突**: 客户端 ( kafka-go 等 )
  取到 metadata 后会按 advertised 地址直连分区 leader。若该地址上存在另一套 Kafka
  ( 典型: 局域网旧集群恰好占用同一 IP ),症状是 `tls: certificate signed by unknown authority`。
  纯本机开发 ( Docker Desktop ) 用 `127.0.0.1` 即可。
- `apache/kafka` 镜像以 uid 1000 ( appuser ) 运行,`init.sh` 会将数据目录属主修正为 `1000:1000`,
  否则首次格式化 KRaft 元数据即失败 ( `AccessDeniedException` )。
- `coreos/etcd` 为 distroless 镜像 ( 无 shell ),healthcheck 必须用 exec 数组形式 ( 已配置 )。
- 数据经 bind mount 持久化在各自目录 ( `data-N` / `kafka-N-data` ),销毁集群时手动清理。
