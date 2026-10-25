#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 共享外部网络 ( 与 etcd 等其它中间件复用 )。
NETWORK_NAME="jrmarcco_net"

echo "🚀 Kafka 集群初始化脚本"
echo ""

# 1. 外部网络 ( 不存在则创建 )
if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK_NAME}$"; then
    echo "🌐 创建外部网络 ${NETWORK_NAME}..."
    docker network create "${NETWORK_NAME}"
else
    echo "ℹ️  外部网络 ${NETWORK_NAME} 已存在"
fi
echo ""

# 2. .env ( 不存在则生成一份可用的默认配置 )
if [ ! -f "./.env" ]; then
    echo "📝 生成 .env..."

    # KRaft CLUSTER_ID: 22 字符 url-safe base64。
    CLUSTER_ID="$(openssl rand -base64 16 | tr '+/' '-_' | tr -d '=')"

    # 默认对外广播宿主机首个 IP。
    # 注意: advertised 地址必须从客户端真实可达, 且不能与其它 Kafka 集群冲突——
    # 客户端会按 advertised 地址直连分区 leader, 地址错误会撞上别的集群 ( TLS 报 unknown authority )。
    # 纯本机开发 ( Docker Desktop ) 建议 127.0.0.1。
    EXTERNAL_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
    if [ -z "${EXTERNAL_IP}" ]; then
        EXTERNAL_IP="127.0.0.1"
    fi

    cat > ./.env <<EOF
KAFKA_CLUSTER_ID=${CLUSTER_ID}
KAFKA_EXTERNAL_IP=${EXTERNAL_IP}
KAFKA_TLS_KEY_PASSWORD=kafka-secret
KAFKA_IMAGE=apache/kafka:4.0.0
KAFKA_UI_IMAGE=provectuslabs/kafka-ui:v0.7.2
KAFKA_UI_USER=admin
KAFKA_UI_PASSWORD=change-me
EOF

    echo "✅ .env 已生成 ( 广播 IP: ${EXTERNAL_IP}, 请按需修改 )"
else
    echo "ℹ️  .env 已存在，跳过生成"
fi
echo ""

# 3. 证书 ( 不存在或缺失关键文件时生成 )
if [ ! -f "./certs/kafka.server.keystore.jks" ] || [ ! -f "./certs/kafka.client.keystore.jks" ]; then
    echo "🔐 生成 mTLS 证书..."
    chmod +x ./certs_gen.sh
    ./certs_gen.sh
    echo "✅ 证书生成完成"
else
    echo "ℹ️  证书已存在，跳过生成"
fi
echo ""

# 4. 数据目录 ( apache/kafka 镜像以 uid 1000 的 appuser 运行,
#    compose 以 root 创建的 bind mount 目录需修正属主, 否则写入被拒 )
echo "📁 准备数据目录 ( 属主修正为 1000:1000 )..."
for i in 1 2 3; do
    DATA_DIR="./kafka-${i}-data"
    [ -d "${DATA_DIR}" ] || mkdir -p "${DATA_DIR}"
done
if [ "$(id -u)" = "0" ]; then
    chown -R 1000:1000 kafka-1-data kafka-2-data kafka-3-data
else
    sudo chown -R 1000:1000 kafka-1-data kafka-2-data kafka-3-data
fi
echo "✅ 数据目录就绪"
echo ""

echo "🎉 初始化完成！"
echo "👉 启动: docker compose up -d"
