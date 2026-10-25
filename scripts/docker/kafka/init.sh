#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ============================================================
# 可配置默认值 ( 仅在生成 .env 时使用; 同名环境变量可覆盖 )
# 生产部署: 直接修改此处默认值, 或运行前 export 对应变量,
# 亦可在 .env 生成后直接改 .env ( .env 为最终生效来源 )。
# ============================================================

# 共享外部网络 ( 与 etcd 等其它中间件复用 )。
NETWORK_NAME="${NETWORK_NAME:-jrmarcco_net}"

# 镜像版本 ( 固定 tag 保证可复现部署 )。
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.4.0}"
KAFKA_UI_IMAGE="${KAFKA_UI_IMAGE:-provectuslabs/kafka-ui:v0.7.2}"

# Kafka UI 登录凭据。
KAFKA_UI_USER="${KAFKA_UI_USER:-admin}"
KAFKA_UI_PASSWORD="${KAFKA_UI_PASSWORD:-change-me}"

# TLS 密钥库密码 ( certs_gen.sh 与 compose 共用同一来源 )。
# 留空则首次生成 .env 时随机生成 ( url-safe 字符, 无需转义 )。
KAFKA_TLS_KEY_PASSWORD="${KAFKA_TLS_KEY_PASSWORD:-}"

# 对外广播地址 ( EXTERNAL listener 的 advertised IP )。
# 留空则自动探测宿主机首个 IP, 探测失败回退 127.0.0.1。
KAFKA_EXTERNAL_IP="${KAFKA_EXTERNAL_IP:-}"

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

    # 密码随机生成: 仅含 url-safe 字符, 规避 .env 与 shell 的转义/引号问题。
    if [ -z "${KAFKA_TLS_KEY_PASSWORD}" ]; then
        KAFKA_TLS_KEY_PASSWORD="$(openssl rand -base64 24 | tr '+/' '-_' | tr -d '=')"
    fi

    # 默认对外广播宿主机首个 IP。
    # 注意: advertised 地址必须从客户端真实可达, 且不能与其它 Kafka 集群冲突——
    # 客户端会按 advertised 地址直连分区 leader, 地址错误会撞上别的集群 ( TLS 报 unknown authority )。
    # 纯本机开发 ( Docker Desktop ) 建议 127.0.0.1。
    if [ -z "${KAFKA_EXTERNAL_IP}" ]; then
        KAFKA_EXTERNAL_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
        [ -n "${KAFKA_EXTERNAL_IP}" ] || KAFKA_EXTERNAL_IP="127.0.0.1"
    fi

    cat > ./.env <<EOF
KAFKA_CLUSTER_ID=${CLUSTER_ID}
KAFKA_EXTERNAL_IP=${KAFKA_EXTERNAL_IP}
KAFKA_TLS_KEY_PASSWORD=${KAFKA_TLS_KEY_PASSWORD}
KAFKA_IMAGE=${KAFKA_IMAGE}
KAFKA_UI_IMAGE=${KAFKA_UI_IMAGE}
KAFKA_UI_USER=${KAFKA_UI_USER}
KAFKA_UI_PASSWORD=${KAFKA_UI_PASSWORD}
EOF

    echo "✅ .env 已生成 ( 广播 IP: ${KAFKA_EXTERNAL_IP}, TLS 密码已随机写入, 请按需修改 )"
else
    echo "ℹ️  .env 已存在，跳过生成"
fi
echo ""

# .env 是唯一事实来源: 导出后供后续证书生成使用,
# 保证 keystore 密码与 compose 中 broker/kafka-ui 配置同源。
set -a
# shellcheck disable=SC1091
source ./.env
set +a

# 3. 证书 ( 不存在或缺失关键文件时生成 )
if [ ! -f "./certs/kafka.server.keystore.jks" ] || [ ! -f "./certs/kafka.client.keystore.jks" ]; then
    echo "🔐 生成 mTLS 证书..."
    chmod +x ./certs_gen.sh
    ./certs_gen.sh
    echo "✅ 证书生成完成"
else
    echo "ℹ️  证书已存在，跳过生成"

    # 密码一致性校验: .env 中的密码必须能打开既有 keystore, 否则 broker 启动即 TLS 握手失败
    # ( 常见诱因: 删掉 .env 重新生成导致密码翻新, 或手动改过密码未同步 )。
    if command -v keytool >/dev/null 2>&1; then
        if ! keytool -list -keystore ./certs/kafka.server.keystore.jks \
            -storepass "${KAFKA_TLS_KEY_PASSWORD}" >/dev/null 2>&1; then
            echo "⚠️  .env 中 KAFKA_TLS_KEY_PASSWORD 与既有证书密码不匹配！"
            echo "    请将 .env 中的密码改回证书生成时的值, 或删除 ./certs 目录后重跑本脚本重新生成证书。"
            exit 1
        fi
    else
        echo "⚠️  未检测到 keytool, 跳过证书密码一致性校验"
    fi
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
