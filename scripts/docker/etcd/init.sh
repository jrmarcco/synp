#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Etcd 集群初始化脚本"
echo ""

# 0. 外部网络 ( compose 依赖 external 网络, 不存在则创建 )
NETWORK_NAME="jrmarcco_net"
if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK_NAME}$"; then
    echo "🌐 创建外部网络 ${NETWORK_NAME}..."
    docker network create "${NETWORK_NAME}"
else
    echo "ℹ️  外部网络 ${NETWORK_NAME} 已存在"
fi
echo ""

# 1. 创建数据目录 ( 与 docker-compose.yaml 的 3 节点一一对应 )
echo "📁 创建 3 个数据目录..."
for i in 1 2 3; do
    DATA_DIR="./data-$i"
    if [ -d "$DATA_DIR" ]; then
        echo "  📁 数据目录 $DATA_DIR 已存在"
    else
        echo "  📁 创建数据目录 $DATA_DIR"
        mkdir -p "$DATA_DIR"
    fi
done
echo "✅ 数据目录创建完成"
echo ""

# 2. 生成证书
if [ ! -f "./certs/server.pem" ] || [ ! -f "./certs/ca.pem" ]; then
    echo "🔐 生成 TLS 证书..."
    chmod +x ./certs_gen.sh
    ./certs_gen.sh
    echo "✅ 证书生成完成"
    echo ""
else
    echo "ℹ️  证书已存在，跳过生成"
    echo ""
fi

echo "🎉 初始化完成！"
