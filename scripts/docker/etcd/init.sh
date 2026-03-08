#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Etcd 集群初始化脚本"
echo ""

# 1. 创建数据目录
NUM_DIRS=${1:-3}
echo "📁 创建 $NUM_DIRS 个数据目录..."
for i in $(seq 1 $NUM_DIRS); do
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
