#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 观测栈初始化脚本"

# 数据目录预创建与属主修正:
# compose 以 root 运行时自动创建的 bind mount 目录归 root,
# 而 prometheus ( uid 65534 ) / grafana ( uid 472 ) 容器内非 root, 无法写入。
chown_to() {
    local dir="$1" uid="$2"
    [ -d "$dir" ] || mkdir -p "$dir"
    if [ "$(stat -c %u "$dir")" != "$uid" ]; then
        if [ "$(id -u)" = "0" ]; then
            chown -R "$uid:$uid" "$dir"
        else
            sudo chown -R "$uid:$uid" "$dir"
        fi
    fi
}

chown_to ./prometheus-data 65534
chown_to ./grafana-data 472

echo "✅ 数据目录就绪"
echo "👉 启动: docker compose up -d"
echo "ℹ️  网关抓取目标: prometheus/targets/gateway.json ( file_sd 热加载 )"
