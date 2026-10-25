#!/usr/bin/env bash
set -euo pipefail

# synp-gateway 本地联调启动 ( 经验固化自 gateway × backend 端到端联调 )。
#
# 用法: ./scripts/dev-up.sh [子命令]
#   check   依赖可达性检查 ( etcd/redis/kafka, 见 scripts/DEV.md 依赖来源 )
#   config  生成 etc/config-joint.yaml ( 注入 kafka mTLS 证书 )
#   build   编译 bin/synp
#   start   后台启动并等待 healthz
#   stop    停止服务
#   all     以上全流程 ( 默认 )
#
# 环境变量:
#   BACKEND_DIR   backend 仓库路径 ( 默认 ../backend, 仅用于 JWKS 指向 )
#   SUDO_PASS     sudo 密码 ( docker 命令经 sudo )

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKEND_DIR="${BACKEND_DIR:-$(cd "$ROOT_DIR/.." && pwd)/backend}"
LOG_FILE="${LOG_FILE:-/tmp/synp-dev.log}"
ADMIN_ADDR="127.0.0.1:9090"

cd "$ROOT_DIR"

if [[ -n "${SUDO_PASS:-}" ]]; then
    echo "$SUDO_PASS" | sudo -S -v 2>/dev/null
fi
DK() { sudo docker "$@"; }

log() { echo "[$(date +%H:%M:%S)] $*"; }

tcp_ok() { # tcp_ok <host:port>
    (exec 3<>/dev/tcp/${1%:*}/${1#*:}) 2>/dev/null && exec 3>&- 3<&- || return 1
}

cmd_check() {
    log "== 依赖检查 =="
    local fail=0
    for dep in "joint-etcd(kafka外的服务发现):127.0.0.1:12379" "joint-redis:127.0.0.1:16379" "kafka:127.0.0.1:19092"; do
        local name="${dep%%:*}" addr="${dep#*:}"
        if tcp_ok "$addr"; then log "✓ $name ($addr)"; else log "✗ $name ($addr) 不可达"; fail=1; fi
    done
    if [[ $fail -eq 1 ]]; then
        cat <<'TIP'
依赖缺失时按序执行 ( 详见 scripts/DEV.md ):
  1. kafka ( 本仓库 ):    cd scripts/docker/kafka && ./init.sh && sudo docker compose up -d
                          ( 记得把 .env 的 KAFKA_EXTERNAL_IP 改为 127.0.0.1 )
  2. redis + etcd ( 无 TLS 联调版, 位于 backend 仓库 ):
                          cd ../backend/scripts/docker/joint && sudo docker compose up -d
TIP
        exit 1
    fi
}

cmd_config() {
    log "== 生成 etc/config-joint.yaml =="
    python3 - "$BACKEND_DIR" <<'PYEOF'
import sys, yaml, pathlib

backend = pathlib.Path(sys.argv[1])
certs = pathlib.Path("scripts/docker/kafka/certs")
if not (certs / "client.key").exists():
    import subprocess
    subprocess.run(["keytool", "-importkeystore",
                    "-srckeystore", str(certs/"kafka.client.keystore.jks"),
                    "-destkeystore", str(certs/"client.p12"),
                    "-srcstoretype", "JKS", "-deststoretype", "PKCS12",
                    "-srcstorepass", "kafka-secret", "-deststorepass", "kafka-secret"],
                   check=True, capture_output=True)
    subprocess.run(["openssl", "pkcs12", "-in", str(certs/"client.p12"), "-nodes", "-nocerts",
                    "-passin", "pass:kafka-secret", "-out", str(certs/"client.key")],
                   check=True, capture_output=True)

ca, crt, key = ((certs/n).read_text() for n in ("ca-cert", "client.crt", "client.key"))

g = yaml.safe_load(open("etc/config.yaml"))
s = g["synp"]
s["admin"]["addr"] = "127.0.0.1:9090"
s["websocket"]["host"] = "127.0.0.1"
s["websocket"]["port"] = 10071
s["grpc"]["addr"] = "127.0.0.1:50051"
s["grpc"]["registry"]["key_prefix"] = "synp"
s["sync_service"] = {"service_name": "synp-backend", "insecure": True}
g["jwt"]["jwks_endpoint"] = "http://127.0.0.1:10081/.well-known/jwks.json"
g["etcd"] = {"endpoints": ["127.0.0.1:12379"], "dial_timeout": "5s", "tls": {"enabled": False}}
g["redis"] = {"addr": "127.0.0.1:16379"}
g["kafka"]["brokers"] = ["127.0.0.1:19092", "127.0.0.1:29092", "127.0.0.1:39092"]
g["kafka"]["tls"] = {"ca_cert": ca, "cert_key": key, "cert_pem": crt}
pathlib.Path("etc/config-joint.yaml").write_text(yaml.safe_dump(g, allow_unicode=True, sort_keys=False, width=4096))
print("  etc/config-joint.yaml written")
PYEOF
    log "✓ 配置生成 ( JWKS 指向 backend :10081, sync_service 经 etcd 发现 synp-backend )"
}

cmd_build() { make build >/dev/null && log "✓ bin/synp"; }

cmd_start() {
    if curl -sf "http://$ADMIN_ADDR/healthz" >/dev/null 2>&1; then log "✓ 服务已在运行"; return; fi
    log "== 启动 synp ( 日志: $LOG_FILE ) =="
    nohup ./bin/synp --config etc/config-joint.yaml > "$LOG_FILE" 2>&1 &
    echo $! > /tmp/synp-dev.pid
    local i=0
    until curl -sf "http://$ADMIN_ADDR/healthz" >/dev/null 2>&1; do
        ((i+=1)); [[ $i -gt 20 ]] && { log "✗ 启动超时, 最近日志:"; tail -5 "$LOG_FILE"; exit 1; }
        sleep 1
    done
    log "✓ synp 就绪: ws :10071 / grpc :50051 / admin :9090"
    log "  前置: backend 须已启动 ( JWKS 与 SyncService 均依赖它 )"
}

cmd_stop() {
    [[ -f /tmp/synp-dev.pid ]] && { kill "$(cat /tmp/synp-dev.pid)" 2>/dev/null || true; rm -f /tmp/synp-dev.pid; }
    pkill -f "bin/synp --config etc/config-joint.yaml" 2>/dev/null || true
    log "✓ 已停止"
}

case "${1:-all}" in
    check)  cmd_check ;;
    config) cmd_config ;;
    build)  cmd_build ;;
    start)  cmd_start ;;
    stop)   cmd_stop ;;
    all)    cmd_check; cmd_config; cmd_build; cmd_start; log "🎉 gateway 就绪" ;;
    *)      grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -14; exit 1 ;;
esac
