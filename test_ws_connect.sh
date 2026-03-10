#!/usr/bin/env bash
set -euo pipefail

AUTH_SERVER="http://127.0.0.1:10081"
ACCOUNT="jrmarcco@gmail.com"
ACCOUNT_TYPE="email"
CREDENTIAL="<passwd>"
WS_SERVER="ws://127.0.0.1:10071"
ENABLE_COMPRESSION=false

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --auth-server <addr>   Auth server address (default: ${AUTH_SERVER})
  --account <account>         Account (default: ${ACCOUNT})
  --account-type <type>       Account type (default: ${ACCOUNT_TYPE})
  --credential <credential>   Credential (default: ${CREDENTIAL})
  --ws-server <addr>     WebSocket server address (default: ${WS_SERVER})
  --enable-compression        Enable permessage-deflate header
  -h, --help                  Show this help message
EOF
}

generate_request_id() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen
    return
  fi

  if [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
    return
  fi

  # Fallback for environments without uuidgen/proc uuid.
  echo "req-$(date +%s)-$RANDOM"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --auth-server)
      AUTH_SERVER="$2"
      shift 2
      ;;
    --account)
      ACCOUNT="$2"
      shift 2
      ;;
    --account-type)
      ACCOUNT_TYPE="$2"
      shift 2
      ;;
    --credential)
      CREDENTIAL="$2"
      shift 2
      ;;
    --ws-server)
      WS_SERVER="$2"
      shift 2
      ;;
    --enable-compression)
      ENABLE_COMPRESSION=true
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

# 登录获取 accessToken
echo ">>> 正在登录..."
LOGIN_RESPONSE=$(curl -s -X POST "${AUTH_SERVER}/api/v1/auth/sign-in" \
  -H "Content-Type: application/json" \
  -d "{
    \"account\": \"${ACCOUNT}\",
    \"accountType\": \"${ACCOUNT_TYPE}\",
    \"credential\": \"${CREDENTIAL}\"
  }")

# 提取 accessToken ( 需要 jq 工具 )
ACCESS_TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.data.accessToken')
REQUEST_ID="$(generate_request_id)"

# 客户端推荐连接参数：
# 	- 无压缩：
#		websocat -v \
# 		  "wss://gateway.example.com/ws?device=pc" \
#   	  --header "Authorization: Bearer <JWT>" \
#   	  --header "X-Request-Id: $(uuidgen)"
#
#  - 启用压缩：
# websocat -v \
#   	"wss://gateway.example.com/ws?device=pc" \
#   	  --header "Authorization: Bearer <JWT>" \
#   	  --header "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=15" \
#   	  --header "X-Request-Id: $(uuidgen)"
WS_HEADERS=(
  --header "Authorization: Bearer ${ACCESS_TOKEN}"
  --header "X-Request-Id: ${REQUEST_ID}"
)

if [[ "${ENABLE_COMPRESSION}" == "true" ]]; then
  WS_HEADERS+=(
    --header "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=15"
  )
fi

websocat -v \
  "${WS_SERVER}?device=pc" \
  "${WS_HEADERS[@]}"
