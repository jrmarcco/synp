#!/usr/bin/env bash
set -euo pipefail

# ====== 配置参数 ======
IM_SERVER_ADDR="http://localhost:10081"
ACCOUNT="jrmarcco@gmail.com"
ACCOUNT_TYPE="email"
CREDENTIAL="<passwd>"

# ====== 登录获取 Token ======
echo ">>> 正在登录..."
LOGIN_RESPONSE=$(curl -s -X POST "${IM_SERVER_ADDR}/api/v1/auth/sign-in" \
  -H "Content-Type: application/json" \
  -d "{
    \"account\": \"${ACCOUNT}\",
    \"accountType\": \"${ACCOUNT_TYPE}\",
    \"credential\": \"${CREDENTIAL}\"
  }")

# 提取 accessToken ( 需要 jq 工具 )
ACCESS_TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.data.accessToken')

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
ADDR="ws://192.168.3.3:10071"

websocat -v \
  "${ADDR}?device=pc" \
  --header "Authorization: Bearer ${ACCESS_TOKEN}" \
  --header "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=15" \
  --header "X-Request-Id: $(uuidgen)"
