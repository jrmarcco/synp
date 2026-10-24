#!/usr/bin/env bash
set -euo pipefail

echo "=== 生成 etcd 3 节点集群 Ed25519 TLS 证书 ==="

VALIDITY_DAYS=365

# SAN 可通过环境变量覆盖 ( 域名与 IP 均支持 )。
SAN_IPS="${SAN_IPS:-127.0.0.1,::1,192.168.3.3}"
SAN_DOMAINS="${SAN_DOMAINS:-localhost,node-1,node-2,node-3}"

build_san_lines() {
    local lines="" idx=1

    IFS=',' read -ra DOMAINS <<< "$SAN_DOMAINS"
    for domain in "${DOMAINS[@]}"; do
        domain=$(echo "$domain" | xargs)
        if [ -n "$domain" ]; then
            lines="${lines}DNS.${idx} = ${domain}\n"
            idx=$((idx + 1))
        fi
    done

    local ip_idx=1
    IFS=',' read -ra IPS <<< "$SAN_IPS"
    for ip in "${IPS[@]}"; do
        ip=$(echo "$ip" | xargs)
        if [ -n "$ip" ]; then
            lines="${lines}IP.${ip_idx} = ${ip}\n"
            ip_idx=$((ip_idx + 1))
        fi
    done

    echo "$lines"
}

SAN_LINES="$(printf '%b' "$(build_san_lines)")"
echo "SAN 配置:"
printf '%b\n' "${SAN_LINES}"

# 创建证书目录
CERT_DIR="./certs"
mkdir -p $CERT_DIR
cd $CERT_DIR

# 清理旧证书
rm -f *.pem *.csr *.srl *.conf

echo "1. 生成 CA 私钥 ( Ed25519 )"
openssl genpkey -algorithm Ed25519 -out ca-key.pem

echo "2. 生成 CA 证书"
openssl req -new -x509 -key ca-key.pem -days "$VALIDITY_DAYS" -out ca.pem \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Synp/OU=CA/CN=etcd-ca"

echo "3. 生成 etcd 服务器私钥 ( Ed25519 )"
openssl genpkey -algorithm Ed25519 -out server-key.pem

echo "4. 创建服务器证书配置文件（仅 DNS 名称）"
cat > server.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = CN
ST = Beijing
L = Beijing
O = Synp
OU = synp-etcd-server
CN = synp-etcd-server

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
# ===== 由 SAN_IPS / SAN_DOMAINS 环境变量生成 =====
$(echo "$SAN_LINES")
EOF

echo "5. 生成 etcd 服务器证书请求"
openssl req -new -key server-key.pem -out server.csr -config server.conf

echo "6. 签发 etcd 服务器证书"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem \
    -CAcreateserial -out server.pem -days "$VALIDITY_DAYS" \
    -extensions v3_req -extfile server.conf

echo "7. 生成 etcd 客户端私钥 ( Ed25519 )"
openssl genpkey -algorithm Ed25519 -out client-key.pem

echo "8. 创建客户端证书配置文件"
cat > client.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = CN
ST = Beijing
L = Beijing
O = Synp
OU = synp-etcd-client
CN = synp-etcd-client

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = synp-etcd-client
IP.1 = 127.0.0.1
EOF

echo "9. 生成 etcd 客户端证书请求"
openssl req -new -key client-key.pem -out client.csr -config client.conf

echo "10. 签发 etcd 客户端证书"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem \
    -CAcreateserial -out client.pem -days "$VALIDITY_DAYS" \
    -extensions v3_req -extfile client.conf

echo "11. 生成对等节点证书"
cp server.pem peer.pem
cp server-key.pem peer-key.pem

echo "12. 设置证书文件权限"
chmod 600 *-key.pem
chmod 644 *.pem
chmod 644 *.conf

echo "13. 清理临时文件"
rm -f *.csr *.srl

echo "=== 证书验证 ==="
echo "📋 CA 证书信息："
openssl x509 -in ca.pem -text -noout | grep -E "(Subject:|Public Key Algorithm:|Signature Algorithm:)"

echo -e "\n📋 服务器证书 SAN 扩展 ( 仅 DNS )"
openssl x509 -in server.pem -text -noout | grep -A 10 "Subject Alternative Name" || echo "未找到 SAN 扩展"

echo -e "\n📋 验证证书链："
openssl verify -CAfile ca.pem server.pem
openssl verify -CAfile ca.pem client.pem

echo -e "\n=== 证书文件列表 ==="
ls -la *.pem *.conf

echo -e "\n✅ etcd 3节点集群 Ed25519 TLS 证书生成成功 ( 仅 DNS 配置 )"
echo "📁 证书位置: $(pwd)"

echo -e "\n🔍 证书算法验证："
echo "CA 证书算法: $(openssl x509 -in ca.pem -text -noout | grep "Public Key Algorithm" | head -1 | awk '{print $NF}')"
echo "服务器证书算法: $(openssl x509 -in server.pem -text -noout | grep "Public Key Algorithm" | head -1 | awk '{print $NF}')"
echo "客户端证书算法: $(openssl x509 -in client.pem -text -noout | grep "Public Key Algorithm" | head -1 | awk '{print $NF}')"
