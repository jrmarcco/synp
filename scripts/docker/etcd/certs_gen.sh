#!/bin/bash
set -e

echo "=== 生成 etcd 3 节点集群 Ed25519 TLS 证书 ( 仅 DNS 配置 )==="

VALIDITY_DAYS=365

# 创建证书目录
CERT_DIR="./certs"
mkdir -p $CERT_DIR
cd $CERT_DIR

# 清理旧证书
rm -f *.pem *.csr *.srl *.conf

echo "1. 生成 CA 私钥 (Ed25519)"
openssl genpkey -algorithm Ed25519 -out ca-key.pem

echo "2. 生成 CA 证书"
openssl req -new -x509 -key ca-key.pem -days "$VALIDITY_DAYS" -out ca.pem \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=Synp/OU=CA/CN=etcd-ca"

echo "3. 生成 etcd 服务器私钥 (Ed25519)"
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
# ===== 本地回环（保留 localhost 用于测试）=====
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.2 = ::1

# ===== Docker 容器名称，集群情况下配置多个 =====
DNS.2 = node-1
DNS.3 = node-2
DNS.4 = node-3

# ===== 如果需要外部访问，添加主机名 或 IP =====
# DNS.5 = etcd.yourdomain.com
IP.3 = 192.168.3.3
EOF

echo "5. 生成 etcd 服务器证书请求"
openssl req -new -key server-key.pem -out server.csr -config server.conf

echo "6. 签发 etcd 服务器证书"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem \
    -CAcreateserial -out server.pem -days "$VALIDITY_DAYS" \
    -extensions v3_req -extfile server.conf

echo "7. 生成 etcd 客户端私钥 (Ed25519)"
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
