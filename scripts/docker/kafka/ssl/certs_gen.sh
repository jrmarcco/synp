#!/bin/bash
set -e

CERTS_DIR="./certs"
PASSWORD="kafka-secret"
VALIDITY=3650
KEYSIZE=2048

# 可配置的 SAN 参数
# 可以通过环境变量或命令行参数覆盖
SAN_IPS="${SAN_IPS:-127.0.0.1,192.168.3.3}"
SAN_DOMAINS="${SAN_DOMAINS:-kafka-1,kafka-2,kafka-3,localhost}"

# 构建 SAN 字符串
build_san_string() {
    local san_string=""

    # 添加域名
    IFS=',' read -ra DOMAINS <<< "$SAN_DOMAINS"
    for domain in "${DOMAINS[@]}"; do
        domain=$(echo "$domain" | xargs)  # 去除前后空格
        if [ -n "$domain" ]; then
            if [ -n "$san_string" ]; then
                san_string="${san_string},"
            fi
            san_string="${san_string}DNS:${domain}"
        fi
    done

    # 添加 IP
    IFS=',' read -ra IPS <<< "$SAN_IPS"
    for ip in "${IPS[@]}"; do
        ip=$(echo "$ip" | xargs)  # 去除前后空格
        if [ -n "$ip" ]; then
            if [ -n "$san_string" ]; then
                san_string="${san_string},"
            fi
            san_string="${san_string}IP:${ip}"
        fi
    done

    echo "$san_string"
}

SAN_STRING=$(build_san_string)

echo "=========================================="
echo "生成 Kafka 双向 TLS ( mTLS ) 证书"
echo "=========================================="
echo "SAN 配置:"
echo "  域名: ${SAN_DOMAINS}"
echo "  IP: ${SAN_IPS}"
echo "  完整 SAN: ${SAN_STRING}"
echo "=========================================="

# 创建 secrets 目录
mkdir -p ${CERTS_DIR}

# 1. 生成 CA 证书
echo ">>> 生成 CA 密钥对和证书..."
openssl req -new -x509 -keyout ${CERTS_DIR}/ca-key -out ${CERTS_DIR}/ca-cert -days ${VALIDITY} \
    -subj "/CN=kafka-ca/OU=Kafka/O=Kafka/L=Beijing/ST=Beijing/C=CN" \
    -passout pass:${PASSWORD}

# 2. 创建共享的密钥库和信任库 ( 用于简化配置 )
echo ">>> 创建共享的服务器密钥库..."
keytool -keystore ${CERTS_DIR}/kafka.server.keystore.jks \
    -alias kafka \
    -validity ${VALIDITY} \
    -genkey \
    -keyalg RSA \
    -keysize ${KEYSIZE} \
    -storepass ${PASSWORD} \
    -keypass ${PASSWORD} \
    -dname "CN=kafka,OU=Kafka,O=Kafka,L=Beijing,ST=Beijing,C=CN" \
    -ext "SAN=${SAN_STRING}"

# 生成 CSR
keytool -keystore ${CERTS_DIR}/kafka.server.keystore.jks \
    -alias kafka \
    -certreq \
    -file ${CERTS_DIR}/kafka.csr \
    -storepass ${PASSWORD} \
    -keypass ${PASSWORD}

# 签名
openssl x509 -req -CA ${CERTS_DIR}/ca-cert -CAkey ${CERTS_DIR}/ca-key \
    -in ${CERTS_DIR}/kafka.csr \
    -out ${CERTS_DIR}/kafka.signed.crt \
    -days ${VALIDITY} \
    -CAcreateserial \
    -passin pass:${PASSWORD} \
    -extfile <(printf "subjectAltName=${SAN_STRING}")

# 导入 CA 到 keystore
keytool -keystore ${CERTS_DIR}/kafka.server.keystore.jks \
    -alias CARoot \
    -import \
    -file ${CERTS_DIR}/ca-cert \
    -storepass ${PASSWORD} \
    -noprompt

# 导入签名证书
keytool -keystore ${CERTS_DIR}/kafka.server.keystore.jks \
    -alias kafka \
    -import \
    -file ${CERTS_DIR}/kafka.signed.crt \
    -storepass ${PASSWORD} \
    -noprompt

# 3. 创建服务器信任库 ( 用于验证客户端证书 )
echo ">>> 创建服务器信任库..."
keytool -keystore ${CERTS_DIR}/kafka.server.truststore.jks \
    -alias CARoot \
    -import \
    -file ${CERTS_DIR}/ca-cert \
    -storepass ${PASSWORD} \
    -noprompt

# 4. 创建客户端密钥库和信任库 ( 用于双向 TLS )
echo ">>> 创建客户端证书 ( 用于 mTLS ) ..."
keytool -keystore ${CERTS_DIR}/kafka.client.keystore.jks \
    -alias client \
    -validity ${VALIDITY} \
    -genkey \
    -keyalg RSA \
    -keysize ${KEYSIZE} \
    -storepass ${PASSWORD} \
    -keypass ${PASSWORD} \
    -dname "CN=kafka-client,OU=Kafka,O=Kafka,L=Beijing,ST=Beijing,C=CN"

keytool -keystore ${CERTS_DIR}/kafka.client.keystore.jks \
    -alias client \
    -certreq \
    -file ${CERTS_DIR}/client.csr \
    -storepass ${PASSWORD} \
    -keypass ${PASSWORD}

openssl x509 -req -CA ${CERTS_DIR}/ca-cert -CAkey ${CERTS_DIR}/ca-key \
    -in ${CERTS_DIR}/client.csr \
    -out ${CERTS_DIR}/client.signed.crt \
    -days ${VALIDITY} \
    -CAcreateserial \
    -passin pass:${PASSWORD}

keytool -keystore ${CERTS_DIR}/kafka.client.keystore.jks \
    -alias CARoot \
    -import \
    -file ${CERTS_DIR}/ca-cert \
    -storepass ${PASSWORD} \
    -noprompt

keytool -keystore ${CERTS_DIR}/kafka.client.keystore.jks \
    -alias client \
    -import \
    -file ${CERTS_DIR}/client.signed.crt \
    -storepass ${PASSWORD} \
    -noprompt

keytool -keystore ${CERTS_DIR}/kafka.client.truststore.jks \
    -alias CARoot \
    -import \
    -file ${CERTS_DIR}/ca-cert \
    -storepass ${PASSWORD} \
    -noprompt

# 5. 导出客户端 PEM 格式证书 ( 供 Go/Python 等非 Java 客户端使用 )
echo ">>> 导出客户端 PEM 格式证书..."

# 将 JKS 转换为 PKCS12
keytool -importkeystore \
    -srckeystore ${CERTS_DIR}/kafka.client.keystore.jks \
    -destkeystore ${CERTS_DIR}/client.p12 \
    -deststoretype PKCS12 \
    -srcalias client \
    -srcstorepass ${PASSWORD} \
    -deststorepass ${PASSWORD} \
    -destkeypass ${PASSWORD} \
    -noprompt

# 从 PKCS12 导出客户端私钥 ( 无密码保护，便于程序读取 )
openssl pkcs12 -in ${CERTS_DIR}/client.p12 -passin pass:"${PASSWORD}" -nocerts -nodes | openssl rsa -out ${CERTS_DIR}/client.key
    # -out ${CERTS_DIR}/client.key \
    #  | openssl rsa -out ${CERTS_DIR}/client.key

# 从 PKCS12 导出客户端证书
openssl pkcs12 -in ${CERTS_DIR}/client.p12 -passin pass:"${PASSWORD}" -clcerts -nokeys | openssl x509 -out ${CERTS_DIR}/client.crt
    # -out ${CERTS_DIR}/client.crt \


# 清理临时文件
rm -f .srl ${CERTS_DIR}/*.csr ${CERTS_DIR}/*.signed.crt ${CERTS_DIR}/client.p12 ${CERTS_DIR}/*.srl

echo "=========================================="
echo "双向 TLS 证书生成完成！"
echo "=========================================="
echo ""
echo "=== 服务器端文件 ( Kafka Broker 使用 ) ==="
echo "  服务器密钥库: ${CERTS_DIR}/kafka.server.keystore.jks"
echo "  服务器信任库: ${CERTS_DIR}/kafka.server.truststore.jks"
echo ""
echo "=== Java 客户端文件 ==="
echo "  客户端密钥库: ${CERTS_DIR}/kafka.client.keystore.jks"
echo "  客户端信任库: ${CERTS_DIR}/kafka.client.truststore.jks"
echo ""
echo "=== Go/Python 等客户端文件 ( PEM 格式 ) ==="
echo "  CA 证书:     ${CERTS_DIR}/ca.crt"
echo "  客户端证书:   ${CERTS_DIR}/client.crt"
echo "  客户端私钥:   ${CERTS_DIR}/client.key"
echo ""
echo "=== 密码 ==="
echo "  所有密钥库密码: ${PASSWORD}"
echo ""
echo "=========================================="
echo "Kafka Broker 配置 ( server.properties ):"
echo "=========================================="
cat << 'EOF'
# 启用 SSL
listeners=SSL://:9093
advertised.listeners=SSL://your-host:9093
security.inter.broker.protocol=SSL

# SSL 配置
ssl.keystore.location=/path/to/kafka.server.keystore.jks
ssl.keystore.password=kafka-secret
ssl.key.password=kafka-secret
ssl.truststore.location=/path/to/kafka.server.truststore.jks
ssl.truststore.password=kafka-secret

# 启用双向 TLS ( 客户端认证 )
ssl.client.auth=required

# 推荐的 SSL 协议配置 ( 推荐使用 TLSv1.3 )
ssl.enabled.protocols=TLSv1.2,TLSv1.3
ssl.protocol=TLSv1.3
EOF
echo "=========================================="
