# syntax=docker/dockerfile:1

# ==================== 构建阶段 ====================
ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-alpine AS builder

# 代理可选注入 ( 国内网络: --build-arg GOPROXY=https://goproxy.cn,direct )，
# 不再写死进镜像层。
ARG GOPROXY=""

# 版本信息 ( make image 传入，经 ldflags 注入二进制，见 internal/pkg/buildinfo )。
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

WORKDIR /app

ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN if [ -n "$GOPROXY" ]; then go env -w GOPROXY="$GOPROXY"; fi \
    && go mod download

COPY . .
RUN go build -trimpath \
        -ldflags "-s -w \
            -X 'github.com/jrmarcco/synp/internal/pkg/buildinfo.Version=${VERSION}' \
            -X 'github.com/jrmarcco/synp/internal/pkg/buildinfo.Commit=${COMMIT}' \
            -X 'github.com/jrmarcco/synp/internal/pkg/buildinfo.Date=${DATE}'" \
        -o /out/synp ./cmd

# ==================== 运行阶段 ====================
# alpine + 非 root: 体积小、带 busybox ( HEALTHCHECK 的 wget、进容器排查 )，
# ca-certificates 供 JWKS 等公网 HTTPS 使用。
FROM alpine:3.21

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 synp

WORKDIR /app

COPY --from=builder /out/synp ./synp

USER synp

# 10071: WebSocket 数据面; 9090: 管理面 ( /metrics /healthz /readyz /version /debug/pprof )。
EXPOSE 10071 9090

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:9090/healthz || exit 1

# 配置文件不打包进镜像，运行时必须挂载:
#   docker run -v $PWD/etc/config.yaml:/app/etc/config.yaml:ro synp-gateway:<version>
ENTRYPOINT ["./synp"]
CMD ["--config", "etc/config.yaml"]
