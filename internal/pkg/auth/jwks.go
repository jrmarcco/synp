package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultJWKSRefreshInterval = 10 * time.Minute
	defaultJWKSRefreshTimeout  = 5 * time.Second

	// negativeCacheTTL 为刷新失败的负缓存时长。
	// 重连风暴下大量 AUTH 携带未知 kid 时，
	// 失败结果短时缓存可防止对 JWKS 端点的群体冲击。
	negativeCacheTTL = 30 * time.Second
)

var ErrKidNotFound = fmt.Errorf("jwks: kid not found")

// JWK 是 JWKS 端点返回的单个 JSON Web Key ( RFC 7517 )。
type JWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`

	// RSA 公钥参数 ( base64url 编码 )。
	N string `json:"n"`
	E string `json:"e"`
}

type jwksDocument struct {
	Keys []JWK `json:"keys"`
}

// JWKSClient 负责从 JWKS 端点 ( /.well-known/jwks.json ) 拉取并缓存公钥集。
//
// 公钥定期刷新；token header 中的 kid 用于密钥轮换——
// 轮换时新旧公钥需在 JWKS 中并存一段重叠期 ( 见 proto/auth/v1/auth.proto )。
type JWKSClient struct {
	endpoint        string
	refreshInterval time.Duration
	httpClient      *http.Client

	mu   sync.RWMutex
	keys map[string]*rsa.PublicKey

	// refreshGroup 合并并发的显式刷新 ( 缓存未命中触发 )。
	refreshGroup singleflight.Group

	// lastRefreshFailedAt 为最近一次刷新失败的时间 ( 负缓存 )，
	// 窗口内不再发起刷新，防止重连风暴冲击 JWKS 端点。
	lastRefreshFailedAt atomic.Value // time.Time

	cancel context.CancelFunc
	closed chan struct{}
}

func NewJWKSClient(endpoint string, refreshInterval time.Duration) *JWKSClient {
	if refreshInterval <= 0 {
		refreshInterval = defaultJWKSRefreshInterval
	}

	return &JWKSClient{
		endpoint:        endpoint,
		refreshInterval: refreshInterval,
		httpClient: &http.Client{
			Timeout: defaultJWKSRefreshTimeout,
		},

		keys: make(map[string]*rsa.PublicKey),

		closed: make(chan struct{}),
	}
}

// Start 启动后台定期刷新。
// 使用内部 context 而非调用方的——调用方 ( 如 fx OnStart ) 的
// context 可能在启动流程结束后被取消，不应终止刷新循环。
func (c *JWKSClient) Start(_ context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	// 启动时先同步刷新一次，保证验签前有可用公钥。
	// 初始刷新失败不阻塞启动，由 KeyFunc 兜底同步刷新。
	_ = c.Refresh(ctx)

	go func() {
		ticker := time.NewTicker(c.refreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.closed:
				return
			case <-ticker.C:
				if err := c.Refresh(ctx); err != nil {
					// 刷新失败保留旧公钥集，等待下一轮。
					continue
				}
			}
		}
	}()
}

// Close 停止后台刷新。
func (c *JWKSClient) Close() {
	if c.cancel != nil {
		c.cancel()
	}

	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
}

// Refresh 拉取并替换整个公钥集。
// 失败会记录负缓存时间戳，窗口内的显式刷新 ( 缓存未命中触发 ) 被跳过。
func (c *JWKSClient) Refresh(ctx context.Context) error {
	if err := c.fetch(ctx); err != nil {
		c.lastRefreshFailedAt.Store(time.Now())
		return err
	}
	return nil
}

func (c *JWKSClient) fetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return fmt.Errorf("jwks: failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("jwks: failed to fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks: unexpected status code %d", resp.StatusCode)
	}

	doc := &jwksDocument{}
	if err = json.NewDecoder(resp.Body).Decode(doc); err != nil {
		return fmt.Errorf("jwks: failed to decode document: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, jwk := range doc.Keys {
		if jwk.Kty != "RSA" || jwk.Kid == "" {
			continue
		}

		pubKey, err := parseRSAJWK(jwk)
		if err != nil {
			continue
		}
		keys[jwk.Kid] = pubKey
	}

	c.mu.Lock()
	c.keys = keys
	c.mu.Unlock()

	c.lastRefreshFailedAt.Store(time.Time{})
	return nil
}

// KeyFunc 返回 kid 对应的 RSA 公钥。
// 缓存未命中时经 singleflight 合并同步刷新一次，
// 用于容忍新公钥刚轮换、本地缓存尚未更新的窗口；
// 重连风暴下并发未命中只产生一次端点请求。
func (c *JWKSClient) KeyFunc(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	c.mu.RLock()
	pubKey, ok := c.keys[kid]
	c.mu.RUnlock()
	if ok {
		return pubKey, nil
	}

	if err := c.refreshOnce(); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if pubKey, ok = c.keys[kid]; ok {
		return pubKey, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrKidNotFound, kid)
}

// refreshOnce 合并并发的显式刷新，并跳过负缓存窗口内的重复尝试。
func (c *JWKSClient) refreshOnce() error {
	// 负缓存窗口内不再冲击端点 ( 定期刷新 goroutine 稍后会重试 )。
	if failedAt, ok := c.lastRefreshFailedAt.Load().(time.Time); ok && !failedAt.IsZero() {
		if time.Since(failedAt) < negativeCacheTTL {
			return fmt.Errorf("jwks: refresh skipped within negative cache window")
		}
	}

	_, err, _ := c.refreshGroup.Do("refresh", func() (any, error) {
		refreshCtx, cancel := context.WithTimeout(context.Background(), defaultJWKSRefreshTimeout)
		defer cancel()

		// 不使用调用方 ctx: 鉴权请求的取消不应中断共享的刷新。
		return nil, c.Refresh(refreshCtx)
	})
	if err != nil {
		return fmt.Errorf("jwks: failed to refresh on cache miss: %w", err)
	}
	return nil
}

// parseRSAJWK 将 JWK 的 n / e ( base64url ) 解析为 rsa.PublicKey。
func parseRSAJWK(jwk JWK) (*rsa.PublicKey, error) {
	n, err := base64.RawURLEncoding.DecodeString(jwk.N)
	if err != nil {
		return nil, fmt.Errorf("jwks: failed to decode n: %w", err)
	}

	e, err := base64.RawURLEncoding.DecodeString(jwk.E)
	if err != nil {
		return nil, fmt.Errorf("jwks: failed to decode e: %w", err)
	}

	if len(n) == 0 || len(e) == 0 {
		return nil, fmt.Errorf("jwks: empty modulus or exponent")
	}

	exponent := 0
	for _, b := range e {
		if exponent >= (1 << 24) {
			return nil, fmt.Errorf("jwks: exponent overflow")
		}
		exponent = exponent<<8 | int(b)
	}

	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(n),
		E: exponent,
	}, nil
}
