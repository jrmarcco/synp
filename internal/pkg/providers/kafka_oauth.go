package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go/sasl"
)

var _ sasl.Mechanism = (*KafkaOAuthMechanism)(nil)

type KafkaOAuthMechanism struct {
	mu sync.RWMutex

	endpoint     string
	clientID     string
	clientSecret string

	accessToken string
	expiresAt   time.Time
}

// Name 返回 SASL 机制名称。
func (m *KafkaOAuthMechanism) Name() string {
	return "OAUTHBEARER"
}

func (m *KafkaOAuthMechanism) Start(ctx context.Context) (ss sasl.StateMachine, ir []byte, err error) {
	// 获取访问令牌。
	token, err := m.getAccessToken(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get access token: %w", err)
	}

	// 构造 OAuthBearer 初始响应 ( 格式 n,,\x01auth=Bearer <token>\x01\x01 )。
	//
	// 根据 RFC 7628 第 3.1 节，GS2 header 部分:
	// 	- "n" 表示客户端不支持 channel binding。
	// 	- 第一个逗号后为可选的 authzid，留空表示让 Kafka 从 token 的 sub 字段中提取 principal。
	// 	- 第二个逗号后为可选的保留字段，通常为空。
	//
	// 注意:
	// 	不能在 authzid 中指定用户名 ( 如 service-account-kafka-client )。
	//  因为 Kafka 会验证它是否与 token 中的 sub 字段匹配。
	initialResponse := fmt.Sprintf("n,,\x01auth=Bearer %s\x01\x01", token)

	return &kafkaOAuthSession{}, []byte(initialResponse), nil
}

func (m *KafkaOAuthMechanism) getAccessToken(ctx context.Context) (string, error) {
	m.mu.RLock()
	// 如果令牌未过期，直接返回令牌。
	if m.accessToken != "" && time.Now().Before(m.expiresAt) {
		defer m.mu.RUnlock()

		token := m.accessToken
		return token, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// 双重检查 ( double check )。
	if m.accessToken != "" && time.Now().Before(m.expiresAt) {
		token := m.accessToken
		return token, nil
	}

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", m.clientID)
	data.Set("client_secret", m.clientSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create http request: %w", err)
	}

	const httpTimeout = 5 * time.Second
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send http request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read http response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get access token with status [ %d ], body: %s", resp.StatusCode, string(body))
	}

	//nolint:tagliatelle // OAuth2 标准使用 snake_case
	var tokenRes struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenRes); err != nil {
		return "", fmt.Errorf("failed to parse http response body: %w", err)
	}

	// 缓存令牌，提前 60 秒过期避免出现边界情况。
	m.accessToken = tokenRes.AccessToken
	m.expiresAt = time.Now().Add(time.Duration(tokenRes.ExpiresIn-60) * time.Second) //nolint:mnd // 提前 60 秒刷新令牌

	return m.accessToken, nil
}

func NewKafkaOAuthMechanism(endpoint, clientID, clientSecret string) *KafkaOAuthMechanism {
	return &KafkaOAuthMechanism{
		endpoint:     endpoint,
		clientID:     clientID,
		clientSecret: clientSecret,
	}
}

var _ sasl.StateMachine = (*kafkaOAuthSession)(nil)

// kafkaOAuthSession 实现 SASL 会话状态机。
type kafkaOAuthSession struct{}

func (s *kafkaOAuthSession) Next(_ context.Context, _ []byte) (done bool, response []byte, err error) {
	// OAUTHBEARER 是单次往返认证，第一次响应后就完成，
	// 服务器可能发送空的成功响应。
	return true, nil, nil
}
