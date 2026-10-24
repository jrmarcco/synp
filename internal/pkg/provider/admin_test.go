package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// newGetRequest 构造带 context 的 GET 请求 ( noctx 规范 )。
func newGetRequest(ctx context.Context, path string) *http.Request {
	return httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil)
}

func TestHandleHealthz(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	handleHealthz(rec, newGetRequest(t.Context(), "/healthz"))

	if rec.Code != http.StatusOK {
		t.Fatalf("healthz 应返回 200, 实际 %d", rec.Code)
	}

	body := map[string]string{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz 应返回 JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("healthz 状态应为 ok, 实际 %q", body["status"])
	}
}

func TestHandleVersion(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	handleVersion(rec, newGetRequest(t.Context(), "/version"))

	if rec.Code != http.StatusOK {
		t.Fatalf("version 应返回 200, 实际 %d", rec.Code)
	}

	body := map[string]string{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("version 应返回 JSON: %v", err)
	}
	for _, key := range []string{"version", "commit", "date"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("version 应包含字段 %s, 实际 %v", key, body)
		}
	}
}

func TestReadinessDegradedWithoutDeps(t *testing.T) {
	t.Parallel()

	// etcd 未配置、redis 不可达: 应返回 503 与分项状态而非 panic。
	handler := newReadinessHandler(nil, (*clientv3.Client)(nil))

	rec := httptest.NewRecorder()
	handler(rec, newGetRequest(t.Context(), "/readyz"))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("依赖不可用时 readyz 应返回 503, 实际 %d", rec.Code)
	}

	body := readinessResult{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("readyz 应返回 JSON: %v", err)
	}
	if body.Status != statusDegraded {
		t.Fatalf("readyz 状态应为 degraded, 实际 %q", body.Status)
	}
	if body.Checks["etcd"] != "not configured" {
		t.Fatalf("etcd 分项应为 not configured, 实际 %q", body.Checks["etcd"])
	}
}
