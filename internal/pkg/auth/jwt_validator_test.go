package auth

import (
	"encoding/json"
	"testing"
)

func TestJwtClaimsUnmarshalStringIDs(t *testing.T) {
	// 后端签发的 token 中 ID claims 为十进制字符串。
	val := `{"business_id":"1","user_id":"134605228232032256","session_id":"s"}`

	claims := &jwtClaims{}
	if err := json.Unmarshal([]byte(val), claims); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if claims.BusinessID != 1 || claims.UserID != 134605228232032256 {
		t.Fatalf("unexpected ids: %d / %d", claims.BusinessID, claims.UserID)
	}
}

func TestJwtClaimsUnmarshalNumberIDs(t *testing.T) {
	// 升级前签发的存量 token 中 ID claims 为数字字面量。
	val := `{"business_id":1,"user_id":134605228232032256,"session_id":"s"}`

	claims := &jwtClaims{}
	if err := json.Unmarshal([]byte(val), claims); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if claims.BusinessID != 1 || claims.UserID != 134605228232032256 {
		t.Fatalf("unexpected ids: %d / %d", claims.BusinessID, claims.UserID)
	}
}
