package codec

import (
	"testing"

	commonv1 "github.com/jrmarcco/synp-api/api/go/common/v1"
	messagev1 "github.com/jrmarcco/synp-api/api/go/message/v1"
)

// TestDownlinkFrameShape 验证下行帧的 protojson 输出形态 ( 契约关键行为 ):
//   - 枚举序列化为枚举名字符串;
//   - 零值 bool 字段照常输出 ( isSuccess / hasMore / cursorExpired 不缺省 )。
func TestDownlinkFrameShape(t *testing.T) {
	frame := &messagev1.Message{
		CommandType: commonv1.CommandType_COMMAND_TYPE_NOTIFY,
		MessageId:   "frame-1",
		Body:        []byte(`{"latestSeq":43}`),
	}

	val, err := JSONMarshalOptions.Marshal(frame)
	if err != nil {
		t.Fatalf("failed to marshal frame: %v", err)
	}
	if !contains(val, `"COMMAND_TYPE_NOTIFY"`) {
		t.Fatalf("expected enum name string in %s", val)
	}

	// 失败应答: isSuccess=false 与空 errorMessage 均不缺省。
	failAck := &messagev1.SyncAckPayload{
		IsSuccess:    false,
		ErrorMessage: "failed to sync: internal error, please retry",
	}
	val, err = JSONMarshalOptions.Marshal(failAck)
	if err != nil {
		t.Fatalf("failed to marshal fail ack: %v", err)
	}
	if !contains(val, `"isSuccess":false`) {
		t.Fatalf("expected isSuccess:false present in %s", val)
	}

	// 成功空响应: hasMore / cursorExpired 零值不缺省。
	resp := &messagev1.SyncResponse{}
	val, err = JSONMarshalOptions.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal sync response: %v", err)
	}
	if !contains(val, `"hasMore":false`) || !contains(val, `"cursorExpired":false`) {
		t.Fatalf("expected hasMore/cursorExpired present in %s", val)
	}
}

func contains(haystack []byte, needle string) bool {
	return len(needle) == 0 || indexOf(haystack, needle) >= 0
}

func indexOf(haystack []byte, needle string) int {
	n := len(needle)
	for i := 0; i+n <= len(haystack); i++ {
		match := true
		for j := 0; j < n; j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
