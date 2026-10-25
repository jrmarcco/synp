package codec

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var _ Codec = (*JSONCodec)(nil)

// JSONMarshalOptions 是 protojson 统一序列化选项。
//
// EmitUnpopulated: 零值字段照常输出 ( isSuccess / hasMore / cursorExpired 等
// bool 为 false 时不再缺省 )。文档示例均按字段存在编写，
// 客户端不应为"字段缺省"做特判。
var JSONMarshalOptions = protojson.MarshalOptions{
	EmitUnpopulated: true,
}

type JSONCodec struct{}

func (c *JSONCodec) Name() string {
	return "json"
}

func (c *JSONCodec) Marshal(val any) ([]byte, error) {
	protoMsg, ok := val.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("failed to marshal message: invalid message type, expected proto.Message, got %T", val)
	}
	return JSONMarshalOptions.Marshal(protoMsg)
}

func (c *JSONCodec) Unmarshal(data []byte, val any) error {
	protoMsg, ok := val.(proto.Message)
	if !ok {
		return fmt.Errorf("failed to unmarshal message: invalid message type, expected proto.Message, got %T", val)
	}
	return protojson.Unmarshal(data, protoMsg)
}

func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}
