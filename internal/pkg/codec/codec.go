package codec

//go:generate mockgen -source=codec.go -destination=mock/codec.mock.go -package=codecmock -typed Codec

// Codec 是消息编码/解码接口。
// 注意：
//
//	Codec 用于将 websocket 连接收发的消息进行编码/解码。
//	即前端 ( 业务客户端 ) 和网关之间收发的消息进行编码/解码。
type Codec interface {
	Name() string
	Marshal(val any) ([]byte, error)
	Unmarshal(data []byte, val any) error
}
