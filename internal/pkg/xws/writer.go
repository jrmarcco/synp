package xws

import (
	"compress/flate"
	"io"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
	"github.com/gobwas/ws/wsutil"
)

// Writer 是对 gobwas/ws 的封装，用于写入 WebSocket 消息。
type Writer struct {
	writer *wsutil.Writer

	messageState *wsflate.MessageState
	flateWriter  *wsflate.Writer

	// minCompressSize 为启用压缩的最小 payload 大小，
	// 更小的消息走未压缩路径 ( 压缩小消息的开销高于收益 )。
	minCompressSize int
}

func (w *Writer) Write(payload []byte) (int, error) {
	if w.messageState.IsCompressed() && len(payload) >= w.minCompressSize {
		return w.writeCompressed(payload)
	}

	// 小消息降级为未压缩帧：需要先关闭本轮压缩标记再写。
	if w.messageState.IsCompressed() {
		w.messageState.SetCompressed(false)
		defer w.messageState.SetCompressed(true)
	}

	return w.writeUncompressed(payload)
}

func (w *Writer) writeCompressed(payload []byte) (int, error) {
	w.flateWriter.Reset(w.writer)

	n, err := w.flateWriter.Write(payload)
	if err != nil {
		return 0, err
	}

	// 完成 flate wirter 压缩，即写入尾标记。
	err = w.flateWriter.Close()
	if err != nil {
		return 0, err
	}

	return n, w.writer.Flush()
}

func (w *Writer) writeUncompressed(payload []byte) (int, error) {
	n, err := w.writer.Write(payload)
	if err != nil {
		return 0, err
	}

	return n, w.writer.Flush()
}

// NewServerSideWriter 创建服务端 writer。
// compressed 为协商结果；level 为压缩级别 ( <=0 使用默认 )；
// minCompressSize 为启用压缩的最小 payload 大小 ( <=0 使用默认 256B )。
func NewServerSideWriter(dst io.Writer, compressed bool, level, minCompressSize int) *Writer {
	if level <= 0 || level > 9 {
		level = flate.DefaultCompression
	}
	if minCompressSize <= 0 {
		minCompressSize = 256
	}

	messageState := &wsflate.MessageState{}
	messageState.SetCompressed(compressed)

	state := ws.StateServerSide | ws.StateExtended
	// 下行以 text 帧承载 ( payload 为 protojson 文本 ):
	// 浏览器 WebSocket.onmessage 对 binary 帧投递 Blob，
	// 未按帧类型分支处理的客户端会静默丢弃全部下行。
	opCode := ws.OpText

	rtn := &Writer{
		writer:          wsutil.NewWriter(dst, state, opCode),
		messageState:    messageState,
		minCompressSize: minCompressSize,
	}

	if compressed {
		rtn.flateWriter = wsflate.NewWriter(nil, func(w io.Writer) wsflate.Compressor {
			fw, _ := flate.NewWriter(w, level)
			return fw
		})
	}

	rtn.writer.SetExtensions(messageState)

	return rtn
}
