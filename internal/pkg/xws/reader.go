package xws

import (
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
	"github.com/gobwas/ws/wsutil"
)

// ErrFrameTooLarge 表示入站帧超过大小上限。
// 异常大帧可能造成内存放大和 OOM，超限连接应被关闭。
var ErrFrameTooLarge = errors.New("websocket frame exceeds size limit")

// DefaultMaxFrameBytes 为默认的单帧大小上限 ( 64KiB )。
const DefaultMaxFrameBytes = 64 * 1024

// Reader 是对 gobwas/ws 的封装，用于读取 WebSocket 消息。
type Reader struct {
	conn net.Conn

	reader *wsutil.Reader

	// maxFrameBytes 为单个数据帧的大小上限，超限返回 ErrFrameTooLarge。
	maxFrameBytes int64

	messageState *wsflate.MessageState
	flateReader  *wsflate.Reader

	handlerFunc wsutil.FrameHandlerFunc
}

func (r *Reader) Read() ([]byte, error) {
	for {
		header, err := r.reader.NextFrame()
		if err != nil {
			return nil, err
		}

		if header.OpCode.IsControl() {
			if err := r.handlerFunc(header, r.reader); err != nil {
				return nil, err
			}
			continue
		}

		// 帧大小上限: 异常大帧 ( 恶意或异常客户端 ) 直接断连，
		// 避免无界 ReadAll 造成内存放大。
		if r.maxFrameBytes > 0 && header.Length > r.maxFrameBytes {
			return nil, fmt.Errorf("%w: frame length %d exceeds limit %d", ErrFrameTooLarge, header.Length, r.maxFrameBytes)
		}

		if r.messageState.IsCompressed() {
			r.flateReader.Reset(r.reader)
			return io.ReadAll(r.flateReader)
		}

		return io.ReadAll(r.reader)
	}
}

func NewServerSideReader(conn net.Conn, maxFrameBytes int64) *Reader {
	if maxFrameBytes <= 0 {
		maxFrameBytes = DefaultMaxFrameBytes
	}

	messageState := &wsflate.MessageState{}
	handlerFunc := wsutil.ControlFrameHandler(conn, ws.StateServerSide)

	return &Reader{
		conn: conn,
		reader: &wsutil.Reader{
			Source:         conn,
			State:          ws.StateServerSide | ws.StateExtended,
			Extensions:     []wsutil.RecvExtension{messageState},
			OnIntermediate: handlerFunc,
		},
		maxFrameBytes: maxFrameBytes,
		messageState:  messageState,
		flateReader: wsflate.NewReader(nil, func(r io.Reader) wsflate.Decompressor {
			return flate.NewReader(r)
		}),
		handlerFunc: handlerFunc,
	}
}

func NewClientSideReader(conn net.Conn, maxFrameBytes int64) *Reader {
	if maxFrameBytes <= 0 {
		maxFrameBytes = DefaultMaxFrameBytes
	}

	messageState := &wsflate.MessageState{}
	handlerFunc := wsutil.ControlFrameHandler(conn, ws.StateClientSide)

	return &Reader{
		conn: conn,
		reader: &wsutil.Reader{
			Source:         conn,
			State:          ws.StateClientSide | ws.StateExtended,
			Extensions:     []wsutil.RecvExtension{messageState},
			OnIntermediate: handlerFunc,
		},
		maxFrameBytes: maxFrameBytes,
		messageState:  messageState,
		flateReader: wsflate.NewReader(nil, func(r io.Reader) wsflate.Decompressor {
			return flate.NewReader(r)
		}),
		handlerFunc: handlerFunc,
	}
}
