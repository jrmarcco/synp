// jointtest 命令: gateway × backend 消息主链路端到端联调客户端。
//
// 用法 ( 在 gateway 目录下 ):
//
//	go run ./cmd/jointtest --a a@joint.test --b b@joint.test --password hermet@2026
//
// 验证链路: 登录 → WS AUTH → UPSTREAM 单聊 → UPSTREAM_ACK →
// 对端 NOTIFY(latest_seq) → SYNC 增量拉取 ( 消息本体 ) → 会话搜索 ( ES )。
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// 帧与端点常量 ( 契约见 api/proto )。
const (
	backendBase = "http://127.0.0.1:10081"
	gatewayWS   = "ws://127.0.0.1:10071"

	// protojson 枚举以名字序列化 ( 发送与比较都用名字，数字亦可为服务端解析 )。
	cmdAuth     = "COMMAND_TYPE_AUTH"
	cmdAuthAck  = "COMMAND_TYPE_AUTH_ACK"
	cmdUpstream = "COMMAND_TYPE_UPSTREAM"
	cmdUpAck    = "COMMAND_TYPE_UPSTREAM_ACK"
	cmdSync     = "COMMAND_TYPE_SYNC"
	cmdSyncAck  = "COMMAND_TYPE_SYNC_ACK"
	cmdNotify   = "COMMAND_TYPE_NOTIFY"
)

// testMessageID 测试帧 ID 前缀 ( 每次运行追加纳秒时间戳，
// 网关按 message_id 帧去重，固定 ID 的重复运行会被判重丢弃 )。
var testMessageID = fmt.Sprintf("joint-up-%d", time.Now().UnixNano())

// frame WS 帧 ( protojson: body 为 base64 )。
type frame struct {
	CommandType string `json:"commandType"`
	MessageID   string `json:"messageId"`
	Body        string `json:"body,omitempty"`
}

type tokenResp struct {
	Code int `json:"code"`
	Data struct {
		AccessToken string `json:"accessToken"`
	} `json:"data"`
}

// wsConn 极简 WS 客户端连接 ( 客户端帧掩码由 wsutil 处理 )。
type wsConn struct {
	c net.Conn
}

func dialWS(addr string) (*wsConn, error) {
	conn, _, _, err := ws.Dial(context.Background(), addr)
	if err != nil {
		return nil, err
	}
	return &wsConn{c: conn}, nil
}

func (w *wsConn) send(f frame) error {
	val, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return wsutil.WriteClientMessage(w.c, ws.OpText, val)
}

func (w *wsConn) recv(timeout time.Duration) (frame, error) {
	if err := w.c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return frame{}, err
	}
	msgs, err := wsutil.ReadServerMessage(w.c, nil)
	if err != nil {
		return frame{}, err
	}
	var buf []byte
	for _, m := range msgs {
		buf = append(buf, m.Payload...)
	}
	var f frame
	if err := json.Unmarshal(buf, &f); err != nil {
		return frame{}, fmt.Errorf("bad frame %q: %w", string(buf), err)
	}
	return f, nil
}

func (w *wsConn) close() { _ = w.c.Close() }

func main() {
	emailA := flag.String("a", "a@joint.test", "sender email")
	emailB := flag.String("b", "b@joint.test", "receiver email")
	password := flag.String("password", "hermet@2026", "account password")
	groupID := flag.Uint64("group-id", 0, "群聊模式: 目标群 ID ( 0 = 单聊 )")
	flag.Parse()

	must := func(step string, err error) {
		if err != nil {
			panic(fmt.Sprintf("[%s] %v", step, err))
		}
	}

	pass("1/7 登录双方")
	tokenA := signIn(*emailA, *password)
	tokenB := signIn(*emailB, *password)
	uidB := userID(tokenB)
	fmt.Printf("   sender=%s receiver_uid=%d\n", *emailA, uidB)

	pass("2/7 建立 WS 并 AUTH")
	connA, err := dialWS(gatewayWS)
	must("dial A", err)
	defer connA.close()
	connB, err := dialWS(gatewayWS)
	must("dial B", err)
	defer connB.close()

	authConn(connA, tokenA, "A")
	authConn(connB, tokenB, "B")

	destinationID := uidB
	destinationType := "single"
	if *groupID > 0 {
		destinationID = *groupID
		destinationType = "group"
	}
	pass("3/7 A 发送 UPSTREAM")
	upBody, _ := json.Marshal(map[string]any{
		"destinationId":   destinationID,
		"destinationType": destinationType,
		"content":         "hello from jointtest",
		"contentType":     "text",
	})
	must("upstream", connA.send(frame{
		CommandType: cmdUpstream,
		MessageID:   testMessageID,
		Body:        b64(upBody),
	}))

	pass("4/7 A 收 UPSTREAM_ACK")
	f, err := connA.recv(5 * time.Second)
	must("recv ack", err)
	if f.CommandType != cmdUpAck || f.MessageID != testMessageID {
		panic(fmt.Sprintf("expect UPSTREAM_ACK(joint-up-1), got %+v", f))
	}
	printBody("   ack", f.Body)

	pass("5/7 B 收 NOTIFY → SYNC 拉增量 ( 单聊内联 / 小群内联 )")
	nf, err := connB.recv(10 * time.Second)
	must("recv notify", err)
	if nf.CommandType != cmdNotify {
		panic(fmt.Sprintf("expect NOTIFY, got commandType=%s body=%s", nf.CommandType, nf.Body))
	}
	printBody("   notify", nf.Body)

	syncBody, _ := json.Marshal(map[string]any{"cursor": 0})
	must("sync", connB.send(frame{CommandType: cmdSync, MessageID: testMessageID + "-sync", Body: b64(syncBody)}))

	sf, err := connB.recv(8 * time.Second)
	must("recv sync ack", err)
	if sf.CommandType != cmdSyncAck || sf.MessageID != testMessageID+"-sync" {
		panic(fmt.Sprintf("expect SYNC_ACK(%s-sync), got %+v", testMessageID, sf))
	}

	// protojson 中 64 位整数序列化为字符串。
	var syncAck struct {
		IsSuccess bool `json:"isSuccess"`
		Response  struct {
			Entries []struct {
				Seq     string `json:"seq"`
				Message *struct {
					ConversationID string `json:"conversationId"`
					MessageID      string `json:"messageId"`
					SenderID       string `json:"senderId"`
				} `json:"message"`
			} `json:"entries"`
			LatestSeq string `json:"latestSeq"`
		} `json:"response"`
	}
	decodeBody(sf.Body, &syncAck)
	if !syncAck.IsSuccess || len(syncAck.Response.Entries) == 0 {
		panic(fmt.Sprintf("sync_ack 无增量条目: %s", sf.Body))
	}
	// 收件箱按 seq 升序，本轮消息在最后一条。
	e := syncAck.Response.Entries[len(syncAck.Response.Entries)-1]
	if e.Message == nil || e.Message.MessageID != testMessageID {
		panic(fmt.Sprintf("增量末条应为 %s, got %s", testMessageID, sf.Body))
	}
	fmt.Printf("   seq=%s conversation=%s sender=%s latest=%s ✓ 消息本体已同步\n",
		e.Seq, e.Message.ConversationID, e.Message.SenderID, syncAck.Response.LatestSeq)
	convID := e.Message.ConversationID

	pass("6/7 会话搜索 ( ES 查询侧 )")
	time.Sleep(2 * time.Second) // 等异步索引
	searchURL := fmt.Sprintf("%s/api/v1/message/search?conversationId=%s&keyword=jointtest", backendBase, url.QueryEscape(convID))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, searchURL, nil)
	must("new search request", err)
	req.Header.Set("x-access-token", "Bearer "+tokenB)
	resp, err := http.DefaultClient.Do(req)
	must("search", err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	fmt.Printf("   http=%d %s\n", resp.StatusCode, truncate(string(body), 200))
	if resp.StatusCode != http.StatusOK {
		panic("search failed")
	}

	pass("7/7 消息主链路端到端通过")
}

func pass(s string) { fmt.Printf("== %s\n", s) }

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// authConn 发送 AUTH 帧并等待成功应答 ( 失败则 panic )。
func authConn(conn *wsConn, token, who string) {
	authBody, _ := json.Marshal(map[string]string{"token": token})
	if err := conn.send(frame{CommandType: cmdAuth, MessageID: "auth-" + who, Body: b64(authBody)}); err != nil {
		panic(err)
	}
	ack, err := conn.recv(5 * time.Second)
	if err != nil {
		panic(err)
	}
	var authAck struct {
		Success bool   `json:"success"`
		Error   string `json:"errorMessage"`
	}
	decodeBody(ack.Body, &authAck)
	if ack.CommandType != cmdAuthAck || !authAck.Success {
		panic(fmt.Sprintf("AUTH 失败 (%s): %s", who, authAck.Error))
	}
	fmt.Printf("   %s AUTH 成功 ✓\n", who)
}

func signIn(email, password string) string {
	body := map[string]string{"account": email, "accountType": "email", "credential": password}
	val, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		backendBase+"/api/v1/auth/sign-in", bytes.NewReader(val))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)
	var tr tokenResp
	if err := json.Unmarshal(raw, &tr); err != nil || tr.Data.AccessToken == "" {
		panic(fmt.Sprintf("sign-in failed: http=%d body=%s", resp.StatusCode, string(raw)))
	}
	return tr.Data.AccessToken
}

// userID 从 JWT payload 解出 uid ( 不验签, 仅联调观测 )。
func userID(token string) uint64 {
	parts := bytes.Split([]byte(token), []byte("."))
	if len(parts) != 3 {
		panic("bad jwt")
	}
	payload, err := base64.RawURLEncoding.DecodeString(string(parts[1]))
	if err != nil {
		panic(err)
	}
	var claims struct {
		UserID uint64 `json:"user_id"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		panic(err)
	}
	return claims.UserID
}

func printBody(prefix, b64Body string) {
	raw, err := base64.StdEncoding.DecodeString(b64Body)
	if err != nil {
		return
	}
	fmt.Printf("%s %s\n", prefix, truncate(string(raw), 200))
}

func decodeBody(b64Body string, v any) {
	raw, _ := base64.StdEncoding.DecodeString(b64Body)
	if err := json.Unmarshal(raw, v); err != nil {
		panic(err)
	}
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
