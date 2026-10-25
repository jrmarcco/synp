# synp WebSocket 网关前端接入指南

> 适用版本: synp-api **v0.1.0** 及以上 ( 帧协议契约见 [api 工程](../../api/README.md) )。
> 文中「网关默认值」按当前部署配置给出, 以实际部署配置为准。

## 1. 总览

```
前端 ( 业务客户端 )
  │  WebSocket ( Message 帧, JSON 编码 )
  ▼
gateway ( WebSocket 网关 )
  │  盖章身份投递 Kafka 上行 topic          ←── UPSTREAM
  │  通知 topic 广播消费 → NOTIFY 帧        ──→ 触发你拉取
  │  代理调用后端 SyncService              ←── SYNC / CONVERSATION_SYNC
  ▼
backend ( 业务后端: 落库、seq 分配、发通知 )
```

核心同步模型 ( 务必理解, 影响所有客户端行为设计 ):

1. **落库即事实**: 消息只有被后端持久化并分配单调递增的 seq 才算成立。
2. **推送即通知**: 服务端主动下发的 `NOTIFY` 帧**只携带最新 seq, 不携带消息本体**。
3. **游标驱动**: 客户端维护本地 cursor, 收到通知后比对, 落后就用 `SYNC` 增量拉取。
4. **at-least-once**: 全链路可能重复投递 ( 通知会重复、SYNC 结果可能重复 ),
   客户端必须按 `seq` ( 收件箱 ) / `message_id` ( 业务消息 ) **幂等去重**。

## 2. 建立连接

### URL

```
ws(s)://<gateway-host>:10071?device=<mobile|tablet|pc>
```

| 参数 | 说明 |
|---|---|
| `device` | 设备类型 `mobile` / `tablet` / `pc`; 缺省或非法值按 `unknown` 处理。**同一账号同一 `device` 的新连接会顶掉旧连接** ( 顶号 )。 |

请求头 ( 可选 ):

| Header | 说明 |
|---|---|
| `Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=15` | 启用压缩。浏览器 `WebSocket` API 自动协商, 无需手动处理。 |
| `X-Auto-Close: true` | 允许网关在连接空闲约 2 分钟后主动关闭 ( 一般不传 )。 |

**注意: token 不要放在 URL 或请求头里。** 鉴权在连接建立后的第一帧完成
( 见下一节 ), token 因此不落入访问日志与代理日志。

### 帧格式 ( Message )

所有帧 ( 上行与下行 ) 统一为 JSON 文本, 结构:

```json
{
  "commandType": "COMMAND_TYPE_UPSTREAM",
  "messageId": "01JD8Z3V9GKQWXFQPXB1H5NH6R",
  "body": "eyJrZXkiOiJ2YWx1ZSJ9"
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `commandType` | string | 指令, 枚举名 ( 见 [指令一览](#5-指令一览) )。 |
| `messageId` | string | **由前端生成**的帧唯一标识, 建议 ULID。作用: ① 请求/应答关联 ( ACK 帧原样回填 ); ② 帧去重 ( 见 [message_id 约定](#message-id-约定) )。 |
| `body` | string | 帧体, **base64 编码**的 JSON。先构造业务 payload 的 JSON, 再整体 base64。 |

> 字段名说明: 网关按 protojson 编解码, 序列化输出 lowerCamelCase
> ( `commandType` / `messageId` / `latestSeq` ... ); 解析时同时兼容
> snake_case ( `command_type` ... )。前端建议统一用 lowerCamelCase。
> 枚举值序列化为**枚举名字符串** ( 如 `"COMMAND_TYPE_SYNC"` ), 不是数字。

**body 编解码示例**:

```typescript
// 编码: payload JSON → base64
const body = btoa(JSON.stringify({ token: jwt }));

// 解码: base64 → payload JSON
const payload = JSON.parse(atob(frame.body));
```

> Node.js 环境用 `Buffer.from(obj, 'utf8').toString('base64')` /
> `Buffer.from(str, 'base64').toString('utf8')`。

### 连接参数与限制 ( 网关默认值 )

| 项 | 值 | 说明 |
|---|---|---|
| 鉴权超时 | **15s** | 连接建立后 15s 内未完成 AUTH, 网关关闭连接。 |
| 单帧大小上限 | **64KiB** | 超限连接被关闭。拆分业务大消息, 或走业务侧对象存储。 |
| 每连接读限流 | **10 帧/s** ( 平滑 ) | 超速帧会被延迟处理, 持续超速建议自查。 |
| 鉴权后读超时 | 90s | 连续 90s 无任何上行帧 ( 含心跳 ) 连接可能被断开。**心跳必发**。 |

## 3. 鉴权 ( 第一帧必须是 AUTH )

```
前端                                gateway
  │  ① AUTH {token}                   │
  │ ─────────────────────────────────► │  本地验签 ( RS256 + JWKS )
  │                                    │  校验: 签名 / exp / aud
  │  ② AUTH_ACK {success}              │  绑定身份、注册路由
  │ ◄───────────────────────────────── │
```

**第一步: 向业务后端登录接口换取 JWT** ( 网关不签发 token )。
token 为短周期凭证 ( 建议 ≤ 2h ), 过期后需要重新登录获取并重连。

**第二步: 连接建立后立即发送 AUTH 帧** ( `messageId` 必填 ):

```typescript
const authFrame = {
  commandType: "COMMAND_TYPE_AUTH",
  messageId: ulid(),
  body: btoa(JSON.stringify({ token: jwt })),   // AuthPayload
};
ws.send(JSON.stringify(authFrame));
```

AUTH_ACK 帧 ( `messageId` 与请求一致 ) 的 body 解码后:

```json
{ "success": true }
```

```json
{ "success": false, "errorMessage": "invalid token: token is expired" }
```

规则:

- 鉴权完成前发送任何其它指令 → **连接直接被关闭**。
- AUTH 失败 → 网关发送 `AUTH_ACK{success:false}` 后**关闭连接**, 拿新 token 重试。
- token 过期表现为 AUTH 失败; 存量连接上 token 过期不会立即断开,
  但断线重连时需要新 token。
- 重复发送 AUTH ( 已鉴权 ) → 幂等返回 `AUTH_ACK{success:true}`。

## 4. 心跳

```typescript
// 建议 30s 一次, 网关原样回显同一帧作为 pong。
// 心跳同时刷新该连接在服务端路由中的存活状态, 不发心跳的连接会被路由淘汰。
const heartbeat = {
  commandType: "COMMAND_TYPE_HEARTBEAT",
  messageId: ulid(),
  body: "",
};
ws.send(JSON.stringify(heartbeat));
```

心跳帧 body 为空。回显帧与发送帧完全一致, 可直接比对 `messageId`。

## 5. 指令一览

| 指令 | 方向 | body ( 解码后 ) | 说明 |
|---|---|---|---|
| `HEARTBEAT` | C → G ( 回显 ) | 空 | 心跳 |
| `AUTH` | C → G | `{"token": "..."}` | 首帧鉴权 |
| `AUTH_ACK` | G → C | `{"success": bool, "errorMessage": "..."}` | 鉴权应答 |
| `UPSTREAM` | C → G | 业务自定义 | 上行业务消息 |
| `UPSTREAM_ACK` | G → C | `{"isSuccess": bool, "timestampMs": 123, "errorMessage": "..."}` | 网关级接收回执 |
| `SYNC` | C → G | `{"cursor": 42, "limit": 100}` | 收件箱增量同步 |
| `SYNC_ACK` | G → C | `{"isSuccess": bool, "errorMessage": "...", "response": {...}}` | 同步应答 |
| `CONVERSATION_SYNC` | C → G | `{"conversationId": "...", "cursor": 7, "limit": 100}` | 会话 timeline 同步 ( 大群 ) |
| `CONVERSATION_SYNC_ACK` | G → C | `{"isSuccess": bool, "errorMessage": "...", "response": {...}}` | 会话同步应答 |
| `NOTIFY` | G → C | `{"latestSeq": 100}` | 服务端通知, 触发同步 |
| `REDIRECT` | G → C | 空 ( 预留 ) | 重定向 ( 当前版本不会下发 ) |
| `RATE_LIMIT_EXCEEDED` | G → C | 空 ( 预留 ) | 限流提示 ( 当前版本不会下发 ) |

## 6. 发送消息 ( UPSTREAM )

```
前端                                gateway                     backend
  │  ① UPSTREAM {业务body}            │                            │
  │ ─────────────────────────────────► │  盖章身份 → Kafka 上行 topic │
  │  ② UPSTREAM_ACK ( 网关级回执 )      │ ──────────────────────────► │  校验/落库/分配 seq
  │ ◄───────────────────────────────── │                            │  写收件箱、发通知
  │                                    │      ③ NOTIFY {latestSeq}   │
  │  ④ 比对游标 → SYNC                  │ ◄────────────────────────── │
  │ ─────────────────────────────────► │ ── 调 SyncService ────────► │
  │  ⑤ SYNC_ACK {entries}              │ ◄────────────────────────── │
  │ ◄───────────────────────────────── │                            │
```

```typescript
const upstream = {
  commandType: "COMMAND_TYPE_UPSTREAM",
  messageId: ulid(),
  body: btoa(JSON.stringify({
    conversationId: "conv_1001",
    content: "hello",
    // ... 业务自定义字段, 网关与后端均不解释
  })),
};
ws.send(JSON.stringify(upstream));
```

**UPSTREAM_ACK 的语义 ( 重要 )**:

- `isSuccess: true` 只表示**网关已接收该帧** ( 已成功投递 Kafka ), 不代表业务处理成功。
- 业务级失败 ( 如被禁言、非群成员 ) **不会**通过 ACK 表达——后端会将其作为一条
  系统消息走同步链路下发, 由业务侧定义。
- `isSuccess: false` 常见于网关侧故障 ( Kafka 不可用 ), 客户端可原 `messageId` 重试。

**message_id 约定**:

- 由前端生成, 建议 [ULID](https://github.com/ulid/javascript) ( 同毫秒可排序, 利于排查 ):
  ```bash
  npm install ulid
  ```
  ```typescript
  import { ulid } from "ulid";
  const messageId = ulid();
  ```
- 网关按 `( 账号, message_id )` 做 **10 秒窗口**去重: 窗口内同一 `messageId`
  的 UPSTREAM 重复帧会被丢弃, 这是 at-least-once 链路的第一层幂等。
- **不要**用同一 `messageId` 发送不同内容。

## 7. 接收通知与增量同步 ( NOTIFY → SYNC )

### NOTIFY 帧

服务端在你 ( 或你的其它会话方 ) 有新消息落库时下发, body 解码后:

```json
{ "latestSeq": 100 }
```

处理逻辑:

```typescript
if (notify.latestSeq > localCursor) {
  // 游标落后 → 拉取增量
  sync(notify.latestSeq);
}
// latestSeq <= localCursor → 忽略 ( 通知可能重复 )
```

**通知允许丢失和重复** ( 只影响实时性, 不影响正确性 ):
丢失的增量靠重连/心跳兜底 ( 见 [重连策略](#10-断线重连) ), 重复靠 `latestSeq` 比对消化。

### SYNC 帧

```typescript
const sync = {
  commandType: "COMMAND_TYPE_SYNC",
  messageId: ulid(),
  body: btoa(JSON.stringify({
    cursor: localCursor,  // 本端已应用的最大收件箱 seq; 首次/新设备传 0
    limit: 100,           // 单页上限; 0 用服务端默认
  })),
};
ws.send(JSON.stringify(sync));
```

SYNC_ACK 的 body ( 成功时 ) 解码后:

```json
{
  "isSuccess": true,
  "response": {
    "entries": [
      {
        "seq": 43,
        "message": {
          "conversationId": "conv_1001",
          "messageId": "01JD8...",
          "senderId": 100,
          "body": "eyJjb250ZW50IjoiaGVsbG8ifQ==",
          "sentAt": "2026-10-24T12:00:00Z"
        }
      },
      {
        "seq": 44,
        "conversationUpdate": {
          "conversationId": "conv_9001",
          "conversationLatestSeq": 87
        }
      }
    ],
    "latestSeq": 100,
    "hasMore": false,
    "cursorExpired": false
  }
}
```

字段语义:

| 字段 | 说明 |
|---|---|
| `entries[].seq` | 该条目在你收件箱中的序号, **严格单调递增**, 以此去重并推进游标 |
| `entries[].message` | 写扩散条目 ( 单聊/小群 ): 消息本体**内联**在此, 一次拉取即可渲染; `body` 仍是 base64 业务 JSON |
| `entries[].conversationUpdate` | 读扩散指针 ( 大群 ): 只告诉你该会话有更新, 需再发 `CONVERSATION_SYNC` 拉正文 |
| `latestSeq` | 服务端收件箱当前最大 seq |
| `hasMore` | 为 `true` 时带新游标继续翻页 |
| `cursorExpired` | 为 `true` 表示本地数据早于服务端保留窗口, **应重置本地消息数据后重新全量同步** |

客户端处理流程:

```text
for entry of entries ( 按 seq 升序 ):
    if entry.seq <= localCursor: continue          // 幂等去重
    if entry 有 message:      渲染消息; localCursor = entry.seq
    if entry 有 conversationUpdate:
        记录该会话的最新 conversationLatestSeq
        localCursor = entry.seq                     // 指针也推进收件箱游标
        发起 CONVERSATION_SYNC ( 见下一节 )
if hasMore: 用返回的最大 seq 作为 cursor 继续翻页
```

**SYNC 请求可安全重试** ( 不参与网关去重, 游标语义天然幂等 ), 网络抖动时放心重发。

## 8. 会话 timeline 同步 ( CONVERSATION_SYNC, 大群场景 )

收到 `conversationUpdate` 指针后, 拉取该会话的正文:

```typescript
const convSync = {
  commandType: "COMMAND_TYPE_CONVERSATION_SYNC",
  messageId: ulid(),
  body: btoa(JSON.stringify({
    conversationId: "conv_9001",
    cursor: localConversationCursor,  // 本端已应用的该会话最大 seq, 首次传 0
    limit: 100,
  })),
};
```

CONVERSATION_SYNC_ACK 的 `response`:

```json
{
  "isSuccess": true,
  "response": {
    "messages": [
      {
        "conversationSeq": 88,
        "message": { "conversationId": "conv_9001", "messageId": "...", "senderId": 200, "body": "...", "sentAt": "..." }
      }
    ],
    "latestSeq": 88,
    "hasMore": false
  }
}
```

- `conversationSeq` 在**单个会话内**严格单调; 跨会话没有全序, 各会话独立渲染。
- 后端会校验你是否为该会话成员, 非成员返回 `isSuccess: false`。
- 历史消息 ( 向更早翻页 ) 同样复用本指令。

## 9. ACK 统一错误处理

所有 `*_ACK` 帧通过 `messageId` 与请求帧关联。通用处理:

| 情况 | 处理建议 |
|---|---|
| 迟迟未收到 ACK ( 建议 5s 超时 ) | UPSTREAM: 可原 `messageId` 重试 ( 10s 去重窗口内网关会幂等消化 ); SYNC: 直接重发 |
| `isSuccess: false` + `errorMessage` 含 `failed to sync` / `timeout` | 后端暂不可用, 退避后重试 |
| 连接断开 | 走 [重连策略](#10-断线重连), 重连成功后**立即主动 SYNC 一次** ( 不等通知 ) |

## 10. 断线重连

```typescript
class GatewayClient {
  private reconnectAttempts = 0;

  async connect() {
    this.ws = new WebSocket(this.url);
    this.ws.onopen = () => {
      this.reconnectAttempts = 0;
      this.auth(this.jwt);                 // 每次连接都必须重新 AUTH
    };
    this.ws.onclose = (ev) => this.scheduleReconnect();
    this.ws.onmessage = (ev) => this.handleFrame(JSON.parse(ev.data));
  }

  private scheduleReconnect() {
    // 指数退避 + 抖动: 1s, 2s, 4s, ... 上限 30s
    const delay = Math.min(30000, 1000 * 2 ** this.reconnectAttempts++);
    const jitter = Math.random() * 500;
    setTimeout(() => {
      // token 过期则先向后端换新 token 再连
      this.connect().then(() => this.sync(0));   // 重连后立即全量比对游标
    }, delay + jitter);
  }
}
```

要点:

- 每次重连都是新连接, **必须重新走 AUTH**。
- 重连成功后**立即主动 SYNC** ( 以本地游标为 cursor ) —— 断线期间丢失的
  通知靠这一步兜底, 这是正确性的最后防线。
- 切前台 / 网络切换事件同样触发主动 SYNC。
- 建议心跳间隔 30s; 若连续多次心跳无回显, 主动断开重连。

## 11. 推荐的客户端状态

| 状态 | 类型 | 说明 |
|---|---|---|
| `cursor` | number | 收件箱全局游标 ( SYNC 推进 ) |
| `conversationCursors` | `Record<conversationId, number>` | 各会话独立游标 ( CONVERSATION_SYNC 推进 ) |
| `dedupCache` | `Set<messageId>` ( LRU ) | 渲染层按 `message_id` 幂等 |
| `pendingAcks` | `Map<messageId, {frame, sentAt}>` | 超时重发与诊断 |

## 12. 完整最小示例 ( TypeScript )

```typescript
import { ulid } from "ulid";

interface Frame {
  commandType: string;
  messageId: string;
  body: string;
}

const encode = (payload: unknown) => btoa(JSON.stringify(payload));
const decode = <T>(b: string): T => JSON.parse(atob(b));

interface SyncEntry {
  seq: number;
  message?: { messageId: string; conversationId: string; body: string };
  conversationUpdate?: { conversationId: string; conversationLatestSeq: number };
}

interface SyncAck {
  isSuccess: boolean;
  errorMessage?: string;
  response?: { entries: SyncEntry[]; latestSeq: number; hasMore: boolean; cursorExpired: boolean };
}

class SynpClient {
  private ws!: WebSocket;
  private cursor = 0;
  private conversationCursors: Record<string, number> = {};
  // 请求-应答关联表: messageId → 应答回调
  private pending = new Map<string, (frame: Frame) => void>();

  constructor(
    private url: string,
    private getJwt: () => Promise<string>,
  ) {}

  async connect() {
    this.ws = new WebSocket(this.url);
    this.ws.onmessage = (ev) => this.onFrame(JSON.parse(ev.data) as Frame);
    await new Promise<void>((resolve, reject) => {
      this.ws.onopen = () => resolve();
      this.ws.onerror = () => reject(new Error("connect failed"));
    });

    // ① 首帧 AUTH
    const jwt = await this.getJwt();
    const ack = await this.request("COMMAND_TYPE_AUTH", { token: jwt });
    if (!decode<{ success: boolean }>(ack.body).success) {
      throw new Error("auth failed");
    }

    // ② 心跳 ( 30s 一次; 生产实现建议加"连续 N 次无回显即重连"的看门狗 )
    setInterval(() => this.send("COMMAND_TYPE_HEARTBEAT", {}), 30_000);

    // ③ 首次同步 ( 以本地持久化的游标为起点 )
    await this.sync();
  }

  /** 发送请求帧并等待同 messageId 的 ACK。 */
  private request(commandType: string, payload: unknown): Promise<Frame> {
    return new Promise((resolve) => {
      const messageId = this.send(commandType, payload);
      this.pending.set(messageId, resolve);
      // 生产实现: 此处加超时定时器, 超时 reject 或按策略重发
    });
  }

  private send(commandType: string, payload: unknown): string {
    const messageId = ulid();
    this.ws.send(JSON.stringify({ commandType, messageId, body: encode(payload) } as Frame));
    return messageId;
  }

  private onFrame(frame: Frame) {
    if (frame.commandType.endsWith("_ACK")) {
      this.pending.get(frame.messageId)?.(frame);
      this.pending.delete(frame.messageId);
      return;
    }

    switch (frame.commandType) {
      case "COMMAND_TYPE_NOTIFY": {
        const { latestSeq } = decode<{ latestSeq: number }>(frame.body);
        if (latestSeq > this.cursor) void this.sync();
        break;
      }
      case "COMMAND_TYPE_HEARTBEAT":
        break; // 回显, 忽略
    }
  }

  private async sync() {
    const ack = await this.request("COMMAND_TYPE_SYNC", { cursor: this.cursor, limit: 100 });
    const payload = decode<SyncAck>(ack.body);
    if (!payload.isSuccess || !payload.response) return;

    if (payload.response.cursorExpired) {
      // 本地数据早于服务端保留窗口: 重置后重建 ( 见 FAQ )
      this.cursor = 0;
    }

    for (const entry of payload.response.entries) {
      if (entry.seq <= this.cursor) continue; // 幂等去重
      if (entry.message) {
        this.renderMessage(entry.message);
      }
      if (entry.conversationUpdate) {
        const cu = entry.conversationUpdate;
        if (cu.conversationLatestSeq > (this.conversationCursors[cu.conversationId] ?? 0)) {
          void this.syncConversation(cu.conversationId);
        }
      }
      this.cursor = entry.seq; // 推进收件箱游标
    }

    if (payload.response.hasMore) await this.sync(); // 继续翻页
  }

  private async syncConversation(conversationId: string) {
    const cursor = this.conversationCursors[conversationId] ?? 0;
    const ack = await this.request("COMMAND_TYPE_CONVERSATION_SYNC", {
      conversationId,
      cursor,
      limit: 100,
    });
    const payload = decode<{
      isSuccess: boolean;
      response?: { messages: Array<{ conversationSeq: number; message: { messageId: string; body: string } }> };
    }>(ack.body);
    if (!payload.isSuccess || !payload.response) return;

    for (const { conversationSeq, message } of payload.response.messages) {
      if (conversationSeq <= cursor) continue; // 会话内幂等去重
      this.renderMessage(message);
      this.conversationCursors[conversationId] = conversationSeq;
    }
  }

  /** 业务渲染入口: 按 messageId 做渲染层 LRU 去重。 */
  private renderMessage(m: { messageId: string; body: string }) {
    // const biz = JSON.parse(atob(m.body)); ...
  }
}
```

> 上例为可运行的骨架, 省略了重连与超时逻辑 ( 见第 10 节 )。
> 生产实现建议补齐: 指数退避重连、心跳看门狗、请求超时重发、渲染层 LRU 去重。

## 13. 常见问题 ( FAQ )

**Q: 为什么我发的消息自己也会收到?**
发送者本人是多端收件箱的参与方 ( 写扩散会写发送者收件箱, 用于多端自同步 ),
收到自己的消息是预期行为, 按 `messageId` 去重即可。

**Q: NOTIFY 一直不来, 消息会丢吗?**
不会。通知只影响实时性; 心跳、重连、切前台时的主动 SYNC 是兜底路径,
只要游标落后就会被拉平。

**Q: `cursorExpired: true` 怎么办?**
本地数据早于服务端保留窗口 ( 如离线过久 ), 服务端从保留窗口最早条目开始返回,
中间存在缺口。应重置本地消息数据, 以当前返回重建 ( 或按业务策略截断展示 )。

**Q: 大群消息为什么是"两跳" ( 先 SYNC 再 CONVERSATION_SYNC )?**
大群走读扩散: 收件箱里只有轻量指针 ( 会话 + 最新 seq ), 正文在会话 timeline。
这是写入可控性 ( 万人群只存一份正文 ) 与读取体验的权衡, 小群/单聊仍是
一次拉取即可渲染。

**Q: 同一账号多设备/多标签页?**
每个设备类型 ( `device` 参数 ) 独立连接、独立游标。同 `device` 的新连接
会顶掉旧连接 ( 例如同浏览器第二个标签页 )。
