# SSE 原始流转发器（带实时 Dashboard）

一个**不解析 SSE**的调试用反向代理：客户端把请求打到代理端口，转发器把上游返回的字节
**按原始边界、原始顺序、原始编码**写给客户端；同时在 Dashboard 上实时看到每一块数据、
请求头、响应头，以及尽可能详细的错误信息。上游可以在 Dashboard 上随时增删、随时切换。

```
客户端 ──HTTP/SSE──▶  :8788 代理端口 ──原样字节流──▶  上游（可随时切换）
                          │
                          └──抓取副本──▶  :8787 Dashboard（实时事件流 + 会话详情 + 错误详情）
```

## 为什么是"转发流"而不是"转发 SSE"

需求里提到 SSE 可能是非标的。解析式转发（读 `data:` / `event:` / 空行分帧再重新拼装）
一旦遇到非标流就会**改写内容**：丢字段、改行尾、合并分片、丢掉心跳注释行。所以这里
完全不做解析：

- 用 `httpx` 的 `aiter_raw()` 拿**未解码**的原始字节（不解 gzip、不重新分帧）
- 用 Starlette `StreamingResponse` 逐块写回，不缓冲、不改 `Content-Type`
- 上游一个 TCP 段吐 1 个字节，客户端就收到 1 个字节（自测第 4 项专门验证了这点）
- SSE 规范里的分帧、`data:` 前缀、空行分隔**一律不校验、不修正**，只在 Dashboard 上
  以"观察结论"的形式提示你哪里非标（例如 `事件之间没有空行分隔`、`裸 JSON 行`、
  `CRLF/LF 混用`、`含非 UTF-8 字节`）

顺带的好处：它同时是个通用反向代理，普通 JSON 响应、二进制下载一样能透传
（非 `text/event-stream` 会在 Dashboard 上给一条非致命警告）。

## 大模型接口改写：本地 key / 模型名 ⇄ 上游 key / 模型名

面向大模型接口的核心能力。**一条规则 = 一对值**：`src` 是本地值（客户端在用的），
`dst` 是上游值（上游要求的）。

- **请求方向**：入口请求的**全部请求头 + body** 做字面替换，`src → dst`
  （`Authorization: Bearer sk-本地…` → `Bearer sk-上游…`，body 里 `"model": "本地名"` → `"上游名"`，
  连嵌在 `messages[].content` 里的 key 也一并换掉）
- **响应方向**：自动反向，`dst → src`，把上游回吐的模型名/key 换回本地值，客户端全程看不到上游凭据
- **规则跟着上游走**：每个上游配自己的一套，Dashboard 上点一下切换上游，规则同时切换，互不影响
- **纯字面替换，不解析 JSON**：接口结构再复杂、字段嵌套再深都不会漏；最长模式优先，避免短串吃掉长串
- **流式安全**：上游把一个模型名拆成两个 TCP 分片（甚至逐字节滴）也能正确替换——
  用滑动窗口保留 `最长模式-1` 字节的尾巴，既不漏替换也不攒数据造成延迟

Dashboard 上 **key 与 body 全部明文显示，不做任何遮蔽**（按你的选择），并额外提供：

- `改写` 页签：本会话用的规则快照、请求方向命中表（规则名 / 位置 headers 或 body / 替换前后 / 次数）、
  响应方向命中表、以及**请求体改写前后左右对照**
- `数据流` 页签：发生过改写的分片会标红「已改写」，右上角可一键切换看**上游原文**还是**客户端收到的字节**
- 顶栏实时显示当前上游的规则条数；每次改写命中都会弹提示（含 body 字节数变化）

按你的选择，**URL 的 path / query 原样透传，不做任何替换**。

三个环境变量即可种下初始规则（之后在 Dashboard 里改，会持久化到 `/data/state.json`）：

```bash
LOCAL_API_KEY=sk-local-xxxx  UPSTREAM_API_KEY=sk-upstream-yyyy \
LOCAL_MODEL=local-qwen3-max-thinking  UPSTREAM_MODEL=qwen-max-upstream-v3 \
  python3 -m app.main
```

压缩响应说明：字面替换对压缩流无效，所以当上游配了响应改写规则时，转发器会把
`Accept-Encoding` 强制为 `identity`；若上游仍然返回 gzip/deflate，则**在线解压后改写、以明文转发**
（移除 `Content-Encoding` 头，内容等价），`br`/`zstd` 不支持在线解压时会放弃响应改写、原样透传并在
Dashboard 记录原因——请求方向的改写始终不受影响。

## 快速开始

镜像默认端口：**代理 6778 / Dashboard 6779**（本机直跑 `python -m app.main` 仍是 8788/8787，
两边都可用 `PROXY_PORT` / `DASHBOARD_PORT` 环境变量覆盖）。

### 从 GHCR 拉现成镜像（x86-64 / ARM64）

```bash
docker run -d --name ssefw -p 6778:6778 -p 6779:6779 \
  -e UPSTREAM_URL=http://your-upstream:8000 \
  -v ssefw-data:/data ghcr.io/yizhixiaoliulang/sse-forwarder:latest
```

每次推送到 main 分支，GitHub Actions 会自动构建 `linux/amd64` + `linux/arm64` 双架构镜像并发布到 GHCR。

### 本地 compose（推荐，带演示用的非标 SSE 假上游）

```bash
docker compose up -d --build      # 起转发器 + 一个演示用的非标 SSE 假上游
```

- Dashboard： http://localhost:6779
- 代理端口： http://localhost:6778  ← 客户端把原本打向上游的请求改打到这里
- 演示假上游： http://localhost:9000（`/std` `/nonstd` `/drip` `/die` `/hang` `/gzip` `/echo` …）

```bash
# 试一下标准 SSE
curl -N http://localhost:6778/std
# 试一下非标 SSE（无空行分隔 / CRLF+LF 混用 / 裸 JSON 行 / 非法 UTF-8）
curl -N http://localhost:6778/nonstd | xxd | head
# 试一下上游中途掐断（Dashboard 会记录 RemoteProtocolError + 堆栈 + 已收字节数）
curl -N http://localhost:6778/die
```

只构建镜像：

```bash
make build                 # 或 docker build -t sse-forwarder:latest .
docker run -d --name ssefw -p 6778:6778 -p 6779:6779 \
  -e UPSTREAM_URL=http://your-upstream:8000 \
  -v ssefw-data:/data sse-forwarder:latest
```

镜像特性：多阶段构建、非 root（uid 10001）运行、内置 `HEALTHCHECK`（打 Dashboard 的
`/api/health`）、`/data` 卷持久化上游配置与当前激活项。

### 不用 Docker，本机直接跑

```bash
pip install -r requirements.txt
python3 -m tools.fake_upstream 9000 &          # 可选：假上游
UPSTREAM_URL=http://127.0.0.1:9000 python3 -m app.main
```

## Dashboard 使用说明

三栏布局，全部实时（内部也是一条 SSE 事件流，断线自动重连并按 `seq` 回放缺失事件）：

| 区域 | 能做什么 |
| --- | --- |
| 左：上游 | 添加 / 编辑 / 删除上游，`规则` 按钮可视化增删改 key/模型名映射（含方向选择）；**点一下即切换**，对后续请求立即生效；`切换并断流` 会同时终止走旧上游的进行中连接（让客户端重连到新上游）；`探测` 会逐层报告 DNS → TCP → TLS → HTTP 结果和耗时 |
| 中：会话 | 每个客户端请求一条记录：方法、路径、状态码、上游名、已收字节、分片数、耗时、状态灯（connecting/streaming/done/error）、错误数；可只看进行中、可清空历史 |
| 右：详情 | 6 个页签：**数据流**（逐分片，带 seq / 时间戳 / Δms / 字节数，支持文本或 HEX 视图、合并连续小分片、自动滚动、暂停渲染）、**请求头**、**响应头**、**改写**（规则快照 + 命中表 + 请求体前后对照）、**错误**、**概览**（含非标 SSE 探测结论、TTFB、丢弃分片数等） |

上游附加请求头（如 `Authorization`）以 JSON 对象填写，会在转发时注入并覆盖同名头。
设置 `DASHBOARD_TOKEN` 后，页面访问需要带 `?token=xxx`。

## 错误上报有多详细

所有异常都会带上 **phase（阶段）+ 异常类型 + 消息 + 结构化 detail + 完整 Python 堆栈**，
既推到 Dashboard 实时事件流，也存进会话详情；连接阶段的错误还会直接返回给客户端（502）。

| phase | 触发场景 | detail 里有什么 |
| --- | --- | --- |
| `config` | 没配上游 / 方法不允许 / 收到 WebSocket 升级请求 | 提示、path、client |
| `send_request` | 读客户端请求体失败 | 异常、目标 URL |
| `connect_error` | TCP 连不上（拒绝、路由不通） | 人类可读原因、target_url、上游名/ID、`dns_result`（实际解析到的 IP 列表）、`resolved_host`、connect/read 超时配置、`exception_args`、`cause`、堆栈 |
| `connect_timeout` | 建连超时 | 同上 + 超时阈值 |
| `dns` | 域名解析失败 | 同上 + 解析失败原因 |
| `protocol_tls` | TLS 握手 / HTTP 协议错误 | 同上 + SSL 细节 |
| `read_response_headers` | 上游状态码 ≥400，或 `Content-Type` 不是 `text/event-stream` | status、content_type、说明"仍原样透传" |
| `stream` | 流中途断（`RemoteProtocolError`）、读超时、连接重置、空闲超时、被 Dashboard 终止 | 已收字节数 `bytes_received`、耗时、空闲阈值、目标 URL、堆栈 |
| `client` | 客户端提前断开（取消订阅 / 关页面） | 已发送字节数、说明 |

客户端侧收到的 502 响应体示例（便于直接在 curl 里定位问题）：

```json
{
  "error": "upstream_unreachable",
  "phase": "connect_error",
  "message": "无法建立到上游的 TCP/TLS 连接",
  "exception": "ConnectError: All connection attempts failed",
  "target_url": "http://127.0.0.1:59999/x",
  "detail": {
    "human_readable": "无法建立到上游的 TCP/TLS 连接",
    "upstream_name": "坏的", "upstream_base_url": "http://127.0.0.1:59999",
    "connect_timeout": 10.0, "read_timeout": 0.0, "verify_tls": true,
    "resolved_host": "127.0.0.1", "dns_result": ["127.0.0.1"],
    "exception_module": "httpx", "exception_args": ["'All connection attempts failed'"],
    "cause": "ConnectError: All connection attempts failed"
  }
}
```

## HTTP API（Dashboard 背后用的就是这些）

```
GET    /api/state                        当前配置、上游列表、激活项、统计
GET    /api/sessions?limit=100&open=1    会话列表（摘要）
GET    /api/sessions/{id}?tail=200       会话详情（请求/响应头、分片、错误）
POST   /api/sessions/{id}/kill           终止某个进行中的会话
POST   /api/sessions/kill-all            终止所有进行中的会话
POST   /api/sessions/clear               清空历史（保留进行中）
POST   /api/upstreams                    新增上游 {name,url,headers,note,activate}
PUT    /api/upstreams/{id}               修改上游
DELETE /api/upstreams/{id}               删除上游
POST   /api/upstreams/{id}/activate      切换上游 {kill_open:bool}
POST   /api/upstreams/{id}/test          逐层连通性探测 {path}
GET    /api/events?after_seq=0           实时事件流（SSE，含 snapshot + 回放）
GET    /api/health                       健康检查
GET    /__proxy_health__                 代理端口侧的健康检查
```

事件类型：`snapshot` `session_created` `request_sending` `response_headers` `chunk`
`error` `session_closed` `upstream_created/updated/removed/switched` `probe_result`
`history_cleared`。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PROXY_HOST` / `PROXY_PORT` | `0.0.0.0` / `8788` | 代理监听地址 |
| `DASHBOARD_HOST` / `DASHBOARD_PORT` | `0.0.0.0` / `8787` | Dashboard 监听地址 |
| `UPSTREAM_URL` / `UPSTREAM_NAME` | `http://127.0.0.1:9000` / `default` | 初始上游（首次启动写入状态文件，之后以 Dashboard 为准） |
| `FORCE_IDENTITY_ENCODING` | `true` | 强制 `Accept-Encoding: identity`，抓到的是明文可读；设为 `false` 则 100% 字节透传（可能是 gzip 流） |
| `CONNECT_TIMEOUT` | `10` | 建连超时（秒） |
| `READ_TIMEOUT` | `0` | 读超时，**0 = 不限**（SSE 长连接必须） |
| `IDLE_TIMEOUT` | `0` | 上游静默多久判定卡死并断开，0 = 关闭。排查"连接还在但不再吐数据"时很好用 |
| `WRITE_TIMEOUT` / `POOL_TIMEOUT` | `30` / `10` | |
| `VERIFY_TLS` | `true` | 全局；也可按上游单独设 `verify_tls` |
| `HTTP2` | `false` | 开启 h2（需 `httpx[http2]`） |
| `FOLLOW_REDIRECTS` | `false` | 默认不跟随，保持原样 |
| `PRESERVE_CLIENT_HOST` | `false` | true 时保留客户端原始 `Host`，否则改写为上游 Host |
| `ALLOWED_METHODS` | `GET,POST,PUT,PATCH,DELETE,HEAD,OPTIONS` | |
| `MAX_SESSIONS` | `300` | 会话历史上限（进行中的永不被淘汰） |
| `MAX_CHUNKS_PER_SESSION` | `5000` | 每会话保留分片数上限 |
| `MAX_CAPTURE_BYTES` | `8388608` | 每会话累计抓取字节上限，超出只计数不存内容 |
| `MAX_CHUNK_CAPTURE` | `262144` | 单个分片抓取上限 |
| `EVENT_BUFFER` | `400` | Dashboard 重连可回放的事件数 |
| `STATE_FILE` | `/data/state.json` | 上游配置持久化路径，空字符串 = 不持久化 |
| `DASHBOARD_TOKEN` | 空 | 非空时 Dashboard 需要 `?token=` 或 `X-Dashboard-Token` |
| `REWRITE_ENABLED` | `true` | 改写总开关，false = 纯透传 |
| `LOCAL_API_KEY` / `UPSTREAM_API_KEY` | 空 | 初始上游的 key 映射（`sk-本地` → `sk-上游`） |
| `LOCAL_MODEL` / `UPSTREAM_MODEL` | 空 | 初始上游的模型名映射 |
| `REWRITE_FORCE_IDENTITY` | `true` | 有响应改写规则时强制上游返回明文，否则压缩流无法替换 |
| `REWRITE_MAX_BODY` | `8388608` | 请求体超过此大小则跳过 body 改写（头照常改写）并告警 |
| `LOG_LEVEL` | `info` | |

## 并发模型

单进程单事件循环 + 两个 uvicorn Server（代理、Dashboard 各一个端口）。
`httpx.AsyncClient` 按 `(verify_tls, http2)` 复用连接池（`max_connections=64`，
keepalive 16），1~10 并发长连接毫无压力，几十上百也没问题。
状态字典的写入都在同一个事件循环里完成，没有跨线程竞争；抓取副本失败会被 catch 住，
**绝不影响转发**（自测里专门验证过）。

内存保护：每会话最多抓 8MB / 5000 分片，超出后只累加计数（`drop_count`）不再存内容；
会话历史上限 300 条，进行中的会话不会被淘汰。

## 自测

不依赖 pytest，也不需要 Docker：

```bash
python3 tests/run_tests.py       # 端到端：86 项断言全绿
python3 tests/test_rewrite.py    # 改写引擎单元测试：33 项断言全绿
```

覆盖：标准 SSE 字节级一致、非标 SSE 字节级一致 + 非标特征识别、非法 UTF-8、逐字节滴流的
帧边界保真、非 SSE JSON 透传、请求头透传 / Host 改写 / 上游附加头注入、上游中途掐断、
上游不可达（502 + DNS/TCP 详情）、DNS 解析失败、Dashboard 切换上游 + 逐层探测、
并发 6 路、手动终止会话、gzip 上游在 `FORCE_IDENTITY_ENCODING=false` 时的字节级透传、
Dashboard API 与事件流。

`tests/test_rewrite.py`（改写引擎单元测试）覆盖：规则构造与序列化、无效规则过滤、
请求/响应双向替换、多处命中全替换、`scope` 方向控制、请求头替换、
**跨分片被拦腰截断的模式**、逐字节喂入、空规则集透传、滑动窗口不会无限攒数据（延迟上界）、
纯二进制与非法 UTF-8 安全、长短模式冲突时最长优先。

假上游 `tools/fake_upstream.py` 是裸 socket 写的，能精确控制返回字节，可单独用来复现问题：
`python3 -m tools.fake_upstream 9000`，路由见文件头注释。

## 已知边界

- 不代理 WebSocket（收到 `Upgrade: websocket` 会返回 501 并在 Dashboard 记录）。
- 上游配置持久化，但**会话历史只在内存里**，重启即清空（调试工具的定位）。
- 改写是**字面替换**，不理解 JSON 语义：如果本地值恰好是某个更长字符串的子串，会被一起替换
  （所以 key 和模型名请用足够长、足够独特的值）。
- 响应方向改写需要滑动窗口缓冲，最坏情况会把末尾 `最长模式-1` 字节压到下一个分片才发出，
  毫秒级、不影响完整性；不需要反向替换时把规则的 `scope` 设为 `request` 即可保持 100% 原样透传。
- 请求体是整体读取后转发的（超过 `MAX_REQUEST_BODY_CAPTURE` 只截断抓取，不影响转发），
  因此不适合"客户端超大流式上传"场景。
- `FORCE_IDENTITY_ENCODING=true`（默认）会改写 `Accept-Encoding`，此时上游返回的是未压缩流。
  需要严格字节级透传请设为 `false`，Dashboard 里改用 HEX 视图看压缩数据。
