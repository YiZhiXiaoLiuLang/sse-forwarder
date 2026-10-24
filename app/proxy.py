"""原始字节流转发器。

核心原则：**不解析 SSE**。上游吐什么字节，就按什么边界、什么顺序原样写给客户端。
- 用 httpx 的 `aiter_raw()` 拿未经解码的原始字节（不做 gzip 解压、不重新分帧）
- 用 Starlette StreamingResponse 逐块写回，不缓冲、不改 Content-Type
- 抓取一份副本给 Dashboard 看，抓取失败/超限绝不影响转发

同时它也是个通用反向代理：非 SSE 的普通 HTTP 响应一样能透传。
"""
from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import time
import zlib
from typing import Any, AsyncIterator, Optional
from urllib.parse import urlsplit, urlunsplit

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .config import Settings
from .rewrite import RuleSet, StreamRewriter
from .store import HOP_BY_HOP, Store, Upstream, ts_iso

log = logging.getLogger("sse-forwarder.proxy")

ANY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


class ClientPool:
    """按 (verify_tls, http2) 复用 AsyncClient，避免每请求握手开销。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._clients: dict[tuple[bool, bool], httpx.AsyncClient] = {}

    def get(self, verify_tls: bool, http2: bool) -> httpx.AsyncClient:
        key = (verify_tls, http2)
        c = self._clients.get(key)
        if c is None or c.is_closed:
            timeout = httpx.Timeout(
                connect=self.settings.connect_timeout,
                read=self.settings.read_timeout_or_none(),
                write=self.settings.write_timeout,
                pool=self.settings.pool_timeout,
            )
            c = httpx.AsyncClient(
                timeout=timeout,
                verify=verify_tls,
                http2=http2,
                follow_redirects=self.settings.follow_redirects,
                limits=httpx.Limits(max_connections=64, max_keepalive_connections=16),
            )
            self._clients[key] = c
        return c

    async def aclose(self) -> None:
        for c in list(self._clients.values()):
            try:
                await c.aclose()
            except Exception:
                pass
        self._clients.clear()


class _BodyDecoder:
    """在线解压上游响应，仅当需要对压缩流做反向改写时启用。

    支持 gzip / deflate；br、zstd 等不支持（返回 None，此时放弃改写、原样透传）。
    """

    def __init__(self, obj: "zlib._Decompress", encoding: str) -> None:
        self._obj = obj
        self.encoding = encoding
        self.raw_in = 0
        self.raw_out = 0

    @staticmethod
    def make(encoding: str) -> Optional["_BodyDecoder"]:
        enc = encoding.strip().lower()
        if enc in ("gzip", "x-gzip"):
            return _BodyDecoder(zlib.decompressobj(zlib.MAX_WBITS | 16), enc)
        if enc == "deflate":
            return _BodyDecoder(zlib.decompressobj(), enc)
        return None

    def decode(self, data: bytes) -> bytes:
        self.raw_in += len(data)
        out = self._obj.decompress(data)
        self.raw_out += len(out)
        return out

    def flush(self) -> bytes:
        try:
            out = self._obj.flush()
        except Exception:
            out = b""
        self.raw_out += len(out)
        return out


def build_target_url(upstream_url: str, path: str, query: str) -> str:
    """把客户端请求的 path/query 拼到上游 base url 后面（支持上游自带路径前缀）。"""
    b = urlsplit(upstream_url)
    base_path = b.path.rstrip("/")
    req_path = path if path.startswith("/") else "/" + path
    if not req_path.endswith("/") and base_path and path == "":
        req_path = "/"
    full_path = base_path + req_path if req_path != "/" or not base_path else (base_path or "/")
    if not full_path:
        full_path = "/"
    return urlunsplit((b.scheme, b.netloc, full_path, query, ""))


def filter_request_headers(raw: list[tuple[bytes, bytes]], upstream: Upstream,
                         settings: Settings, target_host: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    client_host = ""
    for k, v in raw:
        name = k.decode("latin-1")
        low = name.lower()
        if low == "host":
            client_host = v.decode("latin-1")
            continue
        if low in HOP_BY_HOP or low == "content-length":
            continue
        if low == "accept-encoding" and settings.force_identity_encoding:
            continue
        out.append((name, v.decode("latin-1")))
        seen.add(low)
    # 默认把 Host 改写成上游 Host；PRESERVE_CLIENT_HOST=true 时保留客户端原始 Host
    out.append(("Host", client_host if (settings.preserve_client_host and client_host) else target_host))
    if settings.force_identity_encoding and "accept-encoding" not in seen:
        out.append(("Accept-Encoding", "identity"))
    for k, v in (upstream.headers or {}).items():
        out = [(n, val) for n, val in out if n.lower() != k.lower()]
        out.append((k, v))
    return out


def filter_response_headers(resp: httpx.Response, method: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for k, v in resp.headers.raw:
        name = k.decode("latin-1")
        low = name.lower()
        if low in HOP_BY_HOP:
            continue
        if low == "content-length" and method.upper() != "HEAD":
            continue     # 我们用 chunked 重新分帧，保留会与实际字节数冲突
        out.append((name, v.decode("latin-1")))
    return out


def make_proxy_app(store: Store, settings: Settings, pool: ClientPool) -> Starlette:
    async def handle(request: Request) -> Response:
        sid_holder: dict[str, Any] = {}
        upstream = store.get_active()
        client_addr = request.client.host if request.client else "unknown"
        if request.headers.get("x-forwarded-for"):
            client_addr = request.headers["x-forwarded-for"].split(",")[0].strip()

        if upstream is None:
            store.add_error(None, "config", message="未配置任何上游，请先在 Dashboard 添加并激活一个上游",
                            detail={"hint": "POST /api/upstreams 或在页面左侧添加"})
            return JSONResponse(
                {"error": "no_active_upstream",
                 "message": "转发器还没有可用的上游，请打开 Dashboard 配置并激活一个上游",
                 "dashboard_port": settings.dashboard_port},
                status_code=503,
            )

        if request.method.upper() not in settings.allowed_method_set:
            return JSONResponse({"error": "method_not_allowed", "method": request.method}, status_code=405)

        upgrade = request.headers.get("upgrade", "").lower()
        if upgrade == "websocket":
            store.add_error(None, "config", message="收到 WebSocket 升级请求，本转发器只处理 HTTP/SSE 流",
                            detail={"path": request.url.path, "client": client_addr})
            return JSONResponse({"error": "websocket_not_supported"}, status_code=501)

        target = build_target_url(upstream.url, request.url.path, request.url.query)
        sess = store.new_session(
            method=request.method, path=request.url.path, query=request.url.query,
            client=client_addr, upstream_id=upstream.id, upstream_name=upstream.name,
            target_url=target,
        )
        sid_holder["id"] = sess.id

        # ---- 改写规则（跟着上游走：每个上游配自己的 key / 模型名映射）----
        ruleset = RuleSet.from_list(upstream.rules) if settings.rewrite_enabled else RuleSet()
        sess.rewrite_rules = ruleset.to_list()

        # ---- 请求头 ----
        raw_headers = [(k, v) for k, v in request.headers.raw]
        sess.request_headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in raw_headers]
        forwarded = filter_request_headers(raw_headers, upstream, settings, urlsplit(target).netloc)
        if ruleset.request_pairs:
            forwarded, h_hits = ruleset.apply_request_headers(forwarded)
            if h_hits:
                sess.request_rewrites.extend(h_hits)

        # ---- 请求体（流式场景通常为空；有体则整体读取后转发，同时抓取副本）----
        body = b""
        try:
            if request.method.upper() not in {"GET", "HEAD", "OPTIONS", "DELETE"}:
                body = await request.body()
                cap = settings.max_request_body_capture
                sess.bytes_client_body = len(body)
                if ruleset.request_pairs and body:
                    if len(body) > settings.rewrite_max_body:
                        store.add_error(
                            sess, "send_request",
                            message=f"请求体 {len(body)} 字节超过改写上限 {settings.rewrite_max_body}，"
                                    f"本次跳过 body 改写（请求头照常改写）",
                            detail={"body_size": len(body), "limit": settings.rewrite_max_body},
                            fatal=False)
                    else:
                        new_body, b_hits = ruleset.apply_request(body)
                        if b_hits:
                            for h in b_hits:
                                h = dict(h); h["in"] = "body"
                                sess.request_rewrites.append(h)
                            sess.request_body_raw = body[:cap]
                            body = new_body
                sess.request_body = body[:cap]
                sess.request_body_truncated = len(body) > cap
        except Exception as exc:
            store.add_error(sess, "send_request", exc,
                            detail={"stage": "读取客户端请求体失败", "target_url": target})
            store.close_session(sess, "request_body_error", "error")
            return JSONResponse({"error": "request_body_error", "detail": str(exc)}, status_code=400)

        # 需要做响应反向改写时，压缩流没法字面替换 —— 强制上游返回明文
        resp_rewrite = bool(ruleset.response_pairs)
        if resp_rewrite and settings.rewrite_force_identity:
            forwarded = [(k, v) for k, v in forwarded if k.lower() != "accept-encoding"]
            forwarded.append(("Accept-Encoding", "identity"))

        if sess.request_rewrites:
            store.emit("request_rewritten", {
                "session_id": sess.id, "session_short_id": sess.id[:8],
                "rewrites": sess.request_rewrites,
                "body_size_before": len(sess.request_body_raw) or sess.bytes_client_body,
                "body_size_after": len(body),
            })

        store.emit("request_sending", {
            "session_id": sess.id, "session_short_id": sess.id[:8], "method": request.method,
            "target_url": target, "upstream_name": upstream.name,
            "headers": [{"k": k, "v": v} for k, v in forwarded],
            "body_size": len(body), "rewrites": sess.request_rewrites,
            "ts_text": ts_iso(sess.created_at),
        })

        verify = settings.verify_tls if upstream.verify_tls is None else upstream.verify_tls
        follow = settings.follow_redirects if upstream.follow_redirects is None else upstream.follow_redirects
        client = pool.get(verify, settings.http2)
        req = client.build_request(request.method, target, headers=forwarded,
                                   content=body if body else None)

        t0 = time.perf_counter()
        upstream_resp: Optional[httpx.Response] = None
        try:
            upstream_resp = await client.send(req, stream=True, follow_redirects=follow)
        except httpx.ConnectTimeout as exc:
            return _connect_fail(store, sess, settings, exc, "connect_timeout", target, upstream,
                                 f"连接上游超时（>{settings.connect_timeout}s）")
        except httpx.ConnectError as exc:
            return _connect_fail(store, sess, settings, exc, "connect_error", target, upstream,
                                 "无法建立到上游的 TCP/TLS 连接")
        except httpx.ReadTimeout as exc:
            return _connect_fail(store, sess, settings, exc, "read_timeout", target, upstream,
                                 "等待上游响应头超时")
        except (ssl.SSLError, httpx.ProtocolError) as exc:
            return _connect_fail(store, sess, settings, exc, "protocol_tls", target, upstream,
                                 "TLS/HTTP 协议错误")
        except socket.gaierror as exc:
            return _connect_fail(store, sess, settings, exc, "dns", target, upstream,
                                 f"DNS 解析失败: {urlsplit(target).hostname}")
        except OSError as exc:
            return _connect_fail(store, sess, settings, exc, "network", target, upstream,
                                 "网络层错误（OSError）")
        except Exception as exc:
            return _connect_fail(store, sess, settings, exc, "unexpected_connect", target, upstream,
                                 "建立上游连接时发生未预期异常")

        # ---- 响应头 ----
        sess.ttfb_ms = (time.perf_counter() - t0) * 1000
        sess.response_status = upstream_resp.status_code
        sess.response_reason = upstream_resp.reason_phrase or ""
        sess.http_version = upstream_resp.http_version or ""
        resp_headers = filter_response_headers(upstream_resp, request.method)

        # ---- 响应反向改写（上游 key/模型名 -> 本地值）----
        rewriter: Optional[StreamRewriter] = StreamRewriter(ruleset, "response") if resp_rewrite else None
        if rewriter is not None and not rewriter.enabled:
            rewriter = None
        decoder: Optional[_BodyDecoder] = None
        cenc = (upstream_resp.headers.get("content-encoding") or "").strip().lower()
        if rewriter is not None and cenc not in ("", "identity"):
            decoder = _BodyDecoder.make(cenc)
            if decoder is None:
                store.add_error(
                    sess, "read_response_headers",
                    message=f"上游响应是 {cenc} 压缩流，无法对其做字面反向替换，本次响应按原始字节透传",
                    detail={"content_encoding": cenc, "target_url": target,
                            "hint": "关闭 FORCE_IDENTITY_ENCODING=false 会让上游有机会返回压缩流；"
                                    "br/zstd 等编码不支持在线解压",
                            "note": "请求方向的改写不受影响，已正常执行"},
                    fatal=False)
                rewriter = None
            else:
                resp_headers = [(k, v) for k, v in resp_headers if k.lower() != "content-encoding"]
                store.add_error(
                    sess, "read_response_headers",
                    message=f"上游返回 {cenc} 压缩响应；为执行反向改写已在线解压，并以明文转发给客户端",
                    detail={"content_encoding": cenc, "target_url": target,
                            "note": "已移除 Content-Encoding 头，客户端收到的是解压后的等价内容"},
                    fatal=False)

        sess.response_headers = list(resp_headers)
        sess.state = "streaming"

        store.emit("response_headers", {
            "session_id": sess.id, "session_short_id": sess.id[:8],
            "status": sess.response_status, "reason": sess.response_reason,
            "http_version": sess.http_version,
            "headers": [{"k": k, "v": v} for k, v in resp_headers],
            "ttfb_ms": round(sess.ttfb_ms, 2), "target_url": target,
        })

        ctype = upstream_resp.headers.get("content-type", "")
        if "text/event-stream" not in ctype:
            store.add_error(sess, "read_response_headers",
                            message=f"上游 Content-Type 不是 text/event-stream，而是 {ctype or '(缺失)'}",
                            detail={"content_type": ctype, "target_url": target,
                                    "note": "仍然按原始字节透传，不做任何转换"},
                            fatal=False)
        if upstream_resp.status_code >= 400:
            store.add_error(sess, "read_response_headers",
                            message=f"上游返回错误状态码 {upstream_resp.status_code} {sess.response_reason}",
                            detail={"status": upstream_resp.status_code, "target_url": target,
                                    "note": "响应体仍原样透传给客户端"},
                            fatal=False)

        idle_to = settings.idle_timeout_or_none()
        cancel = sess.cancel_event
        # body_iter 内需要能置空它们，用容器规避闭包变量赋值导致的 UnboundLocalError
        rw: dict[str, Any] = {"dec": decoder, "rw": rewriter}

        async def body_iter() -> AsyncIterator[bytes]:
            it = upstream_resp.aiter_raw()
            closed_reason = "upstream_eof"
            read_task: Optional[asyncio.Task] = None
            cancel_task: Optional[asyncio.Task] = None
            try:
                while True:
                    if cancel and cancel.is_set():
                        closed_reason = "killed_from_dashboard"
                        store.add_error(sess, "stream", message="会话被 Dashboard 手动终止",
                                        detail={"target_url": target}, fatal=True)
                        break
                    # 让「读上游」与「Dashboard 取消」「空闲超时」三者赛跑，
                    # 否则上游静默时 kill 无法生效。
                    read_task = asyncio.ensure_future(_anext(it))
                    cancel_task = asyncio.ensure_future(cancel.wait()) if cancel is not None else None
                    watch = {read_task} if cancel_task is None else {read_task, cancel_task}
                    done, _ = await asyncio.wait(watch, timeout=idle_to, return_when=asyncio.FIRST_COMPLETED)
                    if cancel_task is not None and cancel_task in done:
                        read_task.cancel()
                        try:
                            await read_task
                        except (asyncio.CancelledError, StopAsyncIteration, Exception):
                            pass
                        closed_reason = "killed_from_dashboard"
                        store.add_error(sess, "stream", message="会话被 Dashboard 手动终止",
                                        detail={"target_url": target,
                                                "bytes_received": sess.bytes_upstream}, fatal=True)
                        break
                    if cancel_task is not None:
                        cancel_task.cancel()
                    if not done:      # 只有 read_task 未完成 => 空闲超时
                        read_task.cancel()
                        try:
                            await read_task
                        except (asyncio.CancelledError, StopAsyncIteration, Exception):
                            pass
                        closed_reason = "idle_timeout"
                        store.add_error(sess, "stream",
                                        message=f"上游空闲超过 {idle_to}s 没有任何数据，判定为卡死并断开",
                                        detail={"idle_timeout": idle_to, "target_url": target,
                                                "bytes_received": sess.bytes_upstream,
                                                "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1)})
                        break
                    try:
                        chunk = read_task.result()
                    except asyncio.TimeoutError:
                        closed_reason = "idle_timeout"
                        store.add_error(sess, "stream",
                                        message=f"上游空闲超过 {idle_to}s 没有任何数据，判定为卡死并断开",
                                        detail={"idle_timeout": idle_to, "target_url": target,
                                                "last_chunk_ago_ms": round((time.perf_counter() - t0) * 1000, 1)})
                        break
                    except StopAsyncIteration:
                        break
                    except httpx.ReadTimeout as exc:
                        closed_reason = "read_timeout"
                        store.add_error(sess, "stream", exc,
                                        detail={"stage": "读取上游数据超时", "target_url": target,
                                                "read_timeout": settings.read_timeout})
                        break
                    except (httpx.RemoteProtocolError, httpx.ProtocolError) as exc:
                        closed_reason = "upstream_protocol_error"
                        store.add_error(sess, "stream", exc,
                                        detail={"stage": "上游流被异常中断/协议错误",
                                                "target_url": target,
                                                "bytes_received": sess.bytes_upstream})
                        break
                    except httpx.ReadError as exc:
                        closed_reason = "upstream_read_error"
                        store.add_error(sess, "stream", exc,
                                        detail={"stage": "上游连接被重置", "target_url": target,
                                                "bytes_received": sess.bytes_upstream})
                        break
                    except httpx.HTTPError as exc:
                        closed_reason = "upstream_http_error"
                        store.add_error(sess, "stream", exc, detail={"target_url": target})
                        break
                    except OSError as exc:
                        closed_reason = "upstream_network_error"
                        store.add_error(sess, "stream", exc,
                                        detail={"stage": "上游 socket 层错误", "errno": getattr(exc, "errno", None),
                                                "target_url": target})
                        break
                    except Exception as exc:
                        closed_reason = "upstream_unexpected_error"
                        store.add_error(sess, "stream", exc, detail={"target_url": target})
                        break

                    if not chunk:
                        continue
                    src = chunk          # 上游原文
                    emit = chunk         # 发给客户端的字节
                    dec, rew = rw["dec"], rw["rw"]
                    if dec is not None or rew is not None:
                        try:
                            src = dec.decode(chunk) if dec is not None else chunk
                            emit = rew.feed(src) if rew is not None else src
                        except Exception as exc:
                            store.add_error(
                                sess, "stream", exc,
                                message=f"响应解压/改写失败，本会话后续改为原样透传: {exc}",
                                detail={"target_url": target, "bytes_received": sess.bytes_upstream,
                                        "content_encoding": cenc,
                                        "rewrite_rules": ruleset.to_list()},
                                fatal=False)
                            rw["dec"] = rw["rw"] = None
                            src = emit = chunk
                    if emit:
                        # 抓取副本（先抓后发，抓取异常不能影响转发）
                        try:
                            cap = settings.max_chunk_capture
                            store.add_chunk(sess, emit[:cap], len(emit),
                                            src=src[:cap] if src != emit else b"")
                        except Exception as exc:
                            store.add_error(sess, "stream", exc,
                                            message=f"抓取分片到 Dashboard 失败（不影响转发）: {exc}",
                                            fatal=False)
                        yield emit
                # 上游读完了：把解压器/改写器里压着的尾巴吐出去，不能丢字节
                tail = b""
                dec, rew = rw["dec"], rw["rw"]
                try:
                    if dec is not None:
                        tail += dec.flush()
                    if rew is not None:
                        tail = (rew.feed(tail) + rew.flush()) if tail else rew.flush()
                except Exception as exc:
                    store.add_error(sess, "stream", exc,
                                    message=f"冲刷响应改写缓冲失败，可能丢失末尾 {len(tail)} 字节: {exc}",
                                    detail={"target_url": target})
                if tail:
                    try:
                        store.add_chunk(sess, tail[:settings.max_chunk_capture], len(tail),
                                        src=b"")
                    except Exception:
                        pass
                    yield tail

            except (asyncio.CancelledError, GeneratorExit):
                closed_reason = "client_disconnected"
                for task in (read_task, cancel_task):
                    if task is not None and not task.done():
                        task.cancel()
                store.add_error(sess, "client",
                                message="客户端提前断开连接（主动关闭 / 取消订阅 / 网络中断）",
                                detail={"bytes_sent": sess.bytes_upstream, "target_url": target,
                                        "note": "转发被 Starlette 取消，属正常现象之一"},
                                fatal=False)
                raise
            finally:
                if rewriter is not None:
                    hits = rewriter.take_hits()  # noqa: F821  外层变量，只读
                    if hits:
                        sess.response_rewrites = hits
                        store.emit("response_rewritten", {
                            "session_id": sess.id, "session_short_id": sess.id[:8],
                            "rewrites": hits, "target_url": target})
                try:
                    await upstream_resp.aclose()
                except Exception as exc:
                    store.add_error(sess, "stream", exc, message=f"关闭上游连接时出错: {exc}", fatal=False)
                state = "error" if closed_reason in {
                    "read_timeout", "upstream_protocol_error", "upstream_read_error",
                    "upstream_network_error", "upstream_http_error", "upstream_unexpected_error",
                    "idle_timeout", "killed_from_dashboard"} else "done"
                store.close_session(sess, closed_reason, state)

        headers: dict[str, str] = {}
        for k, v in resp_headers:
            if k.lower() in headers:      # 同名头合并（如 Set-Cookie 用逗号会失真，这里保守处理）
                headers[k] = headers[k] + ", " + v
            else:
                headers[k] = v

        return StreamingResponse(
            body_iter(),
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=None,
        )

    async def health(_: Request) -> Response:
        up = store.get_active()
        return JSONResponse({
            "ok": True, "service": "sse-forwarder-proxy",
            "active_upstream": up.to_dict() if up else None,
            "open_sessions": len(store.open_sessions()),
            "stats": dict(store.stats),
        })

    routes = [Route("/__proxy_health__", health, methods=["GET"])]
    routes.append(Route("/{path:path}", handle, methods=ANY_METHODS))
    routes.append(Route("/", handle, methods=ANY_METHODS))

    app = Starlette(routes=routes)
    app.state.store = store
    return app


async def _anext(it: AsyncIterator[bytes]) -> bytes:
    return await it.__anext__()


def _connect_fail(store: Store, sess, settings: Settings, exc: BaseException, phase: str,
                  target: str, upstream: Upstream, human: str) -> Response:
    detail: dict[str, Any] = {
        "phase": phase,
        "human_readable": human,
        "target_url": target,
        "upstream_id": upstream.id,
        "upstream_name": upstream.name,
        "upstream_base_url": upstream.url,
        "client": sess.client,
        "request_method": sess.method,
        "connect_timeout": settings.connect_timeout,
        "read_timeout": settings.read_timeout,
        "verify_tls": settings.verify_tls if upstream.verify_tls is None else upstream.verify_tls,
        "proxy_listen": f"{settings.proxy_host}:{settings.proxy_port}",
    }
    host = urlsplit(target).hostname
    detail["resolved_host"] = host
    try:
        infos = socket.getaddrinfo(host, urlsplit(target).port or (443 if urlsplit(target).scheme == "https" else 80))
        detail["dns_result"] = sorted({i[4][0] for i in infos})
    except Exception as dns_exc:
        detail["dns_result"] = f"解析失败: {type(dns_exc).__name__}: {dns_exc}"
    store.add_error(sess, phase, exc, detail=detail)
    store.close_session(sess, phase, "error")
    body = {
        "error": "upstream_unreachable",
        "phase": phase,
        "message": human,
        "exception": f"{type(exc).__name__}: {exc}",
        "target_url": target,
        "detail": detail,
    }
    return JSONResponse(body, status_code=502)
