"""Dashboard 后端：会话查询、上游切换、实时事件流（SSE）、上游连通性探测。"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import ssl
import time
from typing import Any, Optional
from urllib.parse import urlsplit

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Mount, Route

from .config import Settings
from .proxy import ClientPool, build_target_url
from .store import Store, analyze_stream, ts_iso

log = logging.getLogger("sse-forwarder.dashboard")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _norm_rules(raw: Any) -> list[dict[str, Any]]:
    """校验并规范化改写规则：[{label,src,dst,scope,enabled}]，src=本地值 dst=上游值。"""
    if raw in (None, ""):
        return []
    if not isinstance(raw, list):
        raise ValueError("rules 必须是数组，形如 [{\"src\":\"sk-本地\",\"dst\":\"sk-上游\"}]")
    out: list[dict[str, Any]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"rules[{i}] 必须是对象")
        src = str(item.get("src") or item.get("from") or item.get("local") or "")
        dst = str(item.get("dst") or item.get("to") or item.get("upstream") or "")
        scope = str(item.get("scope") or "both").lower()
        if scope not in ("both", "request", "response"):
            raise ValueError(f"rules[{i}].scope 只能是 both/request/response，收到 {scope}")
        if not src and not dst:
            continue                      # 整条空规则，忽略
        if bool(src) != bool(dst):
            raise ValueError(f"rules[{i}] 的本地值与上游值必须同时填写（只填了一个无法构成映射）")
        out.append({"label": str(item.get("label") or ""), "src": src, "dst": dst,
                    "scope": scope, "enabled": bool(item.get("enabled", True))})
    return out


def _err(status: int, message: str, **kw: Any) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message, **kw}, status_code=status)


def make_dashboard_app(store: Store, settings: Settings, pool: ClientPool) -> Starlette:
    token = settings.dashboard_token

    def authorized(request: Request) -> bool:
        if not token:
            return True
        got = request.headers.get("x-dashboard-token") or request.query_params.get("token") or ""
        return got == token

    def guard(request: Request) -> Optional[JSONResponse]:
        if authorized(request):
            return None
        return _err(401, "Dashboard 鉴权失败：请通过 ?token= 或 X-Dashboard-Token 头访问")

    # ---------------- 页面 ----------------
    async def index(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    # ---------------- 状态 ----------------
    async def state(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        active = store.get_active()
        return JSONResponse({
            "ok": True,
            "settings": {
                "proxy_listen": f"{settings.proxy_host}:{settings.proxy_port}",
                "dashboard_listen": f"{settings.dashboard_host}:{settings.dashboard_port}",
                "force_identity_encoding": settings.force_identity_encoding,
                "http2": settings.http2,
                "follow_redirects": settings.follow_redirects,
                "verify_tls": settings.verify_tls,
                "connect_timeout": settings.connect_timeout,
                "read_timeout": settings.read_timeout,
                "idle_timeout": settings.idle_timeout,
                "max_capture_bytes": settings.max_capture_bytes,
                "max_chunk_capture": settings.max_chunk_capture,
                "preserve_client_host": settings.preserve_client_host,
                "rewrite_enabled": settings.rewrite_enabled,
                "rewrite_force_identity": settings.rewrite_force_identity,
                "rewrite_max_body": settings.rewrite_max_body,
            },
            "upstreams": [u.to_dict() for u in store.upstreams.values()],
            "active_upstream_id": store.active_upstream_id,
            "active_upstream": active.to_dict() if active else None,
            "stats": {**store.stats, "started_at_text": ts_iso(store.stats["started_at"])},
            "open_sessions": len(store.open_sessions()),
            "total_sessions": len(store.sessions),
            "subscribers": store.bus.subscriber_count,
            "server_time": ts_iso(time.time() * 1000),
        })

    # ---------------- 会话 ----------------
    async def list_sessions(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        limit = int(request.query_params.get("limit", "100"))
        only_open = request.query_params.get("open") in {"1", "true"}
        return JSONResponse({"ok": True, "sessions": store.list_sessions(limit=limit, only_open=only_open)})

    def _find(sid: str):
        if sid in store.sessions:
            return store.sessions[sid]
        for s in store.sessions.values():
            if s.id.startswith(sid):
                return s
        return None

    async def session_detail(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        s = _find(request.path_params["sid"])
        if s is None:
            return _err(404, f"会话不存在: {request.path_params['sid']}")
        tail = int(request.query_params.get("tail", "0"))
        with_chunks = request.query_params.get("chunks", "1") not in {"0", "false"}
        return JSONResponse({"ok": True, "session": s.detail(settings.max_event_payload,
                                                             with_chunks=with_chunks, tail=tail)})

    async def kill_session(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        s = _find(request.path_params["sid"])
        if s is None:
            return _err(404, f"会话不存在: {request.path_params['sid']}")
        if s.cancel_event:
            s.cancel_event.set()
        store.add_error(s, "stream", message="Dashboard 手动终止该会话",
                        detail={"action": "kill", "operator": request.client.host if request.client else "?"})
        return JSONResponse({"ok": True, "session": s.summary()})

    async def kill_all(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        n = 0
        for s in store.open_sessions():
            if s.cancel_event:
                s.cancel_event.set()
            n += 1
        return JSONResponse({"ok": True, "killed": n})

    async def clear_history(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        keep = {sid: s for sid, s in store.sessions.items() if s.state in {"connecting", "streaming"}}
        store.sessions = keep
        store.order = list(keep.keys())
        store.emit("history_cleared", {"kept_open": len(keep)})
        return JSONResponse({"ok": True, "kept_open": len(keep)})

    # ---------------- 上游 ----------------
    async def _body_json(request: Request) -> dict[str, Any]:
        try:
            data = await request.json()
            if not isinstance(data, dict):
                raise ValueError("请求体必须是 JSON 对象")
            return data
        except Exception as exc:
            raise ValueError(f"请求体不是合法 JSON: {exc}") from exc

    def _validate_url(url: str) -> Optional[str]:
        if not url:
            return "url 不能为空"
        p = urlsplit(url)
        if p.scheme not in {"http", "https"}:
            return f"只支持 http/https，收到 scheme={p.scheme or '(空)'}"
        if not p.hostname:
            return f"无法从 {url} 解析出主机名"
        return None

    async def create_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        try:
            data = await _body_json(request)
        except ValueError as exc:
            return _err(400, str(exc))
        url = (data.get("url") or "").strip()
        bad = _validate_url(url)
        if bad:
            return _err(400, bad, url=url)
        up = store.add_upstream(
            name=(data.get("name") or "").strip() or url,
            url=url,
            headers=data.get("headers") or {},
            note=data.get("note", ""),
            verify_tls=data.get("verify_tls"),
            follow_redirects=data.get("follow_redirects"),
            rules=_norm_rules(data.get("rules")),
        )
        if data.get("activate", True):
            store.activate(up.id)
        store.save()
        store.emit("upstream_created", up.to_dict())
        return JSONResponse({"ok": True, "upstream": up.to_dict(), "active": store.active_upstream_id == up.id})

    async def update_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        uid = request.path_params["uid"]
        up = store.upstreams.get(uid)
        if up is None:
            return _err(404, f"上游不存在: {uid}")
        try:
            data = await _body_json(request)
        except ValueError as exc:
            return _err(400, str(exc))
        if "url" in data:
            bad = _validate_url((data["url"] or "").strip())
            if bad:
                return _err(400, bad, url=data.get("url"))
            up.url = data["url"].strip().rstrip("/")
        if "name" in data:
            up.name = data["name"] or up.url
        if "headers" in data:
            up.headers = data["headers"] or {}
        if "note" in data:
            up.note = data["note"]
        if "verify_tls" in data:
            up.verify_tls = data["verify_tls"]
        if "follow_redirects" in data:
            up.follow_redirects = data["follow_redirects"]
        if "rules" in data:
            try:
                up.rules = _norm_rules(data["rules"])
            except ValueError as exc:
                return _err(400, str(exc))
        store.save()
        store.emit("upstream_updated", up.to_dict())
        return JSONResponse({"ok": True, "upstream": up.to_dict()})

    async def delete_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        uid = request.path_params["uid"]
        if uid not in store.upstreams:
            return _err(404, f"上游不存在: {uid}")
        name = store.upstreams[uid].name
        store.remove_upstream(uid)
        return JSONResponse({"ok": True, "deleted": uid, "name": name,
                             "active_upstream_id": store.active_upstream_id})

    async def activate_upstream(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        uid = request.path_params["uid"]
        try:
            data = await _body_json(request)
        except ValueError:
            data = {}
        kill_open = bool(data.get("kill_open", False))
        try:
            up = store.activate(uid)
        except KeyError as exc:
            return _err(404, str(exc))
        killed = 0
        if kill_open:
            for s in store.open_sessions():
                if s.upstream_id != uid and s.cancel_event:
                    s.cancel_event.set()
                    killed += 1
        return JSONResponse({"ok": True, "active_upstream": up.to_dict(),
                             "killed_open_sessions": killed,
                             "note": "已建立的流仍走旧上游直到结束；勾选断开可让客户端重连到新上游"})

    async def test_upstream(request: Request) -> Response:
        """主动探测上游连通性，返回逐层（DNS/TCP/TLS/HTTP）的详细结果。"""
        g = guard(request)
        if g:
            return g
        uid = request.path_params.get("uid")
        try:
            data = await _body_json(request)
        except ValueError:
            data = {}
        url = (data.get("url") or "").strip()
        path = data.get("path") or "/__probe__"
        if uid:
            up = store.upstreams.get(uid)
            if up is None:
                return _err(404, f"上游不存在: {uid}")
            url = url or up.url
        bad = _validate_url(url)
        if bad:
            return _err(400, bad, url=url)
        target = build_target_url(url, path, "")
        report: dict[str, Any] = {"target_url": target, "steps": [], "ok": False}
        host = urlsplit(target).hostname
        port = urlsplit(target).port or (443 if urlsplit(target).scheme == "https" else 80)

        def step(name: str, ok: bool, ms: float, detail: Any = "") -> None:
            report["steps"].append({"step": name, "ok": ok, "ms": round(ms, 2), "detail": detail})

        # 1) DNS
        t = time.perf_counter()
        try:
            infos = await asyncio.get_event_loop().getaddrinfo(host, port)
            ips = sorted({i[4][0] for i in infos})
            step("dns", True, (time.perf_counter() - t) * 1000, ips)
        except Exception as exc:
            step("dns", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
            report["error"] = f"DNS 解析失败: {exc}"
            store.emit("probe_result", report)
            return JSONResponse({"ok": False, "report": report}, status_code=200)

        # 2) TCP
        t = time.perf_counter()
        try:
            fut = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(fut, timeout=settings.connect_timeout)
            step("tcp", True, (time.perf_counter() - t) * 1000, f"{host}:{port} 连接成功")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        except asyncio.TimeoutError:
            step("tcp", False, (time.perf_counter() - t) * 1000, f"连接超时（>{settings.connect_timeout}s）")
            report["error"] = "TCP 连接超时"
            store.emit("probe_result", report)
            return JSONResponse({"ok": False, "report": report})
        except Exception as exc:
            step("tcp", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
            report["error"] = f"TCP 连接失败: {exc}"
            store.emit("probe_result", report)
            return JSONResponse({"ok": False, "report": report})

        # 3) TLS（仅 https）
        if urlsplit(target).scheme == "https":
            t = time.perf_counter()
            verify = settings.verify_tls
            if uid and store.upstreams.get(uid) and store.upstreams[uid].verify_tls is not None:
                verify = store.upstreams[uid].verify_tls
            try:
                ctx = ssl.create_default_context()
                if not verify:
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                fut = asyncio.open_connection(host, port, ssl=ctx, server_hostname=host)
                reader, writer = await asyncio.wait_for(fut, timeout=settings.connect_timeout)
                sock = writer.get_extra_info("ssl_object")
                step("tls", True, (time.perf_counter() - t) * 1000,
                     {"version": sock.version() if sock else None,
                      "cipher": sock.cipher() if sock else None,
                      "cert_expiry": sock.getpeercert().get("notAfter") if sock and sock.getpeercert() else None})
                writer.close()
            except ssl.SSLError as exc:
                step("tls", False, (time.perf_counter() - t) * 1000, f"ssl.SSLError: {exc}")
                report["error"] = f"TLS 握手失败: {exc}"
                store.emit("probe_result", report)
                return JSONResponse({"ok": False, "report": report})
            except Exception as exc:
                step("tls", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
                report["error"] = f"TLS 握手失败: {exc}"
                store.emit("probe_result", report)
                return JSONResponse({"ok": False, "report": report})

        # 4) HTTP
        t = time.perf_counter()
        try:
            client = pool.get(settings.verify_tls, settings.http2)
            async with client.stream("GET", target,
                                     timeout=httpx.Timeout(connect=settings.connect_timeout,
                                                           read=5.0, write=5.0, pool=5.0),
                                     follow_redirects=False) as r:
                body = b""
                async for c in r.aiter_raw():
                    body += c
                    if len(body) > 4096:
                        break
                step("http", True, (time.perf_counter() - t) * 1000,
                     {"status": r.status_code, "reason": r.reason_phrase,
                      "http_version": r.http_version,
                      "content_type": r.headers.get("content-type"),
                      "headers": {k: v for k, v in r.headers.items()},
                      "body_preview": body[:512].decode("utf-8", "replace")})
                report["ok"] = r.status_code < 500
                report["status"] = r.status_code
        except Exception as exc:
            step("http", False, (time.perf_counter() - t) * 1000, f"{type(exc).__name__}: {exc}")
            report["error"] = f"HTTP 请求失败: {type(exc).__name__}: {exc}"
        store.emit("probe_result", report)
        return JSONResponse({"ok": report["ok"], "report": report})

    # ---------------- 实时事件流 ----------------
    async def events(request: Request) -> Response:
        g = guard(request)
        if g:
            return g
        after = int(request.query_params.get("after_seq", "0") or 0)

        async def gen():
            sid, q = store.bus.subscribe()
            try:
                snap = {
                    "kind": "snapshot",
                    "seq": 0,
                    "ts": time.time() * 1000,
                    "data": {
                        "sessions": store.list_sessions(limit=50),
                        "upstreams": [u.to_dict() for u in store.upstreams.values()],
                        "active_upstream_id": store.active_upstream_id,
                        "stats": store.stats,
                    },
                }
                yield "event: snapshot\ndata: " + json.dumps(snap, ensure_ascii=False) + "\n\n"
                for evt in store.bus.replay(after_seq=after, limit=200):
                    yield f"event: {evt['kind']}\nid: {evt['seq']}\ndata: " + \
                        json.dumps(evt, ensure_ascii=False) + "\n\n"
                while True:
                    try:
                        evt = await asyncio.wait_for(q.get(), timeout=15)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    yield f"event: {evt['kind']}\nid: {evt['seq']}\ndata: " + \
                        json.dumps(evt, ensure_ascii=False) + "\n\n"
            except asyncio.CancelledError:
                raise
            finally:
                store.bus.unsubscribe(sid)

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache, no-transform",
                                          "X-Accel-Buffering": "no", "Connection": "keep-alive"})

    async def health(_: Request) -> Response:
        return JSONResponse({"ok": True, "service": "sse-forwarder-dashboard"})

    routes = [
        Route("/", index, methods=["GET"]),
        Route("/api/state", state, methods=["GET"]),
        Route("/api/health", health, methods=["GET"]),
        Route("/api/sessions", list_sessions, methods=["GET"]),
        Route("/api/sessions/clear", clear_history, methods=["POST"]),
        Route("/api/sessions/kill-all", kill_all, methods=["POST"]),
        Route("/api/sessions/{sid}", session_detail, methods=["GET"]),
        Route("/api/sessions/{sid}/kill", kill_session, methods=["POST"]),
        Route("/api/upstreams", create_upstream, methods=["POST"]),
        Route("/api/upstreams/{uid}", update_upstream, methods=["PUT"]),
        Route("/api/upstreams/{uid}", delete_upstream, methods=["DELETE"]),
        Route("/api/upstreams/{uid}/activate", activate_upstream, methods=["POST"]),
        Route("/api/upstreams/{uid}/test", test_upstream, methods=["POST"]),
        Route("/api/upstreams/test", test_upstream, methods=["POST"]),
        Route("/api/events", events, methods=["GET"]),
    ]
    app = Starlette(routes=routes)
    app.state.store = store
    return app
