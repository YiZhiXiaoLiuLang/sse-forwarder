"""状态中心：上游管理、会话记录、事件总线。

设计要点
- 全内存 + 单事件循环，锁只用于保护字典写入，1~10 并发毫无压力。
- 抓取到的原始字节以 bytes 保存，出 JSON 时转 base64，Dashboard 端再解码，
  这样即使是二进制/非法 UTF-8 也能"原样"看到。
"""
from __future__ import annotations

import asyncio
import base64
import itertools
import json
import logging
import os
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from .config import Settings

log = logging.getLogger("sse-forwarder.store")

# 逐跳头，转发时必须剔除（RFC 7230 6.1）
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def now_ms() -> float:
    return time.time() * 1000.0


def ts_iso(ms: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000.0)) + ".%03d" % (ms % 1000)


# --------------------------------------------------------------------------
# 上游
# --------------------------------------------------------------------------
@dataclass
class Upstream:
    id: str
    name: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    rules: list[dict[str, Any]] = field(default_factory=list)
    verify_tls: Optional[bool] = None
    follow_redirects: Optional[bool] = None
    note: str = ""
    created_at: float = field(default_factory=now_ms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "url": self.url,
            "headers": dict(self.headers),
            "rules": [dict(r) for r in self.rules],
            "verify_tls": self.verify_tls,
            "follow_redirects": self.follow_redirects,
            "note": self.note,
            "created_at": self.created_at,
            "created_at_text": ts_iso(self.created_at),
        }


# --------------------------------------------------------------------------
# 分片 / 错误
# --------------------------------------------------------------------------
@dataclass
class ChunkRecord:
    seq: int
    ts: float
    delta_ms: float          # 距上一分片间隔
    size: int                # 真实字节数
    data: bytes              # 发给客户端的字节（可能被截断）
    src: bytes = b""         # 上游原文，仅在与 data 不同（发生过改写）时保存
    truncated: bool = False

    def to_dict(self, max_payload: int) -> dict[str, Any]:
        payload = self.data
        cut = len(payload) > max_payload
        if cut:
            payload = payload[:max_payload]
        return {
            "seq": self.seq,
            "ts": self.ts,
            "ts_text": ts_iso(self.ts),
            "delta_ms": round(self.delta_ms, 3),
            "size": self.size,
            "captured": len(self.data),
            "truncated": self.truncated or cut,
            "b64": base64.b64encode(payload).decode("ascii"),
            **({"src_b64": base64.b64encode(self.src[:max_payload]).decode("ascii")}
               if self.src else {}),
        }


@dataclass
class ErrorInfo:
    ts: float
    phase: str               # connect / send_request / read_response_headers / stream / client / config
    type: str                # 异常类名
    message: str
    detail: dict[str, Any] = field(default_factory=dict)
    tb: str = ""
    fatal: bool = True       # 是否导致会话终止

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "ts_text": ts_iso(self.ts),
            "phase": self.phase,
            "type": self.type,
            "message": self.message,
            "detail": self.detail,
            "traceback": self.tb,
            "fatal": self.fatal,
        }


# --------------------------------------------------------------------------
# 会话
# --------------------------------------------------------------------------
@dataclass
class Session:
    id: str
    created_at: float
    method: str
    path: str
    query: str
    client: str
    upstream_id: str
    upstream_name: str
    target_url: str
    request_headers: list[tuple[str, str]] = field(default_factory=list)
    request_body: bytes = b""            # 实际转发出去的（改写后）
    request_body_raw: bytes = b""        # 客户端原始 body（改写前），仅在发生改写时保存
    request_body_truncated: bool = False
    request_rewrites: list[dict[str, Any]] = field(default_factory=list)
    response_rewrites: list[dict[str, Any]] = field(default_factory=list)
    rewrite_rules: list[dict[str, Any]] = field(default_factory=list)   # 本次会话用的规则快照
    response_status: Optional[int] = None
    response_reason: str = ""
    response_headers: list[tuple[str, str]] = field(default_factory=list)
    http_version: str = ""
    chunks: list[ChunkRecord] = field(default_factory=list)
    errors: list[ErrorInfo] = field(default_factory=list)
    state: str = "connecting"        # connecting / streaming / done / error / killed
    closed_at: Optional[float] = None
    close_reason: str = ""
    ttfb_ms: Optional[float] = None
    bytes_upstream: int = 0          # 上游 -> 客户端
    bytes_client_body: int = 0       # 客户端 -> 上游（请求体）
    captured_bytes: int = 0
    drop_count: int = 0              # 超出抓取上限后丢弃的分片数
    sse_analysis: dict[str, Any] = field(default_factory=dict)
    cancel_event: Optional[asyncio.Event] = field(default=None, repr=False)

    # ---------- 序列化 ----------
    def summary(self) -> dict[str, Any]:
        ended = self.closed_at or now_ms()
        return {
            "id": self.id,
            "short_id": self.id[:8],
            "created_at": self.created_at,
            "created_at_text": ts_iso(self.created_at),
            "method": self.method,
            "path": self.path,
            "query": self.query,
            "client": self.client,
            "upstream_id": self.upstream_id,
            "upstream_name": self.upstream_name,
            "target_url": self.target_url,
            "state": self.state,
            "status": self.response_status,
            "http_version": self.http_version,
            "duration_ms": round(ended - self.created_at, 1),
            "ttfb_ms": round(self.ttfb_ms, 1) if self.ttfb_ms is not None else None,
            "bytes_upstream": self.bytes_upstream,
            "chunks": len(self.chunks),
            "errors": len(self.errors),
            "last_error": self.errors[-1].message if self.errors else None,
            "close_reason": self.close_reason,
            "request_rewrite_count": sum(x.get("count", 0) for x in self.request_rewrites),
            "response_rewrite_count": sum(x.get("count", 0) for x in self.response_rewrites),
            "rewrite_rules": self.rewrite_rules,
            "content_type": dict(self.response_headers).get("content-type", ""),
            "sse_analysis": self.sse_analysis,
        }

    def detail(self, max_payload: int, with_chunks: bool = True, tail: int = 0) -> dict[str, Any]:
        d = self.summary()
        d.update(
            {
                "request_headers": [{"k": k, "v": v} for k, v in self.request_headers],
                "response_headers": [{"k": k, "v": v} for k, v in self.response_headers],
                "request_body": {
                    "size": len(self.request_body),
                    "truncated": self.request_body_truncated,
                    "b64": base64.b64encode(self.request_body).decode("ascii"),
                    "raw_size": len(self.request_body_raw),
                    "raw_b64": base64.b64encode(self.request_body_raw).decode("ascii"),
                },
                "request_rewrites": self.request_rewrites,
                "response_rewrites": self.response_rewrites,
                "rewrite_rules": self.rewrite_rules,
                "errors": [e.to_dict() for e in self.errors],
                "captured_bytes": self.captured_bytes,
                "drop_count": self.drop_count,
                "closed_at": self.closed_at,
                "closed_at_text": ts_iso(self.closed_at) if self.closed_at else None,
            }
        )
        if with_chunks:
            src = self.chunks[-tail:] if tail > 0 else self.chunks
            d["chunks"] = [c.to_dict(max_payload) for c in src]
        return d


# --------------------------------------------------------------------------
# 事件总线
# --------------------------------------------------------------------------
class EventBus:
    """发布订阅 + 环形回放缓冲，Dashboard 断线重连不丢事件。"""

    def __init__(self, buffer: int = 400) -> None:
        self._subs: dict[str, asyncio.Queue] = {}
        self._buffer: list[dict[str, Any]] = []
        self._buffer_size = buffer
        self._seq = itertools.count(1)

    def subscribe(self) -> tuple[str, asyncio.Queue]:
        sid = uuid.uuid4().hex
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subs[sid] = q
        return sid, q

    def unsubscribe(self, sid: str) -> None:
        self._subs.pop(sid, None)

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)

    def publish(self, kind: str, payload: dict[str, Any]) -> None:
        evt = {"seq": next(self._seq), "kind": kind, "ts": now_ms(), "data": payload}
        self._buffer.append(evt)
        if len(self._buffer) > self._buffer_size:
            del self._buffer[: len(self._buffer) - self._buffer_size]
        dead = []
        for sid, q in self._subs.items():
            try:
                q.put_nowait(evt)
            except asyncio.QueueFull:
                dead.append(sid)   # 消费太慢的订阅者直接踢掉，靠回放补齐
        for sid in dead:
            self._subs.pop(sid, None)

    def replay(self, after_seq: int = 0, limit: int = 200) -> list[dict[str, Any]]:
        out = [e for e in self._buffer if e["seq"] > after_seq]
        return out[-limit:]


# --------------------------------------------------------------------------
# 非标 SSE 探测
# --------------------------------------------------------------------------
def analyze_stream(sample: bytes) -> dict[str, Any]:
    """只做"观察"，绝不改动转发内容。

    识别常见非标情况：缺空行分隔、无 data: 前缀、CRLF/LF 混用、
    裸 JSON 行、BOM、注释心跳行、单次 chunk 内含多个事件等。
    """
    res: dict[str, Any] = {
        "sampled_bytes": len(sample),
        "has_bom": sample.startswith(b"\xef\xbb\xbf"),
        "crlf_count": sample.count(b"\r\n"),
        "bare_lf_count": sample.count(b"\n") - sample.count(b"\r\n"),
        "bare_cr_count": sample.count(b"\r") - sample.count(b"\r\n"),
        "blank_line_separators": 0,
        "data_lines": 0,
        "event_lines": 0,
        "id_lines": 0,
        "retry_lines": 0,
        "comment_lines": 0,
        "non_field_lines": 0,
        "json_like_lines": 0,
        "bare_json_lines": 0,
        "data_json_lines": 0,
        "non_utf8": False,
        "nonstandard": [],
    }
    try:
        text = sample.decode("utf-8")
    except UnicodeDecodeError:
        res["non_utf8"] = True
        text = sample.decode("utf-8", errors="replace")

    lines = text.split("\n")
    fields = {"data", "event", "id", "retry"}
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            res["blank_line_separators"] += 1
            continue
        if line.startswith(":"):
            res["comment_lines"] += 1
            continue
        name = line.split(":", 1)[0].lower() if ":" in line else line.lower()
        if name == "data" and ":" in line:
            res["data_lines"] += 1
            body = line.split(":", 1)[1].lstrip(" ")
            if body[:1] in {"{", "["}:
                res["json_like_lines"] += 1
                res["data_json_lines"] += 1
        elif name == "event":
            res["event_lines"] += 1
        elif name == "id":
            res["id_lines"] += 1
        elif name == "retry":
            res["retry_lines"] += 1
        else:
            res["non_field_lines"] += 1
            stripped = line.strip()
            if stripped[:1] in {"{", "["}:
                res["json_like_lines"] += 1
                res["bare_json_lines"] += 1

    hints = res["nonstandard"]
    if res["bare_lf_count"] and res["crlf_count"]:
        hints.append("行结束符混用（同时存在 CRLF 与 LF）")
    if res["bare_cr_count"]:
        hints.append("存在裸 CR（老式 Mac 行尾），多数 SSE 客户端会解析异常")
    if res["data_lines"] == 0 and res["event_lines"] == 0 and res["non_field_lines"] > 0:
        hints.append("没有任何 data:/event: 字段，疑似裸 JSON / NDJSON / 自定义协议")
    if res["blank_line_separators"] == 0 and (res["data_lines"] + res["non_field_lines"]) > 1:
        hints.append("事件之间没有空行分隔，不符合 SSE 规范")
    if res["bare_json_lines"] > 0:
        hints.append(f"检测到 {res['bare_json_lines']} 行裸 JSON（没有 data: 前缀），标准 EventSource 会直接丢弃")
    if res["has_bom"]:
        hints.append("流开头带 UTF-8 BOM")
    if res["non_utf8"]:
        hints.append("包含非 UTF-8 字节，已用替换字符展示，请以 HEX 视图为准")
    return res


# --------------------------------------------------------------------------
# Store
# --------------------------------------------------------------------------
class Store:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bus = EventBus(settings.event_buffer)
        self._lock = asyncio.Lock()
        self.upstreams: dict[str, Upstream] = {}
        self.active_upstream_id: Optional[str] = None
        self.sessions: dict[str, Session] = {}
        self.order: list[str] = []          # 会话 id，按创建顺序
        self.stats = {"requests": 0, "errors": 0, "bytes_upstream": 0, "started_at": now_ms()}

    # ---------------- 上游 ----------------
    def add_upstream(self, name: str, url: str, headers: dict[str, str] | None = None,
                     note: str = "", verify_tls: Optional[bool] = None,
                     follow_redirects: Optional[bool] = None,
                     upstream_id: Optional[str] = None,
                     rules: list[dict[str, Any]] | None = None) -> Upstream:
        uid = upstream_id or uuid.uuid4().hex[:12]
        up = Upstream(id=uid, name=name or url, url=url.rstrip("/"), headers=headers or {},
                      rules=[dict(r) for r in (rules or [])],
                      note=note, verify_tls=verify_tls, follow_redirects=follow_redirects)
        self.upstreams[uid] = up
        if self.active_upstream_id is None:
            self.active_upstream_id = uid
        return up

    def get_active(self) -> Optional[Upstream]:
        if not self.active_upstream_id:
            return None
        return self.upstreams.get(self.active_upstream_id)

    def activate(self, uid: str) -> Upstream:
        up = self.upstreams.get(uid)
        if up is None:
            raise KeyError(f"上游不存在: {uid}")
        self.active_upstream_id = uid
        self.bus.publish("upstream_switched", {"upstream": up.to_dict()})
        self.save()
        return up

    def remove_upstream(self, uid: str) -> None:
        self.upstreams.pop(uid, None)
        if self.active_upstream_id == uid:
            self.active_upstream_id = next(iter(self.upstreams), None)
        self.bus.publish("upstream_removed", {"id": uid})
        self.save()

    # ---------------- 会话 ----------------
    def new_session(self, **kw: Any) -> Session:
        sid = uuid.uuid4().hex
        s = Session(id=sid, created_at=now_ms(), cancel_event=asyncio.Event(), **kw)
        self.sessions[sid] = s
        self.order.append(sid)
        self.stats["requests"] += 1
        self._evict()
        self.bus.publish("session_created", s.summary())
        return s

    def _evict(self) -> None:
        limit = self.settings.max_sessions
        while len(self.order) > limit:
            old = self.order.pop(0)
            s = self.sessions.pop(old, None)
            if s and s.state == "streaming":
                continue
        # 保底：仍有过多已结束会话时按时间清理
        if len(self.sessions) > limit * 2:
            for sid in sorted(self.sessions, key=lambda x: self.sessions[x].created_at)[: len(self.sessions) - limit]:
                if self.sessions[sid].state != "streaming":
                    self.sessions.pop(sid, None)
                    if sid in self.order:
                        self.order.remove(sid)

    def list_sessions(self, limit: int = 100, only_open: bool = False) -> list[dict[str, Any]]:
        items = [self.sessions[i] for i in reversed(self.order) if i in self.sessions]
        if only_open:
            items = [s for s in items if s.state in {"connecting", "streaming"}]
        return [s.summary() for s in items[:limit]]

    def open_sessions(self) -> list[Session]:
        return [s for s in self.sessions.values() if s.state in {"connecting", "streaming"}]

    # ---------------- 事件 ----------------
    def emit(self, kind: str, payload: dict[str, Any]) -> None:
        self.bus.publish(kind, payload)

    def add_chunk(self, s: Session, data: bytes, size: int,
                  src: bytes = b"") -> Optional[dict[str, Any]]:
        last_ts = s.chunks[-1].ts if s.chunks else s.created_at
        rec = ChunkRecord(
            seq=len(s.chunks) + 1,
            ts=now_ms(),
            delta_ms=now_ms() - last_ts,
            size=size,
            data=bytes(data),
            src=bytes(src) if src and src != data else b"",
            truncated=len(data) < size,
        )
        s.bytes_upstream += size
        self.stats["bytes_upstream"] += size
        over_budget = s.captured_bytes + len(data) > self.settings.max_capture_bytes
        if len(s.chunks) >= self.settings.max_chunks_per_session or over_budget:
            s.drop_count += 1
            rec.data = b""
            rec.truncated = True
        else:
            s.captured_bytes += len(rec.data)
            s.chunks.append(rec)
        s.sse_analysis = analyze_stream(_recent_sample(s))
        payload = rec.to_dict(self.settings.max_event_payload)
        payload["session_id"] = s.id
        payload["session_short_id"] = s.id[:8]
        payload["state"] = s.state
        payload["bytes_upstream"] = s.bytes_upstream
        self.emit("chunk", payload)
        return payload

    def add_error(self, s: Session | None, phase: str, exc: BaseException | None = None,
                  *, message: str = "", detail: dict[str, Any] | None = None,
                  fatal: bool = True, type_name: str = "") -> ErrorInfo:
        detail = dict(detail or {})
        if exc is not None:
            type_name = type_name or type(exc).__name__
            message = message or f"{type(exc).__name__}: {exc}"
            detail.setdefault("exception_module", type(exc).__module__)
            detail.setdefault("exception_args", [repr(a) for a in exc.args])
            for attr in ("errno", "strerror", "filename", "code", "status_code"):
                v = getattr(exc, attr, None)
                if v is not None:
                    detail[attr] = str(v)
            cause = exc.__cause__ or exc.__context__
            if cause is not None and cause is not exc:
                detail["cause"] = f"{type(cause).__name__}: {cause}"
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        else:
            tb = detail.pop("traceback", "") or "".join(traceback.format_stack(limit=12))
        err = ErrorInfo(ts=now_ms(), phase=phase, type=type_name or "Error",
                        message=message or "unknown error", detail=detail, tb=tb, fatal=fatal)
        self.stats["errors"] += 1
        if s is not None:
            s.errors.append(err)
            if fatal:
                s.state = "error"
        payload = err.to_dict()
        if s is not None:
            payload["session_id"] = s.id
            payload["session_short_id"] = s.id[:8]
            payload["target_url"] = s.target_url
            payload["upstream_name"] = s.upstream_name
        self.emit("error", payload)
        log.error("[%s] %s phase=%s %s", (s.id[:8] if s else "-"), err.type, phase, err.message)
        return err

    def close_session(self, s: Session, reason: str, state: str = "done") -> None:
        if s.closed_at:
            return
        s.closed_at = now_ms()
        s.close_reason = reason
        if state:
            s.state = state
        s.sse_analysis = analyze_stream(_recent_sample(s))
        self.emit("session_closed", s.summary())
        self._evict()

    # ---------------- 持久化 ----------------
    def save(self) -> None:
        path = self.settings.state_file
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            data = {
                "active_upstream_id": self.active_upstream_id,
                "upstreams": [u.to_dict() for u in self.upstreams.values()],
            }
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
        except Exception as exc:  # 持久化失败绝不能影响转发
            log.warning("状态持久化失败: %s", exc)

    def load(self) -> None:
        path = self.settings.state_file
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for u in data.get("upstreams", []):
                self.add_upstream(
                    name=u.get("name", ""), url=u.get("url", ""), headers=u.get("headers") or {},
                    note=u.get("note", ""), verify_tls=u.get("verify_tls"),
                    follow_redirects=u.get("follow_redirects"), upstream_id=u.get("id"),
                    rules=u.get("rules") or [],
                )
            act = data.get("active_upstream_id")
            if act in self.upstreams:
                self.active_upstream_id = act
            log.info("已从 %s 恢复 %d 个上游", path, len(self.upstreams))
        except Exception as exc:
            log.warning("状态加载失败（忽略，使用默认配置）: %s", exc)


def _recent_sample(s: Session, limit: int = 256 * 1024) -> bytes:
    """取最近若干字节做非标探测，避免每次全量扫描。"""
    out = bytearray()
    for c in reversed(s.chunks):
        out[:0] = c.data
        if len(out) >= limit:
            break
    return bytes(out[-limit:])
