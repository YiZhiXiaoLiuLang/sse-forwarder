"""字面改写引擎：请求方向 本地值→上游值，响应方向 上游值→本地值。

针对大模型接口设计：把客户端带的 `sk-本地key` 换成上游要求的 `sk-上游key`，
把本地模型名换成上游模型名。规则是"一对值"，同一条规则自动管住双向。

实现原则
- **纯字节级字面替换**，不解析 JSON、不管字段语义，接口再复杂也不会漏
- 最长模式优先，避免短模式先吃掉长模式的一部分
- 流式改写用滑动窗口：每次只吐出「离尾巴 max_pattern-1 字节之前」的部分，
  这样跨 TCP 分片被拦腰截断的 key / 模型名也能被完整替换，同时不会攒数据造成延迟
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence

Pair = tuple[bytes, bytes, str]     # (查找, 替换为, 规则名)

VALID_SCOPES = {"both", "request", "response"}


@dataclass
class Rule:
    src: str = ""            # 本地值（客户端用的）
    dst: str = ""            # 上游值（上游要求的）
    label: str = ""
    scope: str = "both"      # both / request / response
    enabled: bool = True

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "Rule":
        return Rule(
            src=str(d.get("src") or d.get("from") or d.get("local") or ""),
            dst=str(d.get("dst") or d.get("to") or d.get("upstream") or ""),
            label=str(d.get("label") or ""),
            scope=(str(d.get("scope") or "both").lower()
                   if str(d.get("scope") or "both").lower() in VALID_SCOPES else "both"),
            enabled=bool(d.get("enabled", True)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {"src": self.src, "dst": self.dst, "label": self.label,
                "scope": self.scope, "enabled": self.enabled}


def _replace_all(data: bytes, pairs: Sequence[Pair]) -> tuple[bytes, list[dict[str, Any]]]:
    hits: list[dict[str, Any]] = []
    for needle, repl, label in pairs:
        if not needle:
            continue
        n = data.count(needle)
        if n:
            data = data.replace(needle, repl)
            hits.append({"label": label, "count": n,
                         "src": needle.decode("utf-8", "replace"),
                         "dst": repl.decode("utf-8", "replace")})
    return data, hits


class RuleSet:
    def __init__(self, rules: Iterable[Rule] = ()) -> None:
        self.rules: list[Rule] = list(rules)
        req: list[Pair] = []
        resp: list[Pair] = []
        for r in self.rules:
            if not r.enabled or not r.src or not r.dst or r.src == r.dst:
                continue          # 空规则 / 自己换自己，跳过
            sb, db = r.src.encode("utf-8"), r.dst.encode("utf-8")
            label = r.label or f"{r.src[:12]}→{r.dst[:12]}"
            if r.scope in {"both", "request"}:
                req.append((sb, db, label))
            if r.scope in {"both", "response"}:
                resp.append((db, sb, label))
        # 最长优先，避免短模式先命中长模式的子串
        req.sort(key=lambda p: -len(p[0]))
        resp.sort(key=lambda p: -len(p[0]))
        self.request_pairs: list[Pair] = req
        self.response_pairs: list[Pair] = resp

    # ---------------- 构造 / 序列化 ----------------
    @staticmethod
    def from_list(items: Optional[Sequence[Any]]) -> "RuleSet":
        if not items:
            return RuleSet()
        out = []
        for it in items:
            if isinstance(it, Rule):
                out.append(it)
            elif isinstance(it, dict):
                out.append(Rule.from_dict(it))
        return RuleSet(out)

    def to_list(self) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self.rules]

    @property
    def empty(self) -> bool:
        return not self.request_pairs and not self.response_pairs

    @property
    def max_request_pattern(self) -> int:
        return max((len(p[0]) for p in self.request_pairs), default=0)

    @property
    def max_response_pattern(self) -> int:
        return max((len(p[0]) for p in self.response_pairs), default=0)

    # ---------------- 一次性数据 ----------------
    def apply_request(self, data: bytes) -> tuple[bytes, list[dict[str, Any]]]:
        if not self.request_pairs or not data:
            return data, []
        out, hits = _replace_all(data, self.request_pairs)
        for h in hits:
            h["direction"] = "request"
        return out, hits

    def apply_response(self, data: bytes) -> tuple[bytes, list[dict[str, Any]]]:
        if not self.response_pairs or not data:
            return data, []
        out, hits = _replace_all(data, self.response_pairs)
        for h in hits:
            h["direction"] = "response"
        return out, hits

    def apply_request_headers(self, headers: Sequence[tuple[str, str]]
                              ) -> tuple[list[tuple[str, str]], list[dict[str, Any]]]:
        if not self.request_pairs:
            return list(headers), []
        out: list[tuple[str, str]] = []
        hits: list[dict[str, Any]] = []
        agg: dict[str, dict[str, Any]] = {}
        for name, value in headers:
            raw = value.encode("latin-1", "replace")
            new, h = _replace_all(raw, self.request_pairs)
            for hit in h:
                key = hit["label"]
                slot = agg.setdefault(key, {"label": key, "count": 0, "direction": "request",
                                            "src": hit["src"], "dst": hit["dst"], "in": "headers"})
                slot["count"] += hit["count"]
            out.append((name, new.decode("latin-1", "replace")))
        hits.extend(agg.values())
        return out, hits


def _find_matches(data: bytes, pairs: Sequence[Pair]) -> list[tuple[int, int, bytes, str]]:
    """从左到右扫描，同一位置取最长模式（pairs 已按长度降序）。返回 (start, end, repl, label)。"""
    out: list[tuple[int, int, bytes, str]] = []
    i, n = 0, len(data)
    while i < n:
        for needle, repl, label in pairs:
            ln = len(needle)
            if ln and data.startswith(needle, i):
                out.append((i, i + ln, repl, label))
                i += ln
                break
        else:
            i += 1
    return out


def _build(data: bytes, matches: Sequence[tuple[int, int, bytes, str]], limit: int,
           agg: Optional[dict[str, dict[str, Any]]] = None) -> bytes:
    """把 [0, limit) 区间按命中列表拼装成输出字节。"""
    parts: list[bytes] = []
    pos = 0
    for st, en, repl, label in matches:
        if st >= limit:
            break
        parts.append(data[pos:st])
        parts.append(repl)
        pos = max(pos, en)
        if agg is not None:
            slot = agg.get(label)
            if slot is None:
                agg[label] = {"label": label, "count": 1, "src": data[st:en].decode("utf-8", "replace"),
                              "dst": repl.decode("utf-8", "replace")}
            else:
                slot["count"] += 1
    parts.append(data[pos:limit])
    return b"".join(parts)


class StreamRewriter:
    """流式改写：跨分片安全，且不会无限攒数据。

    每次 feed 的安全发射边界 E = max(len(buf) - hold, 所有「起点在安全线之前」的命中的末尾)。
    hold = 最长模式 - 1，保证任何可能延续到未来的半截模式都留在 pending 里；
    而已经完整落在 buf 内的命中会被立即处理，不会因为跨边界而漏掉。
    """

    def __init__(self, ruleset: RuleSet, direction: str = "response") -> None:
        self.direction = direction
        self.pairs = ruleset.response_pairs if direction == "response" else ruleset.request_pairs
        self.hold = max((len(p[0]) for p in self.pairs), default=0) - 1
        if self.hold < 0:
            self.hold = 0
        self.pending: bytes = b""
        self.hits: list[dict[str, Any]] = []
        self._agg: dict[str, dict[str, Any]] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.pairs)

    def feed(self, chunk: bytes) -> bytes:
        if not self.pairs:
            return chunk
        buf = self.pending + chunk if self.pending else chunk
        cut = len(buf) - self.hold
        if cut <= 0:
            self.pending = buf
            return b""
        matches = _find_matches(buf, self.pairs)
        limit = cut
        for st, en, _repl, _label in matches:
            if st < cut and en > limit:
                limit = en
        out = _build(buf, matches, limit, self._agg)
        self.pending = buf[limit:]
        return out

    def flush(self) -> bytes:
        if not self.pairs:
            return b""
        buf, self.pending = self.pending, b""
        if not buf:
            return b""
        return _build(buf, _find_matches(buf, self.pairs), len(buf), self._agg)

    def take_hits(self) -> list[dict[str, Any]]:
        self.hits = [{"label": v["label"], "count": v["count"], "direction": self.direction,
                      "src": v["src"], "dst": v["dst"]} for v in self._agg.values()]
        return self.hits
