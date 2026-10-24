"""改写引擎单元测试（先写测试，看着它失败，再实现）。

运行：python3 tests/test_rewrite.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS, FAIL = [], []


def ok(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(("  ✔ " if cond else "  ✘ ") + name + (f"   {extra}" if extra else ""), flush=True)


def run():
    from app.rewrite import Rule, RuleSet, StreamRewriter   # noqa: F401  故意后置导入，未实现时直接报错

    LOCAL_KEY = "sk-local-aaaa1111bbbb2222cccc3333dddd4444"
    UP_KEY = "sk-upstream-zzzz9999yyyy8888xxxx7777wwww6666"
    LOCAL_MODEL = "local-qwen3-max-2026-01-25-thinking"
    UP_MODEL = "qwen-max-upstream-v3"

    print("[A] 规则构造与序列化")
    rs = RuleSet.from_list([
        {"label": "API Key", "src": LOCAL_KEY, "dst": UP_KEY},
        {"label": "模型名", "src": LOCAL_MODEL, "dst": UP_MODEL},
    ])
    ok("两条规则都被载入", len(rs.request_pairs) == 2 and len(rs.response_pairs) == 2)
    ok("to_dict 可往返", RuleSet.from_list(rs.to_list()).request_pairs == rs.request_pairs)
    ok("空规则集判定为空", RuleSet.from_list([]).empty is True)
    ok("src==dst 的规则被忽略（无意义）",
       len(RuleSet.from_list([{"src": "x", "dst": "x"}]).request_pairs) == 0)
    ok("src 或 dst 为空的规则被忽略",
       len(RuleSet.from_list([{"src": "", "dst": "y"}, {"src": "z", "dst": ""}]).request_pairs) == 0)
    ok("enabled=False 的规则不参与替换",
       RuleSet.from_list([{"src": "a", "dst": "b", "enabled": False}]).empty is True)

    print("\n[B] 请求方向：本地值 -> 上游值")
    body = ('{"model": "%s", "messages": [{"role":"user","content":"hi %s"}], '
            '"stream": true}' % (LOCAL_MODEL, LOCAL_KEY)).encode()
    out, hits = rs.apply_request(body)
    ok("body 中本地 key 被替换为上游 key", UP_KEY.encode() in out and LOCAL_KEY.encode() not in out)
    ok("body 中本地模型名被替换为上游模型名", UP_MODEL.encode() in out and LOCAL_MODEL.encode() not in out)
    ok("未命中的部分原样保留", b'"stream": true' in out and out.startswith(b'{"model":'))
    ok("命中统计按规则分别计数",
       {h["label"]: h["count"] for h in hits} == {"API Key": 1, "模型名": 1}, str(hits))
    ok("多处出现全部替换（不只管第一处）",
       rs.apply_request((LOCAL_KEY + LOCAL_KEY).encode())[0] == (UP_KEY * 2).encode())
    ok("没有命中时字节完全不变", rs.apply_request(b'{"model":"gpt-4"}')[0] == b'{"model":"gpt-4"}')
    ok("没有命中时 hits 为空", rs.apply_request(b'{"model":"gpt-4"}')[1] == [])

    print("\n[C] 请求头替换（含 Authorization / 自定义头）")
    headers = [("Authorization", f"Bearer {LOCAL_KEY}"), ("X-Api-Key", LOCAL_KEY),
               ("Content-Type", "application/json"), ("Host", "example.com")]
    new_headers, hhits = rs.apply_request_headers(headers)
    d = dict(new_headers)
    ok("Authorization 头被替换", d["Authorization"] == f"Bearer {UP_KEY}", d["Authorization"])
    ok("自定义头 X-Api-Key 被替换", d["X-Api-Key"] == UP_KEY)
    ok("无关头保持原样", d["Content-Type"] == "application/json" and d["Host"] == "example.com")
    ok("头替换有命中记录", sum(h["count"] for h in hhits) == 2, str(hhits))

    print("\n[D] 响应方向：上游值 -> 本地值")
    resp = ('data: {"id":"1","model":"%s","choices":[{"delta":{"content":"k=%s"}}]}\n\n'
            % (UP_MODEL, UP_KEY)).encode()
    out, hits = rs.apply_response(resp)
    ok("响应中上游模型名换回本地模型名", LOCAL_MODEL.encode() in out and UP_MODEL.encode() not in out)
    ok("响应中上游 key 换回本地 key", LOCAL_KEY.encode() in out and UP_KEY.encode() not in out)
    ok("响应 hits 方向标记正确", all(h["direction"] == "response" for h in hits), str(hits))

    print("\n[E] scope 控制方向")
    req_only = RuleSet.from_list([{"src": LOCAL_KEY, "dst": UP_KEY, "scope": "request"}])
    out, _ = req_only.apply_response(f"leak {UP_KEY}".encode())
    ok("scope=request 的规则不改响应", out == f"leak {UP_KEY}".encode())
    resp_only = RuleSet.from_list([{"src": LOCAL_MODEL, "dst": UP_MODEL, "scope": "response"}])
    out, _ = resp_only.apply_request(f'{{"model":"{LOCAL_MODEL}"}}'.encode())
    ok("scope=response 的规则不改请求", out == f'{{"model":"{LOCAL_MODEL}"}}'.encode())

    print("\n[F] 流式改写：模式被 TCP 分片拦腰截断")
    w = StreamRewriter(rs, direction="response")
    pieces = [b'data: {"model":"', UP_MODEL[:6].encode(), UP_MODEL[6:].encode(), b'","ok":true}\n\n']
    got = b"".join(w.feed(p) for p in pieces) + w.flush()
    ok("跨分片的模型名被完整替换", got == b'data: {"model":"' + LOCAL_MODEL.encode() + b'","ok":true}\n\n',
       repr(got[:80]))
    ok("没有吞掉尾部数据", got.endswith(b'","ok":true}\n\n'))

    w = StreamRewriter(rs, direction="response")
    one = UP_KEY.encode()
    got = b"".join(w.feed(bytes([b])) for b in one) + w.flush()
    ok("逐字节喂入也能正确替换", got == LOCAL_KEY.encode(), repr(got[:40]))

    w = StreamRewriter(rs, direction="response")
    got = b"".join(w.feed(p) for p in [b'plain ', b'text no match ', b'at all\n']) + w.flush()
    ok("无匹配时内容一字不改", got == b'plain text no match at all\n', repr(got))

    w = StreamRewriter(RuleSet.from_list([]), direction="response")
    got = b"".join(w.feed(p) for p in [b'a', b'b']) + w.flush()
    ok("空规则集：流式改写完全透传且不留缓冲", got == b'ab' and w.pending == b'')

    print("\n[G] 流式改写不能无限攒数据（延迟控制）")
    w = StreamRewriter(rs, direction="response")
    first = w.feed(b'x' * 10000)
    ok("大块无匹配数据会被及时吐出（只留 max_pattern-1 的尾巴）",
       len(first) == 10000 - (rs.max_response_pattern - 1), f"emitted={len(first)} hold={len(w.pending)}")
    ok("保留的尾巴长度 = 最长模式-1", len(w.pending) == rs.max_response_pattern - 1)
    ok("尾巴里若含半个模式仍会被正确处理",
       (w.feed(UP_KEY[:5].encode()) + w.feed(UP_KEY[5:].encode()) + w.flush()).count(LOCAL_KEY.encode()) == 1)

    print("\n[H] 二进制安全")
    blob = bytes(range(256)) * 4
    out, hits = rs.apply_request(blob)
    ok("纯二进制内容原样返回", out == blob and hits == [])
    mixed = b"\x00\xff" + LOCAL_KEY.encode() + b"\x80\x81"
    out, hits = rs.apply_request(mixed)
    ok("含非法 UTF-8 的字节流也能替换", out == b"\x00\xff" + UP_KEY.encode() + b"\x80\x81", repr(out))

    print("\n[I] 规则冲突：长模式优先，避免短模式先吃掉长模式")
    rs2 = RuleSet.from_list([
        {"src": "qwen", "dst": "Q"},                 # 短
        {"src": "qwen-max-long", "dst": "QML"},      # 长，且包含短模式
    ])
    out, _ = rs2.apply_request(b'{"m":"qwen-max-long"}')
    ok("长模式优先命中", out == b'{"m":"QML"}', repr(out))

    print(f"\n通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
    for f in FAIL:
        print("  -", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as exc:
        import traceback
        traceback.print_exc()
        print(f"\n测试无法运行（功能尚未实现）：{type(exc).__name__}: {exc}")
        sys.exit(2)
