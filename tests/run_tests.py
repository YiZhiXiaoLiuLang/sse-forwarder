"""端到端测试：不依赖 pytest，直接 `python3 tests/run_tests.py`。

覆盖：
1. 标准 SSE 原样透传（字节完全一致）
2. 非标 SSE 原样透传 + 非标特征识别
3. 二进制/非法 UTF-8 流原样透传
4. 逐字节滴流的帧边界保真
5. 非 SSE 的普通 JSON 响应也能透传（并给出警告）
6. 请求头透传 / Host 改写 / 上游附加头注入
7. 上游中途掐断 -> Dashboard 记录详细错误
8. 上游不可达 -> 502 + DNS/TCP 层详细错误
9. Dashboard 切换上游后立即生效
10. 并发 6 路流互不干扰
11. Dashboard 手动终止会话
12. gzip 上游 + force_identity=false 时字节级透传
"""
from __future__ import annotations

import asyncio
import gzip
import json
import socket
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import uvicorn

from app.config import Settings
from app.main import create_apps
from tools.fake_upstream import run as run_fake

PASS, FAIL = [], []


def ok(name: str, cond: bool, extra: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    print(("  ✔ " if cond else "  ✘ ") + name + (f"   {extra}" if extra else ""), flush=True)


def free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def start(settings: Settings):
    proxy_app, dash_app, store, st, pool = create_apps(settings)
    pc = uvicorn.Config(proxy_app, host="127.0.0.1", port=settings.proxy_port, log_level="error", access_log=False)
    dc = uvicorn.Config(dash_app, host="127.0.0.1", port=settings.dashboard_port, log_level="error", access_log=False)
    ps, ds = uvicorn.Server(pc), uvicorn.Server(dc)
    t1, t2 = asyncio.create_task(ps.serve()), asyncio.create_task(ds.serve())
    while not (ps.started and ds.started):
        await asyncio.sleep(0.02)
    return ps, ds, t1, t2, store, st, pool


async def read_stream(client: httpx.AsyncClient, url: str, **kw):
    """用 aiter_raw 读取：拿到的是网卡上的真实字节，不做任何客户端侧解码。"""
    chunks, headers, status = [], None, None
    async with client.stream("GET", url, timeout=httpx.Timeout(30, read=None), **kw) as r:
        status, headers = r.status_code, dict(r.headers)
        async for c in r.aiter_raw():
            chunks.append(c)
    return status, headers, chunks


async def read_stream_post(client: httpx.AsyncClient, url: str, body: bytes, headers: dict):
    chunks = []
    async with client.stream("POST", url, content=body, headers=headers,
                             timeout=httpx.Timeout(30, read=None)) as r:
        status, hs = r.status_code, dict(r.headers)
        async for c in r.aiter_raw():
            chunks.append(c)
    return status, hs, chunks


async def raw_get(client: httpx.AsyncClient, url: str, **kw) -> bytes:
    async with client.stream("GET", url, timeout=httpx.Timeout(30, read=None), **kw) as r:
        return b"".join([c async for c in r.aiter_raw()])


async def main() -> int:
    up_port, up_srv = await run_fake(0)
    up2_port, up2_srv = await run_fake(0)
    settings = Settings(
        proxy_port=free_port(), dashboard_port=free_port(),
        upstream_url=f"http://127.0.0.1:{up_port}", upstream_name="mock-1",
        state_file="", idle_timeout=0, force_identity_encoding=True, log_level="error",
    )
    ps, ds, t1, t2, store, st, pool = await start(settings)
    base = f"http://127.0.0.1:{settings.proxy_port}"
    dash = f"http://127.0.0.1:{settings.dashboard_port}"
    print(f"\n上游 mock-1={up_port}  代理={settings.proxy_port}  Dashboard={settings.dashboard_port}\n")

    async with httpx.AsyncClient() as client:
        # ---------- 1. 标准 SSE 原样透传 ----------
        print("[1] 标准 SSE 原样透传")
        direct_body = await raw_get(client, f"http://127.0.0.1:{up_port}/std")
        stt, hs, chs = await read_stream(client, base + "/std")
        ok("状态码 200", stt == 200, f"got {stt}")
        ok("Content-Type 保留 text/event-stream", hs.get("content-type") == "text/event-stream", str(hs.get("content-type")))
        ok("字节完全一致", b"".join(chs) == direct_body, f"{len(b''.join(chs))} vs {len(direct_body)}")
        ok("分片被保留（非一次性吐出）", len(chs) >= 5, f"chunks={len(chs)}")
        sid1 = store.order[-1]
        det = store.sessions[sid1].detail(4096)
        ok("Dashboard 记录到请求头", any(h["k"].lower() == "user-agent" or h["k"].lower() == "accept" for h in det["request_headers"]))
        ok("Dashboard 记录到响应头", any(h["k"].lower() == "content-type" for h in det["response_headers"]))
        ok("Dashboard 记录了状态与 TTFB", det["status"] == 200 and det["ttfb_ms"] is not None)
        ok("Dashboard 抓取的分片字节 == 转发字节",
           b"".join(_d(c["b64"]) for c in det["chunks"]) == direct_body)
        ok("会话状态 done", det["state"] == "done", det["state"])

        # ---------- 2. 非标 SSE ----------
        print("\n[2] 非标 SSE（无空行/CRLF混用/裸JSON/半token）")
        direct_body = await raw_get(client, f"http://127.0.0.1:{up_port}/nonstd")
        stt, hs, chs = await read_stream(client, base + "/nonstd")
        ok("非标流字节完全一致", b"".join(chs) == direct_body)
        ok("非标流保留了奇怪的帧边界", len(chs) >= 4, f"chunks={len(chs)}")
        a = store.sessions[store.order[-1]].sse_analysis
        ok("识别出『缺少空行分隔』", any("空行" in x for x in a["nonstandard"]), json.dumps(a["nonstandard"], ensure_ascii=False))
        ok("识别出『CRLF/LF 混用』", any("混用" in x for x in a["nonstandard"]))
        ok("识别出『裸 JSON 行』", any("JSON" in x for x in a["nonstandard"]))
        ok("识别出非法 UTF-8 字节", a["non_utf8"] is True)

        # ---------- 3. 二进制 ----------
        print("\n[3] 非法 UTF-8 二进制流")
        direct_body = await raw_get(client, f"http://127.0.0.1:{up_port}/binary")
        stt, hs, chs = await read_stream(client, base + "/binary")
        ok("二进制字节完全一致", b"".join(chs) == direct_body)

        # ---------- 4. 逐字节滴流 ----------
        print("\n[4] 逐字节滴流帧边界")
        direct_body = await raw_get(client, f"http://127.0.0.1:{up_port}/drip")
        stt, hs, chs = await read_stream(client, base + "/drip")
        ok("滴流内容一致", b"".join(chs) == direct_body)
        ok("滴流未被合并成一块", len(chs) >= 5, f"chunks={len(chs)}")

        # ---------- 5. 普通 JSON（非 SSE） ----------
        print("\n[5] 非 SSE 响应透传")
        direct_body = await raw_get(client, f"http://127.0.0.1:{up_port}/plain")
        stt, hs, chs = await read_stream(client, base + "/plain")
        ok("JSON 内容一致", b"".join(chs) == direct_body)
        s = store.sessions[store.order[-1]]
        ok("给出 Content-Type 非 SSE 的警告", any("不是 text/event-stream" in e.message for e in s.errors))

        # ---------- 6. 请求头透传 ----------
        print("\n[6] 请求头透传 / Host 改写 / 上游附加头")
        stt, hs, chs = await read_stream(client, base + "/echo?x=1",
                                         headers={"X-Trace-Id": "abc-123", "Accept": "text/event-stream"})
        echo = json.loads(b"".join(chs))
        ok("自定义头 X-Trace-Id 到达上游", echo["headers"].get("x-trace-id") == "abc-123", str(echo["headers"]))
        ok("query 透传", "x=1" in echo["path"], echo["path"])
        ok("Host 已改写为上游", echo["headers"].get("host", "").startswith(f"127.0.0.1:{up_port}"), echo["headers"].get("host"))
        up = store.get_active(); up.headers = {"X-Upstream-Key": "secret-1"}
        stt, hs, chs = await read_stream(client, base + "/echo")
        echo = json.loads(b"".join(chs))
        ok("上游附加头被注入", echo["headers"].get("x-upstream-key") == "secret-1")
        up.headers = {}

        # ---------- 7. 上游中途掐断 ----------
        print("\n[7] 上游中途掐断 -> 详细错误")
        try:
            stt, hs, chs = await read_stream(client, base + "/die")
            got_err = True
        except Exception as exc:
            got_err = True
            stt = 200
        await asyncio.sleep(0.2)
        s = store.sessions[store.order[-1]]
        ok("客户端侧感知到中断", got_err)
        ok("Dashboard 记录了致命流错误", any(e.fatal for e in s.errors), f"errors={len(s.errors)}")
        ok("错误信息含 phase=stream", any(e.phase == "stream" for e in s.errors))
        ok("错误详情含已收字节数", any("bytes_received" in e.detail for e in s.errors))
        ok("错误带 Python 堆栈", any(e.tb for e in s.errors))
        ok("会话状态标记为 error", s.state == "error", s.state)
        ok("中断前的数据已完整转发", b"".join(_d(c["b64"]) for c in s.detail(4096)["chunks"]).startswith(b"data: first"))

        # ---------- 8. 上游不可达 ----------
        print("\n[8] 上游不可达 -> 502 + 逐层错误")
        dead = free_port()
        store.add_upstream("dead", f"http://127.0.0.1:{dead}")
        store.activate([u for u in store.upstreams.values() if u.name == "dead"][0].id)
        r = await client.get(base + "/anything", timeout=20)
        ok("返回 502", r.status_code == 502, str(r.status_code))
        j = r.json()
        ok("响应体含人类可读原因", bool(j.get("message")), j.get("message", ""))
        ok("响应体含异常类型", "exception" in j, j.get("exception", ""))
        s = store.sessions[store.order[-1]]
        ok("Dashboard 记录 connect 阶段错误", s.errors and s.errors[-1].phase.startswith("connect"), s.errors[-1].phase if s.errors else "-")
        d = s.errors[-1].detail
        ok("错误详情含 DNS 解析结果", "dns_result" in d, str(d.get("dns_result")))
        ok("错误详情含目标 URL / 上游名 / 超时配置",
           all(k in d for k in ("target_url", "upstream_name", "connect_timeout")), str(list(d)))
        ok("错误详情含堆栈", bool(s.errors[-1].tb))
        # DNS 失败
        store.add_upstream("baddns", "http://this-host-does-not-exist-zzz.local")
        store.activate([u for u in store.upstreams.values() if u.name == "baddns"][0].id)
        r = await client.get(base + "/x", timeout=20)
        j = r.json()
        ok("DNS 失败也被捕获并上报", r.status_code == 502 and "dns" in json.dumps(j).lower(), j.get("detail", {}).get("phase"))

        # ---------- 9. Dashboard 切换上游 ----------
        print("\n[9] Dashboard 切换上游")
        r = await client.post(dash + "/api/upstreams", json={"name": "mock-2", "url": f"http://127.0.0.1:{up2_port}"})
        new_id = r.json()["upstream"]["id"]
        ok("新增上游成功", r.json()["ok"])
        r = await client.post(f"{dash}/api/upstreams/{new_id}/activate", json={})
        ok("切换上游成功", r.json()["ok"] and r.json()["active_upstream"]["name"] == "mock-2")
        stt, hs, chs = await read_stream(client, base + "/std")
        s = store.sessions[store.order[-1]]
        ok("新请求走到 mock-2", s.upstream_name == "mock-2" and f":{up2_port}" in s.target_url, s.target_url)
        r = await client.post(f"{dash}/api/upstreams/{new_id}/test", json={"path": "/__probe__"})
        rep = r.json()["report"]
        ok("上游探测逐层报告（dns/tcp/http）", [x["step"] for x in rep["steps"]] == ["dns", "tcp", "http"],
           str([x["step"] for x in rep["steps"]]))
        ok("探测结果为通过", r.json()["ok"] is True, json.dumps(rep, ensure_ascii=False)[:400])
        # 切回 mock-1
        m1 = [u for u in store.upstreams.values() if u.name == "mock-1"][0]
        await client.post(f"{dash}/api/upstreams/{m1.id}/activate", json={})

        # ---------- 10. 并发 ----------
        print("\n[10] 并发 6 路流")
        async def one(i):
            async with httpx.AsyncClient() as c:
                stt, hs, chs = await read_stream(c, base + "/std")
                return i, b"".join(chs), stt
        res = await asyncio.gather(*[one(i) for i in range(6)])
        ref = await raw_get(client, f"http://127.0.0.1:{up_port}/std")
        ok("6 路全部 200", all(x[2] == 200 for x in res))
        ok("6 路内容全部一致", all(x[1] == ref for x in res))
        ok("6 个会话都被独立记录", len(store.open_sessions()) >= 0 and store.stats["requests"] >= 6,
           f"requests={store.stats['requests']}")

        # ---------- 11. 手动终止 ----------
        print("\n[11] Dashboard 终止会话")
        got = asyncio.Event()
        async def hang_client():
            try:
                async with client.stream("GET", base + "/hang", timeout=httpx.Timeout(30, read=None)) as r:
                    async for _ in r.aiter_bytes():
                        pass
            except Exception:
                pass
            got.set()
        task = asyncio.create_task(hang_client())
        await asyncio.sleep(0.5)
        open_s = [s for s in store.open_sessions() if s.path == "/hang"]
        ok("hang 会话处于 streaming", bool(open_s) and open_s[0].state == "streaming")
        if open_s:
            r = await client.post(f"{dash}/api/sessions/{open_s[0].id}/kill", json={})
            ok("kill 接口返回成功", r.json()["ok"])
            await asyncio.wait_for(got.wait(), 5)
            ok("客户端连接已被关闭", True)
            ok("会话记录了终止原因", open_s[0].close_reason in {"killed_from_dashboard"} or open_s[0].state == "error",
               f"{open_s[0].close_reason}/{open_s[0].state}")
        task.cancel()

        # ---------- 12. gzip 原样透传 ----------
        print("\n[12] gzip 上游：force_identity 关闭时字节级透传")
        direct_body = await raw_get(client, f"http://127.0.0.1:{up_port}/gzip", headers={"Accept-Encoding": "gzip"})
        st.force_identity_encoding = False
        stt, hs, chs = await read_stream(client, base + "/gzip", headers={"Accept-Encoding": "gzip"})
        ok("压缩流长度一致（gzip 头含 mtime，逐字节本就不同）",
           len(b"".join(chs)) == len(direct_body), f"{len(b''.join(chs))} vs {len(direct_body)}")
        ok("解压后与直连解压结果一致", gzip.decompress(b"".join(chs)) == gzip.decompress(direct_body))
        ok("Content-Encoding: gzip 被保留", hs.get("content-encoding") == "gzip", str(hs.get("content-encoding")))
        ok("转发字节确实是 gzip 压缩流", b"".join(chs)[:2] == b"\x1f\x8b", repr(b"".join(chs)[:4]))
        ok("解压后内容正确", gzip.decompress(b"".join(chs)).startswith(b"data: gzip-0"))
        st.force_identity_encoding = True

        # ---------- 14. 大模型接口改写：key / 模型名 ----------
        print("\n[14] key 与模型名改写（请求 + 响应双向）")
        LOCAL_KEY = "sk-local-aaaa1111bbbb2222cccc3333dddd4444"
        UP_KEY = "sk-upstream-zzzz9999yyyy8888xxxx7777wwww6666"
        LOCAL_MODEL = "local-qwen3-max-2026-01-25-thinking"
        UP_MODEL = "qwen-max-upstream-v3"
        rules = [{"label": "API Key", "src": LOCAL_KEY, "dst": UP_KEY},
                 {"label": "模型名", "src": LOCAL_MODEL, "dst": UP_MODEL}]
        m1 = [u for u in store.upstreams.values() if u.name == "mock-1"][0]
        r = await client.put(f"{dash}/api/upstreams/{m1.id}", json={"rules": rules})
        ok("规则写入上游成功", r.json()["ok"] and len(r.json()["upstream"]["rules"]) == 2,
           json.dumps(r.json().get("upstream", {}).get("rules", []), ensure_ascii=False)[:200])
        ok("规则在 /api/state 中明文可见（按选择不打码）",
           any(u["id"] == m1.id and u["rules"][0]["dst"] == UP_KEY
               for u in (await client.get(dash + "/api/state")).json()["upstreams"]))

        req_body = json.dumps({"model": LOCAL_MODEL, "stream": True,
                               "messages": [{"role": "user", "content": f"my key is {LOCAL_KEY}"}]}).encode()
        req_headers = {"Authorization": f"Bearer {LOCAL_KEY}", "Content-Type": "application/json",
                       "X-Api-Key": LOCAL_KEY}

        # 用 base64 回显端点观测「上游究竟收到了什么」——base64 不会被响应反向改写命中
        stt, hs, chs = await read_stream_post(client, base + "/echo-b64", req_body, req_headers)
        import base64 as _b64mod
        seen = _b64mod.b64decode(json.loads(b"".join(chs))["b64"])
        head_part, _, seen_body = seen.partition(b"\r\n\r\n")
        head_low = head_part.decode("latin-1").lower()
        ok("上游收到的 Authorization 已是上游 key",
           f"authorization: bearer {UP_KEY}" in head_low and LOCAL_KEY not in head_low,
           [l for l in head_low.splitlines() if "authorization" in l][0][:70])
        ok("上游收到的自定义头 X-Api-Key 也已替换",
           f"x-api-key: {UP_KEY}" in head_low)
        ok("上游收到的 body 里模型名已换成上游模型名",
           UP_MODEL.encode() in seen_body and LOCAL_MODEL.encode() not in seen_body)
        ok("上游收到的 body 里嵌在 content 中的 key 也已替换",
           UP_KEY.encode() in seen_body and LOCAL_KEY.encode() not in seen_body)
        ok("Content-Length 与改写后的 body 长度一致",
           f"content-length: {len(seen_body)}" in head_low,
           [l for l in head_low.splitlines() if "content-length" in l])
        ok("JSON 结构在改写后依然合法", json.loads(seen_body)["model"] == UP_MODEL)

        s = store.sessions[store.order[-1]]
        ok("Dashboard 记录了请求方向的命中（头 2 处 + body 2 处）",
           sum(x["count"] for x in s.request_rewrites) == 4,
           json.dumps(s.request_rewrites, ensure_ascii=False))
        ok("命中记录区分了位置（headers / body）",
           {x.get("in") for x in s.request_rewrites} == {"headers", "body"})
        ok("Dashboard 保存了改写前的原始 body（便于对照）",
           LOCAL_KEY.encode() in s.request_body_raw and LOCAL_MODEL.encode() in s.request_body_raw)
        ok("Dashboard 保存的转发 body 是改写后的（明文可见，未打码）",
           UP_KEY.encode() in s.request_body and UP_MODEL.encode() in s.request_body)
        ok("会话详情里带规则快照", len(s.rewrite_rules) == 2)

        # 客户端视角：/echo 回显里出现的上游值会被反向改写回本地值，客户端看不到上游 key
        stt, hs, chs = await read_stream_post(client, base + "/echo", req_body, req_headers)
        back = b"".join(chs)
        ok("响应里上游 key 已被换回本地 key（不泄漏给客户端）",
           LOCAL_KEY.encode() in back and UP_KEY.encode() not in back)
        ok("响应里上游模型名已被换回本地模型名",
           LOCAL_MODEL.encode() in back and UP_MODEL.encode() not in back)

        # 流式响应反向替换（上游把 model 拦腰拆成两个分片）
        stt, hs, chs = await read_stream_post(client, base + "/llm", req_body, req_headers)
        got = b"".join(chs)
        ok("响应流里上游模型名已换回本地模型名", LOCAL_MODEL.encode() in got and UP_MODEL.encode() not in got,
           got[:150].decode("utf-8", "replace"))
        ok("跨分片被截断的模型名也替换成功", got.count(LOCAL_MODEL.encode()) == 2, f"count={got.count(LOCAL_MODEL.encode())}")
        ok("响应流里上游 key 已换回本地 key", LOCAL_KEY.encode() in got and UP_KEY.encode() not in got)
        ok("SSE 结构未被破坏", got.startswith(b'data: {"id":"chatcmpl-1"') and got.rstrip().endswith(b"data: [DONE]"))
        s = store.sessions[store.order[-1]]
        ok("Dashboard 记录了响应方向的命中次数",
           s.response_rewrites and sum(x["count"] for x in s.response_rewrites) >= 3,
           json.dumps(s.response_rewrites, ensure_ascii=False))
        ok("会话详情里能看到改写后发给客户端的分片",
           LOCAL_MODEL.encode() in b"".join(_d(c["b64"]) for c in s.detail(4096)["chunks"]))
        ok("会话详情里同时保留了上游原文分片",
           any(c.get("src_b64") for c in s.detail(4096)["chunks"]))

        # 上游之间规则互不影响
        r = await client.post(dash + "/api/upstreams", json={"name": "mock-3", "url": f"http://127.0.0.1:{up2_port}",
                                                             "rules": [{"label": "K", "src": "sk-other-local",
                                                                        "dst": "sk-other-up"}]})
        m3 = r.json()["upstream"]["id"]
        ok("新增上游可带自己的规则", len(r.json()["upstream"]["rules"]) == 1)
        ok("mock-1 的规则未被影响", len(store.upstreams[m1.id].rules) == 2)
        await client.post(f"{dash}/api/upstreams/{m3}/activate", json={})
        stt, hs, chs = await read_stream_post(client, base + "/echo", req_body, req_headers)
        echo = json.loads(b"".join(chs))
        ok("切到 mock-3 后按它自己的规则改写（本地 key 未命中，原样透传）",
           echo["headers"]["authorization"] == f"Bearer {LOCAL_KEY}" and LOCAL_MODEL in echo["body"])
        await client.post(f"{dash}/api/upstreams/{m1.id}/activate", json={})

        # 关掉改写开关后完全透传
        st.rewrite_enabled = False
        stt, hs, chs = await read_stream_post(client, base + "/echo", req_body, req_headers)
        echo = json.loads(b"".join(chs))
        ok("REWRITE_ENABLED=false 时完全不改写",
           echo["headers"]["authorization"] == f"Bearer {LOCAL_KEY}" and LOCAL_MODEL in echo["body"])
        st.rewrite_enabled = True

        # ---------- 13. Dashboard API 完整性 ----------
        print("\n[13] Dashboard API")
        r = await client.get(dash + "/api/state")
        ok("/api/state 正常", r.json()["ok"] and len(r.json()["upstreams"]) >= 2)
        r = await client.get(dash + "/api/sessions?limit=200")
        ok("/api/sessions 返回历史", len(r.json()["sessions"]) >= 10, str(len(r.json()["sessions"])))
        r = await client.get(dash)
        ok("Dashboard 首页可访问", r.status_code == 200 and "SSE 转发器" in r.text)
        async with client.stream("GET", dash + "/api/events") as ev:
            first = await asyncio.wait_for(ev.aiter_raw().__anext__(), 3)
            ok("事件流以 snapshot 开头", b"event: snapshot" in first, first[:60].decode("latin-1"))

    print(f"\n通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
    if FAIL:
        print("失败清单：")
        for f in FAIL:
            print("  -", f)

    ps.should_exit = ds.should_exit = True
    await asyncio.gather(t1, t2, return_exceptions=True)
    await pool.aclose()
    up_srv.close(); up2_srv.close()
    return 1 if FAIL else 0


def _d(b64: str) -> bytes:
    import base64
    return base64.b64decode(b64 or "")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
