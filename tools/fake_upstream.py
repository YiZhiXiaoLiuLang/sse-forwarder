"""裸 socket 实现的假上游，用于精确控制返回字节（含各种非标 SSE 与错误场景）。

启动：python3 -m tools.fake_upstream [port]
路由：
  /std            标准 SSE，5 个事件
  /nonstd         非标 SSE：无空行分隔、CRLF/LF 混用、裸 JSON 行、事件被拆成半个 token
  /binary         含非法 UTF-8 字节
  /drip           一次一个字节慢慢吐（考验分片保真）
  /plain          普通 JSON 响应（非 SSE，带 Content-Length）
  /gzip           gzip 压缩的 SSE（Content-Encoding: gzip）
  /500            500 错误
  /404            404
  /die            发 2 个事件后直接掐断连接（不发结束块）
  /hang           发 1 个事件后永久静默
  /echo           回显收到的请求头与请求体（JSON，一次性）
  /echo-b64       把「请求行 + 全部请求头 + body」整体 base64 回显。
                  因为返回的是 base64，转发器的响应反向改写不会命中，
                  可用来精确验证上游究竟收到了什么字节
  /llm            模拟大模型流式接口：把收到的 model 与 Authorization 以 SSE 吐回，
                  并故意把 model 字符串拦腰拆成两个 TCP 分片（考验跨分片改写）
"""
from __future__ import annotations

import asyncio
import gzip
import json
import sys


async def read_request(reader: asyncio.StreamReader) -> tuple[str, str, dict[str, str], bytes]:
    line = await reader.readline()
    if not line:
        return "", "", {}, b""
    parts = line.decode("latin-1").split()
    method, path = (parts + ["", ""])[:2]
    headers: dict[str, str] = {}
    while True:
        h = await reader.readline()
        if h in (b"\r\n", b"\n", b""):
            break
        k, _, v = h.decode("latin-1").partition(":")
        headers[k.strip().lower()] = v.strip()
    body = b""
    n = int(headers.get("content-length") or 0)
    if n:
        body = await reader.readexactly(n)
    return method, path, headers, body


def head(status: int, reason: str, headers: list[tuple[str, str]]) -> bytes:
    out = f"HTTP/1.1 {status} {reason}\r\n".encode()
    for k, v in headers:
        out += f"{k}: {v}\r\n".encode()
    return out + b"\r\n"


def chunk(data: bytes) -> bytes:
    return f"{len(data):X}\r\n".encode() + data + b"\r\n"


async def send_chunks(writer: asyncio.StreamWriter, pieces: list[bytes], delay: float = 0.0) -> None:
    for p in pieces:
        writer.write(chunk(p))
        await writer.drain()
        if delay:
            await asyncio.sleep(delay)


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        method, path, headers, body = await read_request(reader)
        p = path.split("?")[0]

        if p == "/__probe__":
            writer.write(head(200, "OK", [("Content-Type", "text/plain"), ("Content-Length", "2")]) + b"ok")
            await writer.drain(); return

        if p == "/std":
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Cache-Control", "no-cache"), ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            pieces = [b": connected\n\n"]
            for i in range(1, 6):
                pieces.append(f"event: tick\nid: {i}\ndata: {{\"i\": {i}, \"msg\": \"标准事件\"}}\n\n".encode())
            pieces.append(b"data: [DONE]\n\n")
            await send_chunks(writer, pieces, 0.03)
            writer.write(b"0\r\n\r\n"); await writer.drain(); return

        if p == "/nonstd":
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            # 故意非标：没有空行分隔、CRLF 与 LF 混用、裸 JSON 行、事件被拦腰截断
            await send_chunks(writer, [
                b"data:{\"seq\":1}\r\n",
                b"data:{\"seq\":2}",          # 半个 token
                b"\n{\"raw\":\"no-prefix\"}\r\n",
                b"event:ping\r\ndata:pong\n",
                b"\xff\xfe broken-bytes",   # 混入真正的非法 UTF-8 字节
            ], 0.02)
            writer.write(b"0\r\n\r\n"); await writer.drain(); return

        if p == "/binary":
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            await send_chunks(writer, [b"data:\xff\xfe\x00\x01raw-binary\n\n", b"data:end\n\n"], 0.02)
            writer.write(b"0\r\n\r\n"); await writer.drain(); return

        if p == "/drip":
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            for b in b"data:drip-test\n\n":
                writer.write(chunk(bytes([b])))
                await writer.drain()
                await asyncio.sleep(0.005)
            writer.write(b"0\r\n\r\n"); await writer.drain(); return

        if p == "/plain":
            payload = json.dumps({"hello": "world", "sse": False}).encode()
            writer.write(head(200, "OK", [("Content-Type", "application/json"),
                                          ("Content-Length", str(len(payload)))]))
            writer.write(payload); await writer.drain(); return

        if p == "/gzip":
            raw = b"".join(f"data: gzip-{i}\n\n".encode() for i in range(3))
            comp = gzip.compress(raw)
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Content-Encoding", "gzip"),
                                          ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            await send_chunks(writer, [comp[: len(comp) // 2], comp[len(comp) // 2:]])
            writer.write(b"0\r\n\r\n"); await writer.drain(); return

        if p in ("/500", "/404"):
            code = 500 if p == "/500" else 404
            payload = json.dumps({"error": "boom", "path": p}).encode()
            writer.write(head(code, "Internal Server Error" if code == 500 else "Not Found",
                              [("Content-Type", "application/json"), ("Content-Length", str(len(payload)))]))
            writer.write(payload); await writer.drain(); return

        if p == "/die":
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            await send_chunks(writer, [b"data: first\n\n", b"data: second\n\n"], 0.02)
            # 不发结束块，直接关闭底层连接 -> 客户端侧看到协议错误/连接重置
            writer.transport.abort()
            return

        if p == "/hang":
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            await send_chunks(writer, [b"data: only-one\n\n"])
            await asyncio.sleep(120)   # 永久静默
            return

        if p == "/llm":
            import re as _re
            model = ""
            m = _re.search(rb'"model"\s*:\s*"([^"]*)"', body)
            if m:
                model = m.group(1).decode("utf-8", "replace")
            auth = headers.get("authorization", "")
            writer.write(head(200, "OK", [("Content-Type", "text/event-stream"),
                                          ("Cache-Control", "no-cache"),
                                          ("Transfer-Encoding", "chunked")]))
            await writer.drain()
            first = (f'data: {{"id":"chatcmpl-1","object":"chat.completion.chunk",'
                     f'"model":"').encode()
            await send_chunks(writer, [first], 0.01)
            # 把 model 名拦腰截断，模拟真实网关的任意分片
            mid = len(model) // 2
            await send_chunks(writer, [model[:mid].encode()], 0.01)
            await send_chunks(writer, [model[mid:].encode()], 0.01)
            await send_chunks(writer, [
                f'","choices":[{{"delta":{{"content":"auth={auth}"}}}}]}}\n\n'.encode(),
                f'data: {{"model":"{model}","choices":[{{"delta":{{"content":"hello"}}}}]}}\n\n'.encode(),
                b"data: [DONE]\n\n",
            ], 0.01)
            writer.write(b"0\r\n\r\n"); await writer.drain(); return

        if p == "/echo-b64":
            import base64 as _b64
            raw = f"{method} {path} HTTP/1.1\r\n".encode()
            for k, v in headers.items():
                raw += f"{k}: {v}\r\n".encode()
            raw += b"\r\n" + body
            payload = json.dumps({"b64": _b64.b64encode(raw).decode("ascii"),
                                  "size": len(raw)}).encode()
            writer.write(head(200, "OK", [("Content-Type", "application/json"),
                                          ("Content-Length", str(len(payload)))]))
            writer.write(payload); await writer.drain(); return

        if p == "/echo":
            payload = json.dumps({"method": method, "path": path, "headers": headers,
                                  "body": body.decode("utf-8", "replace")}, ensure_ascii=False).encode()
            writer.write(head(200, "OK", [("Content-Type", "application/json"),
                                          ("Content-Length", str(len(payload)))]))
            writer.write(payload); await writer.drain(); return

        payload = b'{"error":"unknown path"}'
        writer.write(head(404, "Not Found", [("Content-Type", "application/json"),
                                             ("Content-Length", str(len(payload)))]))
        writer.write(payload); await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as exc:
        print("fake_upstream error:", type(exc).__name__, exc, flush=True)
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def run(port: int = 0) -> tuple[int, asyncio.Server]:
    srv = await asyncio.start_server(handle, "127.0.0.1", port)
    real = srv.sockets[0].getsockname()[1]
    return real, srv


if __name__ == "__main__":
    async def _main():
        port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
        real, srv = await run(port)
        print(f"fake upstream listening on 127.0.0.1:{real}", flush=True)
        async with srv:
            await srv.serve_forever()
    asyncio.run(_main())
