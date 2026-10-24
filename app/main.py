"""入口：一个进程内同时跑「代理端口」和「Dashboard 端口」。"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

import uvicorn

from .config import Settings
from .dashboard_api import make_dashboard_app
from .proxy import ClientPool, make_proxy_app
from .store import Store


def build(settings: Settings | None = None) -> tuple[Store, Settings, ClientPool]:
    settings = settings or Settings.from_env()
    store = Store(settings)
    store.load()
    if not store.upstreams:
        url = (settings.upstream_url or "").strip()
        if url:
            store.add_upstream(name=settings.upstream_name or "default", url=url,
                               note="来自环境变量 UPSTREAM_URL", rules=settings.seed_rules())
            store.save()
    pool = ClientPool(settings)
    return store, settings, pool


def create_apps(settings: Settings | None = None):
    store, settings, pool = build(settings)
    proxy_app = make_proxy_app(store, settings, pool)
    dash_app = make_dashboard_app(store, settings, pool)
    return proxy_app, dash_app, store, settings, pool


async def serve(settings: Settings | None = None) -> None:
    proxy_app, dash_app, store, settings, pool = create_apps(settings)

    level = settings.log_level.lower()
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    proxy_cfg = uvicorn.Config(proxy_app, host=settings.proxy_host, port=settings.proxy_port,
                               log_level=level, access_log=False, server_header=False,
                               timeout_graceful_shutdown=5, h11_max_incomplete_event_size=64 * 1024)
    dash_cfg = uvicorn.Config(dash_app, host=settings.dashboard_host, port=settings.dashboard_port,
                              log_level=level, access_log=False, server_header=False,
                              timeout_graceful_shutdown=5)
    proxy_srv = uvicorn.Server(proxy_cfg)
    dash_srv = uvicorn.Server(dash_cfg)

    active = store.get_active()
    print("=" * 78, flush=True)
    print(" SSE 原始流转发器已启动", flush=True)
    print(f"   代理端口    : http://{settings.proxy_host}:{settings.proxy_port}  (把请求打到这里，会原样转发到上游)", flush=True)
    print(f"   Dashboard   : http://{settings.dashboard_host}:{settings.dashboard_port}", flush=True)
    print(f"   当前上游    : {active.name + ' → ' + active.url if active else '(未配置，请在 Dashboard 添加)'}", flush=True)
    print(f"   明文抓取    : force_identity_encoding={settings.force_identity_encoding}"
          f" (false 时 100% 字节透传，可能是压缩流)", flush=True)
    rules = active.rules if active else []
    print(f"   改写规则    : {'关闭' if not settings.rewrite_enabled else (str(len(rules)) + ' 条（跟随上游）' if rules else '当前上游未配置')}", flush=True)
    for r in rules:
        print(f"     - {r.get('label') or '(未命名)'}: {r['src']} -> {r['dst']}  [{r.get('scope', 'both')}]", flush=True)
    print(f"   读超时      : {settings.read_timeout or '不限（SSE 长连接）'}   空闲超时: {settings.idle_timeout or '关闭'}", flush=True)
    print("=" * 78, flush=True)

    stop = asyncio.Event()

    def _sig(*_):
        stop.set()
        proxy_srv.should_exit = True
        dash_srv.should_exit = True

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _sig)
        except NotImplementedError:
            pass

    try:
        await asyncio.gather(proxy_srv.serve(), dash_srv.serve(), stop.wait())
    finally:
        await pool.aclose()
        store.save()


def main() -> None:
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
