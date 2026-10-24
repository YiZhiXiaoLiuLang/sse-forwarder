"""运行配置：全部通过环境变量覆盖，容器内无需改代码。"""
from __future__ import annotations

import os
from dataclasses import dataclass, asdict


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    try:
        return int(raw) if raw not in (None, "") else default
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    try:
        return float(raw) if raw not in (None, "") else default
    except ValueError:
        return default


@dataclass
class Settings:
    # ---- 端口 ----
    proxy_host: str = "0.0.0.0"
    proxy_port: int = 8788
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8787

    # ---- 初始上游（可在 Dashboard 随时增删切换）----
    upstream_url: str = "http://127.0.0.1:9000"
    upstream_name: str = "default"

    # ---- 超时（秒）。read_timeout=0 表示不超时，SSE 长连接必需 ----
    connect_timeout: float = 10.0
    read_timeout: float = 0.0
    write_timeout: float = 30.0
    pool_timeout: float = 10.0
    # 空闲多久算上游卡死（0=关闭）。开启后能主动发现"连接还在但不再吐数据"的上游
    idle_timeout: float = 0.0

    # ---- 转发行为 ----
    # 强制 Accept-Encoding: identity，这样抓到的是明文、可直接在 Dashboard 阅读。
    # 关掉则是 100% 字节级原样透传（可能是 gzip 压缩流）。
    force_identity_encoding: bool = True
    http2: bool = False
    follow_redirects: bool = False
    verify_tls: bool = True
    # 转发时保留客户端 Host 头（默认改写为上游 Host）
    preserve_client_host: bool = False
    # 允许转发的方法
    allowed_methods: str = "GET,POST,PUT,PATCH,DELETE,HEAD,OPTIONS"

    # ---- 大模型接口改写（本地 key/模型名 <-> 上游 key/模型名）----
    rewrite_enabled: bool = True
    # 有响应改写规则时，强制上游返回明文（Accept-Encoding: identity），
    # 否则压缩流没法做字面替换
    rewrite_force_identity: bool = True
    # 请求体超过此大小则跳过 body 改写（头照常改写），避免大文件被整体读入
    rewrite_max_body: int = 8 * 1024 * 1024
    # 初始上游的规则，可用环境变量直接种下去
    local_api_key: str = ""
    upstream_api_key: str = ""
    local_model: str = ""
    upstream_model: str = ""

    # ---- 抓取上限（防止长跑把内存吃光）----
    max_sessions: int = 300              # 保留的会话历史上限
    max_chunks_per_session: int = 5000   # 每会话保留的分片数
    max_capture_bytes: int = 8 * 1024 * 1024   # 每会话累计抓取字节上限
    max_chunk_capture: int = 256 * 1024  # 单个分片抓取上限
    max_request_body_capture: int = 1 * 1024 * 1024
    event_buffer: int = 400              # Dashboard 重连时可回放的事件数
    max_event_payload: int = 16 * 1024   # 单个推送事件里的分片载荷上限

    # ---- 持久化 / 鉴权 / 日志 ----
    state_file: str = "/data/state.json"
    dashboard_token: str = ""
    log_level: str = "info"

    @staticmethod
    def from_env() -> "Settings":
        s = Settings()
        for key in asdict(s):
            env = key.upper()
            if env not in os.environ:
                continue
            cur = getattr(s, key)
            if isinstance(cur, bool):
                setattr(s, key, _bool(env, cur))
            elif isinstance(cur, int):
                setattr(s, key, _int(env, cur))
            elif isinstance(cur, float):
                setattr(s, key, _float(env, cur))
            else:
                setattr(s, key, os.environ[env])
        return s

    def seed_rules(self) -> list[dict]:
        """由环境变量生成初始上游的改写规则。"""
        out = []
        if self.local_api_key and self.upstream_api_key and self.local_api_key != self.upstream_api_key:
            out.append({"label": "API Key", "src": self.local_api_key,
                        "dst": self.upstream_api_key, "scope": "both", "enabled": True})
        if self.local_model and self.upstream_model and self.local_model != self.upstream_model:
            out.append({"label": "模型名", "src": self.local_model,
                        "dst": self.upstream_model, "scope": "both", "enabled": True})
        return out

    @property
    def allowed_method_set(self) -> set[str]:
        return {m.strip().upper() for m in self.allowed_methods.split(",") if m.strip()}

    def read_timeout_or_none(self) -> float | None:
        return None if self.read_timeout <= 0 else self.read_timeout

    def idle_timeout_or_none(self) -> float | None:
        return None if self.idle_timeout <= 0 else self.idle_timeout
