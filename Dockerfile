# ---------- 构建层 ----------
FROM python:3.12-slim AS builder
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# ---------- 运行层 ----------
FROM python:3.12-slim
LABEL org.opencontainers.image.title="sse-forwarder" \
      org.opencontainers.image.description="原样转发上游 SSE/任意 HTTP 流的调试代理，带实时 Dashboard" \
      org.opencontainers.image.source="https://github.com/YiZhiXiaoLiuLang/sse-forwarder"
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PROXY_HOST=0.0.0.0 \
    PROXY_PORT=6778 \
    DASHBOARD_HOST=0.0.0.0 \
    DASHBOARD_PORT=6779 \
    STATE_FILE=/data/state.json \
    UPSTREAM_URL=http://127.0.0.1:9000

WORKDIR /app
COPY --from=builder /install /usr/local
COPY app ./app
COPY tools ./tools
COPY README.md ./

RUN useradd --uid 10001 --create-home ssefw \
    && mkdir -p /data \
    && chown -R ssefw:ssefw /data /app
USER ssefw
VOLUME ["/data"]

# 6778 = 代理端口（客户端往这里打）；6779 = Dashboard
EXPOSE 6778 6779

HEALTHCHECK --interval=15s --timeout=4s --start-period=6s --retries=3 \
  CMD python -c "import os,urllib.request,sys; \
p=os.environ.get('DASHBOARD_PORT','6779'); \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:%s/api/health'%p,timeout=3).status==200 else 1)"

ENTRYPOINT ["python", "-m", "app.main"]
