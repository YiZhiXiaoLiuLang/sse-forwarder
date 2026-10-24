IMAGE ?= sse-forwarder:latest
PORT_PROXY ?= 8788
PORT_DASH ?= 8787

.PHONY: test run build up down logs mock clean
test:            ## 端到端自测（60 项断言，不需要 Docker）
	python3 tests/run_tests.py

run:             ## 本机直接跑（需要已安装 requirements.txt）
	PROXY_PORT=$(PORT_PROXY) DASHBOARD_PORT=$(PORT_DASH) STATE_FILE=./data/state.json \
	UPSTREAM_URL=$${UPSTREAM_URL:-http://127.0.0.1:9000} python3 -m app.main

mock:            ## 起一个非标 SSE 假上游
	python3 -m tools.fake_upstream 9000

build:           ## 构建镜像
	docker build -t $(IMAGE) .

up:              ## compose 起转发器 + mock 上游
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f forwarder

clean:
	rm -rf ./data __pycache__ app/__pycache__ tools/__pycache__ tests/__pycache__
