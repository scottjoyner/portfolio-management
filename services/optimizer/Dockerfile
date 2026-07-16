# Optimizer service — rebuilt from current code.
# Needs the `coinbase` CLI (npm @coinbase/coinbase-cli, Node 22+) for CLI verification.
FROM node:22-slim AS cli
RUN npm install -g @coinbase/coinbase-cli --silent 2>/dev/null || true

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Bring the coinbase CLI (built in the node stage) + node runtime into this image
COPY --from=cli /usr/local/bin/node /usr/local/bin/node
COPY --from=cli /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -sf /usr/local/lib/node_modules/@coinbase/coinbase-cli/dist/index.js /usr/local/bin/coinbase 2>/dev/null || true

WORKDIR /app

COPY deploy/ /app/deploy/
COPY deploy/requirements.venv-lock.txt /app/requirements.lock.txt
RUN pip install --no-cache-dir -r /app/requirements.lock.txt

COPY trading_system/ /app/trading_system/
COPY coinbase/ /app/coinbase/
COPY config.yaml /app/
COPY strategy_engine.py confidence_matrix.py state_store.py market_universe.py neo4j_store.py notification.py portfolio_optimizer.py approval_server.py /app/
COPY archive/root/alerts.py /app/alerts.py
COPY archive/root/data_collector.py /app/data_collector.py
COPY rust_core/ /app/rust_core/

ENV PYTHONPATH=/app:/app/trading_system:/app/coinbase \
    PATH=/usr/local/lib/node_modules/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    COINBASE_CLI_PATH=coinbase

RUN mkdir -p /app/logs /app/data /app/state /app/templates

RUN useradd -u 1000 -m -s /bin/bash appuser && \
    chown -R appuser:appuser /app

USER appuser

WORKDIR /app
CMD ["python3", "-u", "portfolio_optimizer.py", "--interval", "300", "--db", "/app/state/optimizer_state.db", "--pending-file", "/app/data/pending_approvals.json", "--approval-base-url", "http://approval-server:8080"]
