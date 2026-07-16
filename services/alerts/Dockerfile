# alerts service — rebuilt from current code (shared Python runtime).
# Build context: repo root (portfolio-management/)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     PIP_NO_CACHE_DIR=1

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

ENV PYTHONPATH=/app:/app/trading_system:/app/coinbase

RUN mkdir -p /app/logs /app/data /app/state /app/templates

# Non-root user for security
RUN useradd -u 1000 -m -s /bin/bash appuser &&     chown -R appuser:appuser /app

USER appuser

WORKDIR /app
CMD ["python3", "-u", "alerts.py"]
