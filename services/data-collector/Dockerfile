# Data Collector Service
# Build context: repository root (docker build -f services/data-collector/Dockerfile .)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install dependencies (canonical prod deps)
COPY deploy/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy real application source
COPY coinbase/ /app/coinbase/
COPY trading_system/ /app/trading_system/

# Create non-root user (security)
RUN useradd --create-home --uid 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# Real live entrypoint (EventTraderV4) with health server on a dedicated port
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start_period=30s \
    CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/status', timeout=3).status == 200 else 1)"

CMD ["python3", "-u", "/app/coinbase/src/run_trader_v4.py", "--health-port", "8080"]
