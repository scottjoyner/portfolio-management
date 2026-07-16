# Alerts Service
# Build context: repo root (portfolio-management/)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies from the canonical requirements file
COPY deploy/requirements.txt /app/deploy/requirements.txt
RUN pip install --no-cache-dir -r /app/deploy/requirements.txt

# Copy real source trees that exist in the repo
COPY coinbase/ /app/coinbase/
COPY trading_system/ /app/trading_system/
COPY notification.py config.yaml /app/
COPY deploy/ /app/deploy/

RUN mkdir -p /app/logs /app/templates

# Non-root user for security
RUN useradd -u 1000 -m -s /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 9090

# Real HTTP healthcheck against the trader health server
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python3 -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:9090/ping'); sys.exit(0)" || exit 1

# Run the real Coinbase trader with an HTTP health port
CMD ["python3", "-u", "/app/coinbase/src/run_trader_v4.py", "--mode", "paper", "--health-port", "9090"]
