# Backtester Service
# Build context: repo root (portfolio-management/)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY deploy/requirements.txt /app/deploy/requirements.txt
RUN pip install --no-cache-dir -r /app/deploy/requirements.txt

# Copy real source trees that exist in the repo
COPY coinbase/ /app/coinbase/
COPY trading_system/ /app/trading_system/
COPY scripts/ /app/scripts/
COPY config.yaml /app/

RUN mkdir -p /app/logs /app/data

RUN useradd -u 1000 -m -s /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 3002

# The backtester is a batch process; verify the module imports cleanly
HEALTHCHECK --interval=60s --timeout=30s --retries=3 \
    CMD python3 -c "import sys; sys.path.insert(0, '/app/trading_system'); import backtester; print('OK')" || exit 1

# Run the real backtester entrypoint
CMD ["python3", "-u", "/app/trading_system/backtester.py"]
