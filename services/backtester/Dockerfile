# Backtester Service
# Build context: repository root (docker build -f services/backtester/Dockerfile .)
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

# Real backtester entrypoint (trading_system/backtester.py has a __main__)
EXPOSE 3002
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start_period=30s \
    CMD python3 -c "import sys; sys.path.insert(0, '/app'); import trading_system.backtester; print('OK')"

CMD ["python3", "-u", "/app/trading_system/backtester.py"]
