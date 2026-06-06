# Portfolio Manager Service
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    pandas==2.2.0 \
    numpy==1.26.2 \
    pyyaml==6.0.1 \
    requests==2.31.0

# Copy application code
COPY portfolio_manager.py /app/
COPY run_backtest.py /app/

# Create directories for logs and data
RUN mkdir -p /app/logs /app/data

# Switch to non-root user (security)
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app
USER appuser

# Expose ports for health checks
EXPOSE 3001

# Start script
CMD ["python", "/app/run_backtest.py"]
