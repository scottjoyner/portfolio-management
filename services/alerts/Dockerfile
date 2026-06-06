# Alerts Service
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    pandas==2.2.0 \
    numpy==1.26.2 \
    pyyaml==6.0.1 \
    requests==2.31.0 \
    websockets==12.0

# Copy application code
COPY alerts.py /app/

# Create directories
RUN mkdir -p /app/logs /app/templates

# Switch to non-root user (security)
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app
USER appuser

# Expose port for webhooks
EXPOSE 3003

# Health check
HEALTHCHECK --interval=60s --timeout=30 --retries=2 \
    CMD python -c "import sys; sys.path.insert(0, '/app'); import alerts"

# Start monitoring loop
CMD ["python", "/app/alerts.py"]
