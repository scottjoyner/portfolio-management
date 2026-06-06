# Data Collector Service
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    pandas==2.2.0 \
    numpy==1.26.2 \
    pyyaml==6.0.1 \
    requests==2.31.0 \
    psycopg2-binary==2.9.9

# Copy application code
COPY data_collector.py /app/

# Create directories
RUN mkdir -p /app/logs /app/data /app/db

# Switch to non-root user (security)
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=60s --timeout=30 --retries=2 \
    CMD python -c "import sys; sys.path.insert(0, '/app'); import data_collector"

# Start with curl to API server before starting collector
CMD ["python", "/app/data_collector.py"]
