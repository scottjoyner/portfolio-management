FROM python:3.12-slim AS builder

WORKDIR /build
COPY pyproject.toml .
COPY trading_system/ .
RUN pip install --no-cache-dir build && python -m build --wheel


FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -r trader && useradd -r -g trader -d /app trader

WORKDIR /app
COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

COPY . .

RUN pip install -e .[dev]

USER trader

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["uvicorn", "apps.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
