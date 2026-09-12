FROM rust:1.89.0-bookworm AS rust-builder

WORKDIR /src
RUN apt-get update -qq \
  && apt-get install -y -qq python3 python3-dev python3-venv --no-install-recommends \
  && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /venv \
  && /venv/bin/pip install --quiet --no-cache-dir 'maturin>=1.7,<2.0'

COPY pyproject.toml ./
COPY rust_core ./rust_core
RUN /venv/bin/maturin build --release --out /wheels


FROM node:22-bookworm-slim AS runtime

WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV COINBASE_BRIDGE_SCRIPT=coinbase/src/bridge_execution.py
ENV COINBASE_PYTHON_PATH=python3
ENV CERTIFIED_RUNTIME_BUNDLE_PATH=/app/deploy/generated/certified-runtime-bundle.json
ENV CERTIFIED_RUNTIME_BUNDLE_REQUIRED=true

ARG REQUIRE_CERTIFIED_RUNTIME_BUNDLE=false

RUN apt-get update -qq \
  && apt-get install -y -qq python3 python3-pip --no-install-recommends \
  && rm -rf /var/lib/apt/lists/*
RUN pip3 install --break-system-packages --quiet --no-cache-dir pandas==2.2.0 numpy==1.26.2 pyyaml==6.0.1 requests==2.31.0 websockets==12.0 psycopg2-binary==2.9.9 yfinance==0.2.54
RUN pip3 install --break-system-packages --quiet --no-cache-dir coinbase-advanced-trade-python python-dotenv 2>/dev/null || true
RUN npm install -g @coinbase/coinbase-cli 2>/dev/null || true

COPY --from=rust-builder /wheels /tmp/rust-wheels
RUN pip3 install --break-system-packages --quiet --no-cache-dir /tmp/rust-wheels/*.whl \
  && rm -rf /tmp/rust-wheels

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

COPY apps ./apps
COPY packages ./packages
COPY coinbase ./coinbase
COPY scripts ./scripts
COPY . /app

# The checkout source package is intentionally first on PYTHONPATH in the
# scanner. Copy the installed native extension beside that source package so
# production resolves the same package layout exercised by runtime code.
RUN native="$(cd /tmp && python3 -c 'import importlib; print(importlib.import_module("rust_core.rust_core").__file__)')" \
  && cp "$native" /app/rust_core/ \
  && cd /tmp \
  && PYTHONPATH=/app python3 - <<'PY'
import importlib
import pathlib
import rust_core

native = importlib.import_module("rust_core.rust_core")
path = pathlib.Path(native.__file__).resolve()
assert rust_core.RUST_CORE_AVAILABLE is True
assert path.parent == pathlib.Path("/app/rust_core")
assert path.suffix in {".so", ".pyd", ".dylib"}
assert callable(rust_core.run_rsi_revert_opens_configured_py)
print(f"certified_native_runtime={path}")
PY

# Production compose sets this build argument to true. CI/source-only image
# checks may leave it false because generated certification artifacts are never
# committed to Git.
RUN if [ "$REQUIRE_CERTIFIED_RUNTIME_BUNDLE" = "true" ]; then \
      test -s /app/deploy/generated/certified-runtime-bundle.json; \
    fi

RUN mkdir -p /app/data /app/state

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 3000) + '/health').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "apps/api/src/server.p1.mjs"]
