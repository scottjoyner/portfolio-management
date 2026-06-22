FROM node:22-bookworm-slim AS runtime

WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV COINBASE_BRIDGE_SCRIPT=coinbase/src/bridge_execution.py
ENV COINBASE_PYTHON_PATH=python3

RUN apt-get update -qq && apt-get install -y -qq python3 python3-pip --no-install-recommends && rm -rf /var/lib/apt/lists/*
RUN pip3 install --break-system-packages --quiet --no-cache-dir pandas==2.2.0 numpy==1.26.2 pyyaml==6.0.1 requests==2.31.0 websockets==12.0 psycopg2-binary==2.9.9 yfinance==0.2.54
RUN pip3 install --break-system-packages --quiet --no-cache-dir coinbase-advanced-trade-python python-dotenv 2>/dev/null || true

RUN corepack enable && corepack prepare pnpm@9.12.3 --activate
RUN npm install -g @coinbase/coinbase-cli 2>/dev/null || true

COPY package.json pnpm-lock.yaml* ./
RUN pnpm install --prod --frozen-lockfile=false 2>/dev/null || pnpm install --prod

COPY apps ./apps
COPY packages ./packages
COPY coinbase ./coinbase
COPY scripts ./scripts
COPY . /app

RUN mkdir -p /app/data /app/state

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 3000) + '/health').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "apps/api/src/server.p1.mjs"]
