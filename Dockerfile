FROM node:22-bookworm-slim AS runtime

WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000

RUN corepack enable && corepack prepare pnpm@9.12.3 --activate

COPY package.json ./
RUN pnpm install --prod --frozen-lockfile=false

COPY apps ./apps
COPY packages ./packages
COPY scripts ./scripts
COPY docs ./docs

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 3000) + '/health').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "apps/api/src/server.p1.mjs"]
