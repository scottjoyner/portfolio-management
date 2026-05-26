# Trading System Deployment

This folder contains deployment assets for the `trading_system` service.

## Files

- `docker-compose.prod.yml` — production-ready compose stack for API, Postgres, and Redis
- `systemd/portfolio.service` — sample systemd unit for `uvicorn`
- `bootstrap.sh` — Ubuntu bootstrap script for Python 3.12 and local dev
- `.env.example` — example environment file for runtime configuration

## Quick start

1. Copy environment variables:
   ```bash
   cp deploy/.env.example deploy/.env
   ```
2. Edit `deploy/.env` with your production values.
3. Build and start containers:
   ```bash
   cd trading_system/deploy
   docker compose -f docker-compose.prod.yml up -d --build
   ```
4. Validate:
   ```bash
   docker compose -f docker-compose.prod.yml ps
   docker compose -f docker-compose.prod.yml logs -f api
   ```

## Systemd deployment

Copy the systemd unit and start the service:

```bash
sudo cp trading_system/deploy/systemd/portfolio.service /etc/systemd/system/portfolio.service
sudo systemctl daemon-reload
sudo systemctl enable --now portfolio.service
sudo journalctl -u portfolio.service -f
```

## Notes

- The service exposes `http://0.0.0.0:8000`.
- Metrics are available on `/metrics`.
- Health is available on `/health`.
- `TRADING_MODE` defaults to `PAPER`, and live modes require `LIVE_TRADING_ENABLED=true`.
