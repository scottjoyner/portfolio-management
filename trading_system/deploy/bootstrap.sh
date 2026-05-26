#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

if ! command -v python3.12 >/dev/null 2>&1; then
  echo "Python 3.12 is required. Install it before continuing."
  exit 1
fi

if [ ! -d ".venv" ]; then
  python3.12 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]

if [ ! -f deploy/.env ]; then
  cp deploy/.env.example deploy/.env
  echo "Created deploy/.env from deploy/.env.example. Edit deploy/.env before starting the service."
fi

echo "Bootstrap complete."
echo "To start with Docker Compose:"
echo "  cd deploy && docker compose -f docker-compose.prod.yml up -d --build"
echo "To use systemd: see deploy/systemd/portfolio.service"
