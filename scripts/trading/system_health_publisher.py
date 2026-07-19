#!/usr/bin/env python3
"""Publish host-only Portfolio OS system truth atomically into data/system-health.json.

Runs under the production supervisor. It never changes trading state, starts/stops
services, or probes a broker; it only observes local health and durable cache state.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA = ROOT / "data"
OUTPUT = DATA / "system-health.json"
TRADER_URL = "http://127.0.0.1:9090/health"
STALE_SECONDS = 180.0


def freshness(age: float | None) -> str:
    return "unknown" if age is None else ("fresh" if age <= STALE_SECONDS else "stale")


def trader_health() -> dict:
    try:
        with urllib.request.urlopen(TRADER_URL, timeout=0.25) as response:
            payload = json.loads(response.read().decode("utf-8"))
        mode = str(payload.get("mode", payload.get("trading_mode", ""))).lower()
        return {"available": True, "mode": mode if mode in {"paper", "live"} else None,
                "status": str(payload.get("status", "unknown")), "source": "127.0.0.1:9090"}
    except Exception as exc:
        return {"available": False, "mode": None, "status": "unknown", "source": "127.0.0.1:9090", "error": type(exc).__name__}


def service_state(name: str) -> str:
    try:
        return subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True, timeout=2).stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def build() -> dict:
    now = time.time()
    heartbeat = DATA / ".daemon_heartbeat"
    age = None
    try:
        age = max(0.0, now - float(heartbeat.read_text().strip()))
    except (OSError, ValueError):
        pass
    trader = trader_health()
    try:
        from data.feed_cache import inspect_cache
        cache = inspect_cache()
    except Exception as exc:
        cache = {"status": "unknown", "source": "feed_cache", "error": type(exc).__name__}
    return {
        "generated_at": now,
        "trading_mode": trader["mode"],
        "trader": trader,
        "feed": {"heartbeat": {"age_sec": round(age, 1) if age is not None else None, "freshness": freshness(age)}},
        "cache": cache,
        "services": {"portfolio_trader": service_state("portfolio-trader.service")},
    }


def publish() -> None:
    DATA.mkdir(exist_ok=True)
    payload = json.dumps(build(), sort_keys=True, separators=(",", ":"))
    fd, temporary = tempfile.mkstemp(prefix=".system-health.", suffix=".tmp", dir=DATA)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, OUTPUT)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=15.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    while True:
        publish()
        if args.once:
            return
        time.sleep(max(1.0, args.interval))


if __name__ == "__main__":
    main()
