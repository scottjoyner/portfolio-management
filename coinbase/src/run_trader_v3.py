#!/usr/bin/env python3
"""
Unified Trader v3 — high-throughput trading daemon with GPU/CPU batch signal
processing, direct REST data feeds (no CLI subprocess), WebSocket live ticker,
and stage-by-stage latency profiling.

Data Pipeline (fastest → fallback):
  1. WebSocket feed  — real-time ticker updates (when websocket-client installed)
  2. REST keep-alive — parallel candle fetching via urllib3 PoolManager
  3. CLI fallback    — subprocess (legacy, ~3x slower)

Usage:
    python3 -m coinbase.src.run_trader_v3 --mode paper --interval 30
    python3 -m coinbase.src.run_trader_v3 --mode live --health-port 9090
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("trader_v3")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from strategy_engine import run_strategies as _run_strategies
from strategy_engine import backtest_strategy as _backtest_strategy
from strategy_engine import batch_backtest_rust, _HAS_RUST
from strategy_engine import batch_signals_fast as _batch_signals_fast

from trading_system.core.timing import LatencyProfiler, measure_coinbase_latency, http_roundtrip

_IO_POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="v3_io")


class CandleCache:
    """Background pre-fetcher for candle data.
    Uses direct REST API (urllib3 keep-alive) instead of CLI subprocess.
    Runs on a daemon thread, refreshes every `refresh_secs` seconds.
    """

    def __init__(
        self,
        products: List[str],
        granularity: int = 3600,
        limit: int = 100,
        refresh_secs: float = 15.0,
        profiler: Optional[LatencyProfiler] = None,
    ):
        self.products = products
        self.granularity = granularity
        self.limit = limit
        self.refresh_secs = refresh_secs
        self.profiler = profiler
        self._cache: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_refresh: float = 0

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="candle_cache")
        self._thread.start()
        log.info("CandleCache started: %d products, refresh every %.0fs (REST keep-alive)", len(self.products), self.refresh_secs)

    def stop(self):
        self._running = False

    def _run(self):
        while self._running:
            t0 = time.perf_counter()
            self._refresh_all()
            elapsed = time.perf_counter() - t0
            self._last_refresh = elapsed
            if self.profiler:
                self.profiler.record("candle_cache_refresh", elapsed * 1000.0)
            remaining = self.refresh_secs - elapsed
            if remaining > 0:
                time.sleep(remaining)

    def _refresh_all(self):
        """Parallel candle fetch via REST API with HTTP keep-alive."""
        from coinbase.src.rest_feed import fetch_candles_rest, candle_arrays

        def _fetch(pid: str) -> Optional[dict]:
            try:
                candles = fetch_candles_rest(pid, granularity=self.granularity, limit=self.limit)
                if len(candles) < 30:
                    return None
                arrs = candle_arrays(candles)
                arrs["ts"] = time.time()
                return arrs
            except Exception as e:
                log.debug("Candle fetch failed for %s: %s", pid, e)
                return None

        futures = {_IO_POOL.submit(_fetch, pid): pid for pid in self.products}
        for fut in as_completed(futures):
            pid = futures[fut]
            try:
                data = fut.result()
                if data:
                    with self._lock:
                        self._cache[pid] = data
            except Exception as e:
                log.debug("Candle cache refresh failed for %s: %s", pid, e)

    def get(self, pid: str) -> Optional[dict]:
        with self._lock:
            return self._cache.get(pid)

    def get_all(self) -> Dict[str, dict]:
        with self._lock:
            return dict(self._cache)


class HealthServer:
    """Health + latency endpoint on a daemon thread.
    Serves /health (basic) and /latency (stage-by-stage timing).
    """

    def __init__(self, port: int = 9090):
        self.port = port
        self.status: Dict[str, Any] = {"status": "starting", "tick_count": 0}

    def start(self):
        t = threading.Thread(target=self._serve, daemon=True, name="health")
        t.start()

    def _serve(self):
        import http.server
        handler = self._make_handler()
        server = http.server.HTTPServer(("0.0.0.0", self.port), handler)
        log.info("Health server on port %d", self.port)
        server.serve_forever()

    def _make_handler(self):
        status_ref = self.status

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/latency":
                    body = json.dumps(status_ref.get("latency", {}), indent=2).encode()
                elif self.path == "/ping":
                    import subprocess
                    hosts = [
                        ("api.coinbase.com", "Coinbase API"),
                        ("api.exchange.coinbase.com", "Coinbase Exchange"),
                    ]
                    results = {}
                    for host, label in hosts:
                        try:
                            out = subprocess.run(
                                ["ping", "-c", "3", "-W", "3", host],
                                capture_output=True, text=True, timeout=10,
                            )
                            for line in out.stdout.splitlines():
                                if "rtt min/avg/max/mdev" in line:
                                    parts = line.split("=")[-1].strip().split("/")
                                    results[label] = {
                                        "min_ms": float(parts[0]),
                                        "avg_ms": float(parts[1]),
                                        "max_ms": float(parts[2]),
                                    }
                        except Exception as e:
                            results[label] = {"error": str(e)}
                    body = json.dumps(results, indent=2).encode()
                else:
                    body = json.dumps(status_ref, indent=2).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a, **kw):
                pass

        return H


class UnifiedTraderV3:
    """Unified trading daemon with background data feed and parallel execution."""

    # Target allocation buckets
    CORE_ASSETS = {"BTC", "ETH", "SOL"}
    OP_COOLDOWN: Dict[str, float] = {
        "strategy": 60.0, "rebalance": 3600.0, "tlh": 86400.0,
    }

    def __init__(
        self,
        mode: str = "paper",
        interval: float = 30.0,
        products: Optional[List[str]] = None,
        health_port: int = 0,
        dry_run: bool = True,
    ):
        self.mode = mode
        self.interval = interval
        self.dry_run = dry_run
        self._shutdown = False
        self._tick_count = 0
        self._last_execution: Dict[str, float] = {}
        self._bt_cache: Dict[str, Any] = {}

        # Latency profiler — measures tick stages
        self.profiler = LatencyProfiler(max_history=500)

        # Product universe
        if products:
            self.products = products
        else:
            try:
                from graph_alpha_bot.app.strategies.coinbase_universe import COINBASE_SPOT_PAIRS
                self.products = list(COINBASE_SPOT_PAIRS)
            except Exception:
                self.products = [
                    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD",
                    "DOGE-USD", "AVAX-USD", "DOT-USD", "LINK-USD", "UNI-USD",
                ]

        # Background candle pre-fetcher (REST keep-alive, no CLI subprocess)
        self.candle_cache = CandleCache(
            products=self.products,
            granularity=3600,
            limit=100,
            refresh_secs=max(interval / 2, 10.0),
            profiler=self.profiler,
        )

        # WebSocket feed for live ticker data (real-time, no polling)
        self._ws_feed = None
        self._ticker_cache = None
        try:
            from coinbase.src.feed import TickerCache, WebSocketFeed
            self._ticker_cache = TickerCache()
            self._ws_feed = WebSocketFeed(self._ticker_cache)
            for pid in self.products:
                self._ws_feed.subscribe([pid])
            started = self._ws_feed.start()
            if started:
                log.info("WebSocket feed active for %d products", len(self.products))
            else:
                log.info("WebSocket feed unavailable, using REST + polling only")
        except Exception as e:
            log.debug("WebSocket init: %s", e)

        # Health + latency status
        self.health_status: Dict[str, Any] = {
            "status": "starting", "mode": mode, "tick_count": 0,
            "products": len(self.products), "rust_enabled": _HAS_RUST,
            "ws_feed": self._ws_feed is not None and self._ws_feed._running,
        }
        self.health_server = HealthServer(port=health_port) if health_port else None

        log.info(
            "UnifiedTraderV3 initialized: mode=%s interval=%.0fs products=%d rust=%s ws=%s",
            mode, interval, len(self.products), _HAS_RUST,
            self._ws_feed is not None and self._ws_feed._running,
        )

    def start(self):
        self.candle_cache.start()
        if self.health_server:
            self.health_server.start()
            self.health_server.status = self.health_status

        signal.signal(signal.SIGINT, lambda *a: setattr(self, "_shutdown", True))
        signal.signal(signal.SIGTERM, lambda *a: setattr(self, "_shutdown", True))

        self.health_status["status"] = "running"
        log.info("Main loop starting (interval=%.0fs)", self.interval)

        try:
            while not self._shutdown:
                tick_start = time.time()
                self._tick()
                elapsed = time.time() - tick_start
                self.profiler.record_tick()
                self._tick_count += 1
                self.health_status["tick_count"] = self._tick_count
                self.health_status["last_tick_ms"] = round(elapsed * 1000)
                sleep_time = max(0.1, self.interval - elapsed)
                if self._shutdown:
                    break
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            log.info("Shutdown requested")
        except Exception as e:
            log.error("Fatal error: %s", e, exc_info=True)
            self.health_status["status"] = "crashed"
            self.health_status["last_error"] = str(e)
        finally:
            self._cleanup()

    def _cleanup(self):
        log.info("Shutting down...")
        self.candle_cache.stop()
        if self._ws_feed:
            self._ws_feed.stop()
        if self.health_server:
            self.health_server.status = self.health_status
            # Add final latency summary
            try:
                self.health_status["latency"] = self.profiler.summary()
            except Exception:
                pass
        self.health_status["status"] = "stopped"
        log.info("Shutdown complete")

    def _tick(self):
        """Single tick: batch signal generation + backtest + opportunity ranking.
        Every I/O-bound stage is profiled for latency tracking.
        """
        # 1. Update live prices from WebSocket (zero-latency, already cached)
        ws_price: Dict[str, float] = {}
        if self._ticker_cache:
            for pid in self.products:
                ticker = self._ticker_cache.get_ticker(pid)
                if ticker and ticker.price > 0:
                    ws_price[pid] = ticker.price

        # 2. Read cached candle data (zero I/O — pre-fetched by background thread)
        all_data = self.candle_cache.get_all()
        if not all_data:
            log.debug("No cached data yet, skipping tick")
            return

        # 3. Batch signal generation via compute backend (GPU/NumPy) — ~1ms
        with self.profiler.measure("batch_signals"):
            try:
                products_list = [
                    (pid, "safe" if pid.split("-")[0] in self.CORE_ASSETS else "growth")
                    for pid in all_data
                ]
                closes_dict = {pid: d["closes"] for pid, d in all_data.items()}
                volumes_dict = {pid: d["volumes"] for pid, d in all_data.items()}
                highs_dict = {pid: d.get("highs", []) for pid, d in all_data.items()}
                lows_dict = {pid: d.get("lows", []) for pid, d in all_data.items()}
                batch_sigs = _batch_signals_fast(
                    products_list, closes_dict, volumes_dict, highs_dict, lows_dict
                )
            except Exception as e:
                log.debug("Batch signal generation unavailable: %s", e)
                batch_sigs = {}

        # 4. Collect all signals for all products
        with self.profiler.measure("signal_collect"):
            candidates = []
            for pid, data in all_data.items():
                base = pid.split("-")[0]
                closes = data["closes"]
                volumes = data["volumes"]
                highs = data.get("highs")
                lows = data.get("lows")
                current_price = ws_price.get(pid) or (closes[-1] if closes else 0)
                asset_class = "safe" if base in self.CORE_ASSETS else "growth"

                if pid in batch_sigs and batch_sigs[pid]:
                    signals = []
                    for s_name, action in batch_sigs[pid].items():
                        if action != "HOLD":
                            signals.append(type("Sig", (), {
                                "strategy": s_name, "action": action, "confidence": 0.5,
                                "reason": f"batch:{s_name}", "symbol": base,
                            }))
                else:
                    signals = _run_strategies(
                        currency=base, asset_class=asset_class,
                        closes=closes, volumes=volumes,
                        current_price=current_price, highs=highs, lows=lows,
                    )
                candidates.append((base, pid, closes, volumes, highs, lows, current_price, signals))

        # 5. Batch backtest un-cached strategies across products in parallel
        with self.profiler.measure("batch_backtest"):
            bt_strategies = []
            for base, pid, closes, volumes, highs, lows, _, signals in candidates:
                for sig in signals:
                    s_name = getattr(sig, "strategy", "?")
                    ck = f"{s_name}/{base}"
                    if ck not in self._bt_cache:
                        bt_strategies.append((s_name, base, closes, volumes, highs, lows))
            if bt_strategies:
                bt_results = batch_backtest_rust(bt_strategies)
                for ck, verdict in bt_results.items():
                    self._bt_cache[ck] = verdict
                # Fallback for non-Rust
                for s_name, base, closes, volumes, highs, lows in bt_strategies:
                    ck = f"{s_name}/{base}"
                    if ck not in self._bt_cache:
                        try:
                            verdict = _backtest_strategy(s_name, base, closes, volumes,
                                highs=highs if highs else None,
                                lows=lows if lows else None,
                            )
                            self._bt_cache[ck] = verdict
                        except Exception as e:
                            log.debug("BT fallback %s: %s", ck, e)

        # 6. Generate opportunities from passed signals
        with self.profiler.measure("opportunity_rank"):
            opportunities = []
            for base, pid, closes, volumes, _, _, current_price, signals in candidates:
                for sig in signals[:5]:
                    s_name = getattr(sig, "strategy", "?")
                    ck = f"{s_name}/{base}"
                    verdict = self._bt_cache.get(ck)
                    if verdict and verdict.passed:
                        opportunities.append({
                            "currency": base, "product_id": pid,
                            "strategy": s_name, "action": sig.action,
                            "confidence": getattr(sig, "confidence", 0.5),
                            "price": current_price,
                        })

        # 7. Update health status with latency data
        self.health_status["last_tick_ms"] = round(self.profiler.summary().get("_tick", {}).get("last_ms", 0))
        self.health_status["latency"] = self.profiler.summary() if self._tick_count % 10 == 0 else self.health_status.get("latency", {})

        if opportunities:
            log.info("Tick #%d: %d opportunities from %d products (ws=%d batch=%s rust=%s)",
                      self._tick_count, len(opportunities), len(all_data),
                      len(ws_price), bool(batch_sigs), _HAS_RUST)

    # ── CLI entry ─────────────────────────────────────────────────

    @classmethod
    def from_cli(cls) -> "UnifiedTraderV3":
        p = argparse.ArgumentParser(description="Unified Trader v3")
        p.add_argument("--mode", choices=["paper", "approval", "live"], default="paper")
        p.add_argument("--interval", type=float, default=30.0, help="Tick interval in seconds")
        p.add_argument("--health-port", type=int, default=0, help="Health HTTP server port")
        p.add_argument("--products", nargs="*", help="Override product list")
        p.add_argument("--live", action="store_true", help="Short for --mode live --interval 15")
        args = p.parse_args()

        if args.live:
            args.mode = "live"
            args.interval = 15.0

        dry_run = args.mode != "live"
        return cls(
            mode=args.mode,
            interval=args.interval,
            products=args.products,
            health_port=args.health_port,
            dry_run=dry_run,
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )
    trader = UnifiedTraderV3.from_cli()
    trader.start()


if __name__ == "__main__":
    main()
