from __future__ import annotations

import asyncio
import json
import logging
import os
import signal as signal_module
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from event_markets.arbitrage import EventArbitrageScanner
from event_markets.crypto_divergence import CryptoPriceDivergenceDetector
from event_markets.unified_client import PredictionMarket, UnifiedPredictionMarketClient
from trading_system.exchange.coinbase.websocket.market_feed import CoinbaseWebSocketMarketClient

try:
    from coinbase.src.cb_client import CBClient
except ImportError:
    CBClient = None  # type: ignore[assignment,misc]

log = logging.getLogger("unified_market_daemon")

STATE_PATH = ROOT / "data" / "operator-state.json"
PAPER_TRADES_PATH = ROOT / "data" / "paper-trades.json"
DEFAULT_PRODUCTS = [
    "BTC-USD", "ETH-USD", "SOL-USD",
    "XRP-USD", "ADA-USD", "DOGE-USD",
    "AVAX-USD", "DOT-USD", "LINK-USD",
    "POL-USD", "ATOM-USD", "LTC-USD",
    "NEAR-USD", "APT-USD", "SUI-USD",
    "SHIB-USD", "PEPE-USD", "TRUMP-USD",
]


def _load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default if default is not None else {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return default if default is not None else {}


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    tmp.replace(path)


def _score_market(market: Any) -> float:
    volume = float(getattr(market, "volume", 0) or 0)
    liquidity = float(getattr(market, "liquidity_score", 0) or 0)
    spread = float(getattr(market, "spread", 0) or 0)
    sports = 1.25 if any(
        term in f"{getattr(market, 'question', '')} {getattr(market, 'category', '')}".lower()
        for term in ("sport", "soccer", "football", "world cup", "fifa", "uefa", "nfl", "nba", "mlb")
    ) else 1.0
    return volume * max(liquidity, 0.0) * max(1.0 - min(spread, 0.95), 0.05) * sports


class UnifiedMarketDaemon:
    def __init__(
        self,
        products: list[str] | None = None,
        scan_interval_seconds: int = 60,
        state_path: Path = STATE_PATH,
        pm_timeout_seconds: int = 8,
        arb_timeout_seconds: int = 10,
    ) -> None:
        self.products = products or list(DEFAULT_PRODUCTS)
        self.scan_interval_seconds = max(15, int(scan_interval_seconds))
        self.state_path = state_path
        self.pm_timeout_seconds = max(3, int(pm_timeout_seconds))
        self.arb_timeout_seconds = max(5, int(arb_timeout_seconds))
        try:
            from dotenv import load_dotenv
            load_dotenv(ROOT / ".env")
        except ImportError:
            pass
        self.coinbase = CoinbaseWebSocketMarketClient()
        self.pm_client = UnifiedPredictionMarketClient(
            kalshi_api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
            kalshi_private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
        )
        self.arb_scanner = EventArbitrageScanner(self.pm_client)
        self._cb_client: Any = None
        if CBClient is not None:
            try:
                self._cb_client = CBClient()
            except Exception:
                pass
        self.divergence_detector = CryptoPriceDivergenceDetector(
            coinbase_client=self._cb_client
        )
        self.divergence_history: list[dict[str, Any]] = []
        self._divergence_history_initialized = False
        self.stop_event = asyncio.Event()
        self.latest_coinbase: dict[str, dict[str, Any]] = {}
        self.latest_prediction: dict[str, Any] = {}
        self.latest_arb: dict[str, Any] = {}
        self.latest_crypto_divergence: dict[str, Any] = {}

    async def _handle_coinbase_message(self, message: dict[str, Any]) -> None:
        # Try top-level fields first
        product_id = message.get("product_id") or message.get("productId")
        price = message.get("price")

        # Coinbase WS ticker format: events[].tickers[].{price,product_id}
        if not product_id or price is None:
            for event in message.get("events") or []:
                for ticker in (event.get("tickers") or event.get("updates") or []):
                    if isinstance(ticker, dict):
                        pid = ticker.get("product_id") or ticker.get("productId")
                        if pid:
                            product_id = pid
                        p = ticker.get("price")
                        if p is not None:
                            price = p
                        if product_id and price is not None:
                            break
                if product_id and price is not None:
                    break

        if not product_id or price is None:
            return

        self.latest_coinbase[str(product_id)] = {
            "price": float(price),
            "channel": message.get("channel", message.get("type", "unknown")),
            "timestamp": message.get("timestamp") or time.time(),
        }

    async def _run_coinbase_feed(self) -> None:
        for product in self.products:
            self.coinbase.subscribe(product)
        self.coinbase.on_any(self._handle_coinbase_message)
        await self.coinbase.run()

    async def _scan_predictions(self) -> list[PredictionMarket]:
        categories = await asyncio.wait_for(
            asyncio.to_thread(
                self.pm_client.search_all_categories,
                limit_per_platform=25,
                min_volume=0,
                max_spread=0.45,
            ),
            timeout=self.pm_timeout_seconds,
        )
        all_markets: list[PredictionMarket] = []
        flattened = []
        counts = {}
        for category, items in (categories or {}).items():
            counts[category] = len(items)
            for item in items:
                all_markets.append(item)
                flattened.append({
                    "platform": getattr(item, "platform", "unknown"),
                    "market_id": getattr(item, "market_id", ""),
                    "question": getattr(item, "question", ""),
                    "category": getattr(item, "category", category),
                    "volume": float(getattr(item, "volume", 0) or 0),
                    "spread": float(getattr(item, "spread", 0) or 0),
                    "liquidity_score": float(getattr(item, "liquidity_score", 0) or 0),
                    "mid_price": float(getattr(item, "mid_price", 0) or 0),
                    "probability_extremity": float(getattr(item, "probability_extremity", 0) or 0),
                    "is_relevant": bool(getattr(item, "is_relevant", False)),
                    "heat_score": _score_market(item),
                })

        flattened.sort(key=lambda x: (x["heat_score"], x["liquidity_score"], x["volume"]), reverse=True)
        self.latest_prediction = {
            "markets": flattened[:50],
            "rankings": flattened[:50],
            "categories": counts,
            "total_markets": len(flattened),
        }
        return all_markets

    async def _scan_arbitrage(self, markets: list[PredictionMarket] | None = None) -> None:
        if markets:
            opportunities = await asyncio.wait_for(
                asyncio.to_thread(self.arb_scanner.scan_markets, markets),
                timeout=self.arb_timeout_seconds,
            )
        else:
            opportunities = await asyncio.wait_for(
                asyncio.to_thread(self.arb_scanner.scan, 20),
                timeout=self.arb_timeout_seconds,
            )
        rankings = []
        for opp in opportunities or []:
            rankings.append({
                "event_key": opp.event_key,
                "category": opp.category,
                "platform_buy": opp.platform_buy,
                "platform_hedge": opp.platform_hedge,
                "buy_yes_price": float(opp.buy_yes_price),
                "hedge_yes_price": float(opp.hedge_yes_price),
                "total_cost": float(opp.total_cost),
                "guaranteed_payout": float(opp.guaranteed_payout),
                "edge": float(opp.edge),
                "edge_pct": float(opp.edge_pct),
                "confidence": float(opp.confidence),
                "reason": opp.reason,
                "source_markets": opp.source_markets,
            })
        self.latest_arb = {"opportunities": rankings[:50], "total_opportunities": len(rankings)}

    async def _scan_crypto_divergence(self, markets: list[PredictionMarket]) -> None:
        coinbase_prices = {}
        for pid, data in self.latest_coinbase.items():
            price = data.get("price")
            if price and float(price) > 0:
                coinbase_prices[pid] = float(price)
        if not coinbase_prices:
            self.latest_crypto_divergence = {"divergences": [], "total": 0, "total_significant": 0, "significant": [], "history": []}
            return

        self.divergence_detector.set_coinbase_prices(coinbase_prices)

        # Refresh historical vol from Coinbase candles once on first scan
        if not self._divergence_history_initialized:
            try:
                self.divergence_detector.refresh_historical_vols(self.products)
            except Exception:
                log.debug("Historical vol refresh failed", exc_info=True)
            self._divergence_history_initialized = True

        results = self.divergence_detector.analyze_markets(markets)
        significant = [r for r in results if r.is_significant]

        # Track divergence count over time for sparkline
        self.divergence_history.append({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total": len(results),
            "significant": len(significant),
        })
        if len(self.divergence_history) > 60:
            self.divergence_history = self.divergence_history[-60:]

        self.latest_crypto_divergence = {
            "divergences": [r.to_dict() for r in results[:30]],
            "significant": [r.to_dict() for r in significant],
            "total": len(results),
            "total_significant": len(significant),
            "history": self.divergence_history,
        }

        # Feed significant divergences into the unified signal cache
        self._write_divergence_signals(significant)

        # Auto paper trade divergences with very high conviction
        self._auto_paper_trade_divergences(significant)

    def _write_divergence_signals(self, divergences: list) -> None:
        signal_cache_path = ROOT / "data" / ".unified_signal_cache.json"
        existing = _load_json(signal_cache_path, {"signals": []})
        old_signals = existing.get("signals", [])

        kept = []
        seen_reasons: set[str] = set()
        for sig in old_signals:
            sn = str(sig.get("strategy_name", ""))
            if "divergence" not in sn.lower():
                kept.append(sig)
                continue
            reason = sig.get("signal_reason", "")
            if reason not in seen_reasons:
                seen_reasons.add(reason)
                kept.append(sig)

        for d in divergences:
            dv = d.to_dict() if hasattr(d, "to_dict") else d
            reason = f"crypto_divergence: {dv['signal']} | {dv['question'][:40]}"
            if reason in seen_reasons:
                continue
            seen_reasons.add(reason)

            entry = {
                "action": "BUY" if dv["signal"] == "PM_UNDERPRICING_YES" else "SELL",
                "symbol": f"{dv['asset_symbol']}-USD",
                "instrument": f"{dv['asset_symbol']}-USD",
                "strategy_name": f"Daemon:CryptoDivergence:{dv['asset_symbol']}",
                "confidence": round(dv["confidence"], 3),
                "opportunity_score": round(abs(dv["divergence"]) * dv["confidence"], 3),
                "signal_reason": reason,
                "source": "crypto_divergence",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            kept.append(entry)

        existing["signals"] = kept
        _write_json(signal_cache_path, existing)

    def _auto_paper_trade_divergences(self, divergences: list) -> None:
        """Auto-record paper trades for high-conviction divergences."""
        trades: list[dict[str, Any]] = []
        if PAPER_TRADES_PATH.exists():
            try:
                raw = json.loads(PAPER_TRADES_PATH.read_text())
                if isinstance(raw, list):
                    trades = raw
            except Exception:
                pass
        now_ts = time.time()
        one_day = 86400

        for d in divergences:
            dv = d.to_dict() if hasattr(d, "to_dict") else d
            # Threshold: |divergence| >= 25% AND confidence >= 0.5 AND kelly >= 0.1
            if abs(dv.get("divergence", 0)) < 0.25:
                continue
            if dv.get("confidence", 0) < 0.5:
                continue
            if dv.get("kelly_fraction", 0) < 0.10:
                continue

            event_key = f"crypto_divergence:{dv['market_id']}"
            # Dedup within 24h
            existing_24h = any(
                t.get("event_key") == event_key and (now_ts - t.get("timestamp", 0)) < one_day
                for t in trades[-50:]
            )
            if existing_24h:
                continue

            action = "SELL" if dv.get("signal") == "PM_OVERPRICING_YES" else "BUY"
            entry = {
                "event_key": event_key,
                "timestamp": now_ts,
                "datetime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "type": "crypto_divergence",
                "platform": dv.get("platform", "unknown"),
                "question": dv.get("question", ""),
                "action": action,
                "symbol": f"{dv.get('asset_symbol', '')}-USD",
                "edge_pct": round(abs(dv.get("divergence", 0)) * 100, 1),
                "kelly_fraction": round(dv.get("kelly_fraction", 0), 4),
                "confidence": round(dv.get("confidence", 0), 3),
                "market_prob": round(dv.get("market_probability", 0), 3),
                "fair_prob": round(dv.get("fair_probability", 0), 3),
                "spot_price": dv.get("spot_price", 0),
                "implied_vol": round(dv.get("implied_vol", 0), 2),
                "annualized_vol": round(dv.get("annualized_vol", 0), 2),
                "reason": f"divergence {dv.get('signal', '')} edge={abs(dv.get('divergence',0)):.1%} kelly={dv.get('kelly_fraction',0):.2%}",
            }
            trades.append(entry)
            log.info("Paper trade divergence: %s %s (kelly=%.2f%%)", action, event_key, dv.get("kelly_fraction", 0) * 100)

        _write_json(PAPER_TRADES_PATH, trades[-200:])

    def _build_snapshot(self) -> dict[str, Any]:
        snapshot = _load_json(self.state_path, {})
        paper_trades_path = ROOT / "data" / "paper-trades.json"
        paper_trade_count = 0
        if paper_trades_path.exists():
            try:
                pt = json.loads(paper_trades_path.read_text())
                paper_trade_count = len(pt)
            except Exception:
                pass
        snapshot["marketIntelligence"] = {
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "coinbase": {
                "products": self.products,
                "last_updates": self.latest_coinbase,
            },
            "prediction_markets": self.latest_prediction,
            "arbitrage": self.latest_arb,
            "crypto_divergence": {
                **self.latest_crypto_divergence,
                "history": self.divergence_history,
            },
            "paper_trade_count": paper_trade_count,
            "summary": {
                "coinbase_updates": len(self.latest_coinbase),
                "prediction_markets": int(self.latest_prediction.get("total_markets", 0) or 0),
                "arbitrage_opportunities": int(self.latest_arb.get("total_opportunities", 0) or 0),
                "crypto_divergences": int(self.latest_crypto_divergence.get("total", 0) or 0),
                "significant_divergences": int(self.latest_crypto_divergence.get("total_significant", 0) or 0),
                "paper_trades": paper_trade_count,
            },
        }
        return snapshot

    async def _scan_loop(self) -> None:
        while not self.stop_event.is_set():
            started = time.time()
            try:
                markets = await self._scan_predictions()
                await self._scan_arbitrage(markets)
                await self._scan_crypto_divergence(markets)
                _write_json(self.state_path, self._build_snapshot())
                # Write watchdog heartbeat
                heartbeat = self.state_path.parent / ".daemon_heartbeat"
                heartbeat.write_text(str(time.time()))
                log.info(
                    "scan_cycle complete coinbase=%d markets=%d arbs=%d divergences=%d sig=%d",
                    len(self.latest_coinbase),
                    int(self.latest_prediction.get("total_markets", 0) or 0),
                    int(self.latest_arb.get("total_opportunities", 0) or 0),
                    int(self.latest_crypto_divergence.get("total", 0) or 0),
                    int(self.latest_crypto_divergence.get("total_significant", 0) or 0),
                )
            except BaseException as e:
                if isinstance(e, (KeyboardInterrupt, asyncio.CancelledError)):
                    raise
                log.warning("scan cycle failed: %s", e, exc_info=True)

            elapsed = time.time() - started
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=max(1.0, self.scan_interval_seconds - elapsed))
            except asyncio.TimeoutError:
                pass

    def _log_task_exception(self, task: asyncio.Task, name: str) -> None:
        if not task.done():
            return
        try:
            exc = task.exception()
        except (asyncio.CancelledError, KeyboardInterrupt):
            return
        if exc and not isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
            log.error("task %s failed: %s: %s", name, type(exc).__name__, exc)

    async def run(self) -> None:
        def _stop() -> None:
            log.info("shutdown signal received")
            self.stop_event.set()

        loop = asyncio.get_running_loop()
        if hasattr(loop, "add_signal_handler"):
            try:
                loop.add_signal_handler(signal_module.SIGINT, _stop)
                loop.add_signal_handler(signal_module.SIGTERM, _stop)
            except (NotImplementedError, RuntimeError):
                pass

        feed_task = asyncio.create_task(self._run_coinbase_feed())
        scan_task = asyncio.create_task(self._scan_loop())
        try:
            await self.stop_event.wait()
        except BaseException:
            self.stop_event.set()
            raise
        finally:
            self.stop_event.set()
            feed_task.cancel()
            scan_task.cancel()
            await asyncio.gather(feed_task, scan_task, return_exceptions=True)
            self._log_task_exception(feed_task, "coinbase_feed")
            self._log_task_exception(scan_task, "scan_loop")
            _write_json(self.state_path, self._build_snapshot())
            log.info("daemon stopped")


def _parse_args(argv: list[str]) -> dict[str, Any]:
    args = {
        "products": ",".join(DEFAULT_PRODUCTS),
        "scan_interval_seconds": 60,
        "state_file": str(STATE_PATH),
        "pm_timeout_seconds": 30,
        "arb_timeout_seconds": 20,
    }
    for idx, arg in enumerate(argv):
        if arg == "--products" and idx + 1 < len(argv):
            args["products"] = argv[idx + 1]
        elif arg == "--scan-interval" and idx + 1 < len(argv):
            args["scan_interval_seconds"] = int(argv[idx + 1])
        elif arg == "--state-file" and idx + 1 < len(argv):
            args["state_file"] = argv[idx + 1]
        elif arg == "--pm-timeout" and idx + 1 < len(argv):
            args["pm_timeout_seconds"] = int(argv[idx + 1])
        elif arg == "--arb-timeout" and idx + 1 < len(argv):
            args["arb_timeout_seconds"] = int(argv[idx + 1])
    return args


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse

    try:
        import websockets  # noqa: F401
        log.info("websockets package available — Coinbase live feed enabled")
    except ImportError:
        log.warning(
            "websockets package NOT installed — Coinbase WebSocket feed will be unavailable.\n"
            "  Install with:  pip install websockets   (or use the project venv)"
        )

    parser = argparse.ArgumentParser(description="Run the unified market daemon")
    parser.add_argument("--products", default=",".join(DEFAULT_PRODUCTS), help="Comma-separated Coinbase products")
    parser.add_argument("--scan-interval", type=int, default=60, help="Seconds between prediction/arb scans")
    parser.add_argument("--state-file", default=str(STATE_PATH), help="Operator state JSON path")
    parser.add_argument("--pm-timeout", type=int, default=8, help="Prediction market scan timeout")
    parser.add_argument("--arb-timeout", type=int, default=10, help="Arbitrage scan timeout")
    parsed = parser.parse_args()

    attempt = 0
    while True:
        attempt += 1
        try:
            daemon = UnifiedMarketDaemon(
                products=[p.strip() for p in parsed.products.split(",") if p.strip()],
                scan_interval_seconds=parsed.scan_interval,
                state_path=Path(parsed.state_file),
                pm_timeout_seconds=parsed.pm_timeout,
                arb_timeout_seconds=parsed.arb_timeout,
            )
            asyncio.run(daemon.run())
        except (KeyboardInterrupt, SystemExit):
            log.info("daemon stopped by user")
            break
        except BaseException as e:
            log.critical("daemon crashed: %s: %s", type(e).__name__, e, exc_info=True)
            delay = min(2 ** attempt, 120)
            log.info("restarting in %ds (attempt %d)...", delay, attempt)
            time.sleep(delay)
            continue
        break


if __name__ == "__main__":
    main()
