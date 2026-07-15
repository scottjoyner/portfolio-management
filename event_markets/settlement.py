"""Settlement tracking for prediction-market paper trades.

Resolves open arbitrage paper trades (written by ``EventArbitrageScanner``) once
their underlying markets settle, computing realized P&L from the *actual* outcomes.

For a genuine same-event arbitrage the payout is $1 regardless of which way the
event resolves. Tracking the real outcomes lets us detect mismatched pairs (our
semantic matcher thought two markets were the same event but they resolved
differently), which would turn a "locked" edge into a real loss/windfall — the
key signal for validating match quality.

Resolution sources:
  - Polymarket: fetch the market detail; a closed market's winning outcome is the
    one priced ~1.0.
  - Kalshi: fetch the market by ticker; when settled, its ``result`` field
    ("yes"/"no") gives the outcome. Falls back to age-based settlement only when a
    leg can't be resolved (e.g. network error).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .unified_client import UnifiedPredictionMarketClient

logger = logging.getLogger("event_settlement")

ROOT = Path(__file__).resolve().parent.parent
PAPER_TRADES_PATH = ROOT / "data" / "paper-trades.json"
DEFAULT_MAX_AGE_DAYS = 7


def _parse_ts(ts: Any) -> float:
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            return 0.0
    return 0.0


class SettlementTracker:
    def __init__(
        self,
        client: Optional[UnifiedPredictionMarketClient] = None,
        trades_path: Path = PAPER_TRADES_PATH,
    ):
        self.client = client or UnifiedPredictionMarketClient()
        self.trades_path = Path(trades_path)

    # ── persistence ──────────────────────────────────────────────────────────
    def _load(self) -> List[Dict[str, Any]]:
        if not self.trades_path.exists():
            return []
        try:
            data = json.loads(self.trades_path.read_text())
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError):
            return []

    def _save(self, trades: List[Dict[str, Any]]) -> None:
        self.trades_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.trades_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(trades, indent=2))
        tmp.replace(self.trades_path)

    # ── resolution lookups ───────────────────────────────────────────────────
    def _resolve_yes(self, platform: str, market_id: str) -> Optional[int]:
        """Return 1 if the market resolved YES, 0 if NO, None if unresolved/unknown."""
        if not market_id:
            return None
        if platform == "polymarket":
            try:
                pm = self.client._polymarket.fetch_market_detail(market_id)
            except Exception as e:  # pragma: no cover - network
                logger.debug("poly detail failed for %s: %s", market_id, e)
                return None
            if pm is None or not getattr(pm, "closed", False):
                return None
            prices = getattr(pm, "outcome_prices", {}) or {}
            yes = prices.get("YES")
            if yes is None and prices:
                yes = next(iter(prices.values()))
            if yes is None:
                return None
            return 1 if float(yes) >= 0.5 else 0
        if platform == "kalshi":
            try:
                return self.client._kalshi.get_settlement(market_id)
            except Exception as e:  # pragma: no cover - network
                logger.debug("kalshi settlement failed for %s: %s", market_id, e)
                return None
        return None

    # ── settlement ───────────────────────────────────────────────────────────
    def settle_open_trades(self, max_age_days: int = DEFAULT_MAX_AGE_DAYS) -> Dict[str, Any]:
        trades = self._load()
        now = time.time()
        settled = expired = still_open = 0
        realized_total = 0.0
        changed = False

        for t in trades:
            if t.get("status") not in ("open", "live_open") or t.get("type") != "arbitrage":
                continue

            notional = float(t.get("notional", 0.0) or 0.0)
            total_cost = float(t.get("total_cost", 0.0) or 0.0)
            expected_profit = float(t.get("expected_profit", 0.0) or 0.0)
            fees = float(t.get("estimated_fees", 0.0) or 0.0)
            # Prefer the fee-adjusted expectation when present (executor records it).
            net_expected = t.get("net_expected_profit")
            net_expected = float(net_expected) if net_expected is not None else (expected_profit - fees)
            is_live = t.get("status") == "live_open" or t.get("mode") == "live"
            # Paper trades use `notional` as the number of $1-payout units (legacy
            # convention). Live trades record the real `contracts` count, which is
            # the correct multiplier for actual realized P&L.
            multiplier = float(t.get("contracts") or 0.0) or notional
            settled_status = "settled"

            buy_yes_res = self._resolve_yes(t.get("platform_buy", ""), t.get("buy_market_id", ""))
            hedge_yes_res = self._resolve_yes(t.get("platform_hedge", ""), t.get("hedge_market_id", ""))

            age_days = (now - _parse_ts(t.get("timestamp", 0))) / 86400.0

            if buy_yes_res is not None and hedge_yes_res is not None:
                # Actual payout: we hold YES on buy leg and NO on hedge leg.
                payout_units = (1.0 if buy_yes_res == 1 else 0.0) + (1.0 if hedge_yes_res == 0 else 0.0)
                realized = (payout_units - total_cost) * multiplier - fees
                t["status"] = settled_status
                t["settled_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                t["resolution"] = {
                    "buy_yes_resolved": buy_yes_res,
                    "hedge_yes_resolved": hedge_yes_res,
                    "payout_units": round(payout_units, 4),
                    "hedge_held": buy_yes_res != hedge_yes_res,
                    "live": is_live,
                }
                t["realized_pnl"] = round(realized, 2)
                realized_total += realized
                settled += 1
                changed = True
                # A genuine same-event hedge resolves oppositely (YES on one leg,
                # NO on the other). They resolving IDENTICALLY means the semantic
                # matcher paired two different events — a real mismatch.
                if buy_yes_res == hedge_yes_res:
                    logger.warning(
                        "Arb legs DIVERGED for %s (buy_yes=%d hedge_yes=%d) — mismatched pair, realized %.2f",
                        t.get("event_key", "?"), buy_yes_res, hedge_yes_res, realized,
                    )
            elif age_days >= max_age_days:
                # Fallback: assume a true same-event hedge held and pay the locked edge.
                t["status"] = "expired"
                t["settled_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                t["resolution"] = {"assumed_hedge_held": True, "reason": f"age {age_days:.1f}d >= {max_age_days}d"}
                t["realized_pnl"] = round(net_expected, 2)
                realized_total += net_expected
                expired += 1
                changed = True
            else:
                still_open += 1

        if changed:
            self._save(trades)

        return {
            "settled": settled,
            "expired": expired,
            "still_open": still_open,
            "realized_pnl": round(realized_total, 2),
            "total_trades": len(trades),
        }

    def summary(self) -> Dict[str, Any]:
        trades = self._load()
        by_status: Dict[str, int] = {}
        realized = 0.0
        open_expected = 0.0
        diverged = 0
        for t in trades:
            st = t.get("status", "unknown")
            by_status[st] = by_status.get(st, 0) + 1
            if st in ("settled", "expired"):
                realized += float(t.get("realized_pnl", 0.0) or 0.0)
                res = t.get("resolution") or {}
                if res.get("hedge_held") is False:
                    diverged += 1
            elif st in ("open", "live_open"):
                open_expected += float(t.get("expected_profit", 0.0) or 0.0)
        return {
            "total_trades": len(trades),
            "by_status": by_status,
            "realized_pnl": round(realized, 2),
            "open_expected_pnl": round(open_expected, 2),
            "diverged_pairs": diverged,
        }
