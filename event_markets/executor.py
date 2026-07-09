"""Two-leg prediction-market arbitrage executor.

Takes a detected arbitrage opportunity and (in live mode) places both legs —
buy YES on one venue, buy NO (hedge) on the other — with risk controls and a
one-leg-filled unwind path so you are never left with a naked directional
position.

SAFETY MODEL (important — real money):
  * Default mode is "dry_run": it records an intended paper trade and places NO
    real orders.
  * "live" mode is gated by ALL of:
      - env ARBITRAGE_LIVE_ENABLED == "true"
      - no kill-switch file present (data/arbitrage-kill-switch)
      - BOTH leg venues are live-configured (Kalshi: api key + private key;
        Polymarket: on-chain wallet — NOT yet implemented, so any Polymarket
        leg blocks live execution). This prevents placing a single real leg
        (which would be an unhedged, naked bet — the opposite of arbitrage).
  * Per-request confirmation is enforced at the API layer.

Records are appended to data/paper-trades.json in a format compatible with the
existing settlement tracker, plus extra execution metadata.
"""
from __future__ import annotations

import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import logging
    logger = logging.getLogger(__name__)
except Exception:  # pragma: no cover
    logger = None


ROOT = Path(__file__).resolve().parents[1]
PAPER_TRADES_PATH = ROOT / "data" / "paper-trades.json"
KILL_SWITCH_PATH = ROOT / "data" / "arbitrage-kill-switch"


def _log(level: str, msg: str, *args) -> None:
    if logger:
        getattr(logger, level, logger.info)(msg, *args)


@dataclass
class ExecConfig:
    mode: str = "dry_run"                 # "dry_run" | "live"
    max_notional_usd: float = 100.0       # hard cap per execution
    min_edge_pct: float = 0.01            # require >=1% edge to execute
    max_slippage: float = 0.02            # reject if book price worse than quoted by > this
    min_confidence: float = 0.30
    kalshi_fee_rate: float = 0.07         # Kalshi fee = ceil(rate * C * P * (1-P))
    require_net_profit: bool = True       # reject if edge doesn't cover est. fees

    @classmethod
    def from_env(cls) -> "ExecConfig":
        live = os.environ.get("ARBITRAGE_LIVE_ENABLED", "").lower() in ("1", "true", "yes")
        return cls(
            mode="live" if live else "dry_run",
            max_notional_usd=float(os.environ.get("ARBITRAGE_MAX_NOTIONAL_USD", "100") or 100),
            min_edge_pct=float(os.environ.get("ARBITRAGE_MIN_EDGE_PCT", "0.01") or 0.01),
            max_slippage=float(os.environ.get("ARBITRAGE_MAX_SLIPPAGE", "0.02") or 0.02),
            min_confidence=float(os.environ.get("ARBITRAGE_MIN_CONFIDENCE", "0.30") or 0.30),
            kalshi_fee_rate=float(os.environ.get("KALSHI_FEE_RATE", "0.07") or 0.07),
            require_net_profit=os.environ.get("ARBITRAGE_REQUIRE_NET_PROFIT", "true").lower()
            in ("1", "true", "yes"),
        )


@dataclass
class LegPlan:
    platform: str
    market_id: str
    action: str          # "buy"
    side: str            # "yes" | "no"
    price: float         # dollars in [0,1]
    count: int           # contracts
    cost: float          # price * count


@dataclass
class ExecResult:
    ok: bool
    mode: str
    status: str                                  # planned|filled|rejected|partial_unwound|error
    reasons: List[str] = field(default_factory=list)
    plan: Dict[str, Any] = field(default_factory=dict)
    legs: List[Dict[str, Any]] = field(default_factory=list)
    record: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "mode": self.mode,
            "status": self.status,
            "reasons": self.reasons,
            "plan": self.plan,
            "legs": self.legs,
            "record": self.record,
        }


class ArbitrageExecutor:
    def __init__(self, client=None, config: Optional[ExecConfig] = None):
        self.client = client
        self.config = config or ExecConfig.from_env()
        self._pm_exec = None

    # ── venue capability ────────────────────────────────────────────
    def _kalshi(self):
        return getattr(self.client, "_kalshi", None) if self.client else None

    def _polymarket_exec(self):
        if self._pm_exec is None:
            try:
                from event_markets.polymarket_executor import PolymarketExecutionClient
                data = getattr(self.client, "_polymarket", None) if self.client else None
                self._pm_exec = PolymarketExecutionClient(data_client=data)
            except Exception as e:
                _log("debug", "polymarket exec init failed: %s", e)
                self._pm_exec = None
        return self._pm_exec

    def venue_live_ready(self, platform: str) -> tuple[bool, str]:
        platform = (platform or "").lower()
        if platform == "kalshi":
            kc = self._kalshi()
            if kc is None:
                return False, "Kalshi client unavailable"
            if not (getattr(kc, "api_key_id", "") and getattr(kc, "private_key_path", "")):
                return False, "Kalshi not authenticated (missing api key / private key)"
            return True, ""
        if platform == "polymarket":
            pm = self._polymarket_exec()
            if pm is None:
                return False, "Polymarket execution client unavailable"
            ok, why = pm.is_configured()
            return ok, ("" if ok else why)
        return False, f"Unknown venue '{platform}'"

    def kill_switch_active(self) -> bool:
        return KILL_SWITCH_PATH.exists()

    # ── fees ────────────────────────────────────────────────────────
    def _leg_fee(self, leg: LegPlan) -> float:
        """Estimated trading fee (USD) for a leg. Kalshi charges
        ceil(rate * C * P * (1-P)); Polymarket currently charges 0%."""
        if leg.platform.lower() != "kalshi":
            return 0.0
        p = max(0.0, min(1.0, float(leg.price)))
        raw = self.config.kalshi_fee_rate * leg.count * p * (1.0 - p)
        # rounded up to the next cent (guard against fp noise before ceil)
        cents = math.ceil(round(raw * 100.0, 6))
        return cents / 100.0

    def estimate_fees(self, buy_leg: LegPlan, hedge_leg: LegPlan) -> float:
        return round(self._leg_fee(buy_leg) + self._leg_fee(hedge_leg), 2)

    # ── planning ────────────────────────────────────────────────────
    def build_plan(self, opp: Dict[str, Any], notional_usd: float) -> tuple[Optional[LegPlan], Optional[LegPlan], float]:
        """Return (buy_leg, hedge_leg, contracts). Buy YES on platform_buy,
        buy NO on platform_hedge. Sized so total capital ~= notional."""
        buy_price = float(opp.get("buy_yes_price", 0.0))
        hedge_yes = float(opp.get("hedge_yes_price", 0.0))
        hedge_price = max(0.0, 1.0 - hedge_yes)      # buying NO == 1 - yes
        total_cost = float(opp.get("total_cost") or (buy_price + hedge_price)) or 0.0
        if total_cost <= 0:
            return None, None, 0
        contracts = max(1, int(math.floor(notional_usd / total_cost)))
        buy_leg = LegPlan(
            platform=opp.get("platform_buy", ""),
            market_id=opp.get("buy_market_id", ""),
            action="buy", side="yes", price=round(buy_price, 4),
            count=contracts, cost=round(buy_price * contracts, 2),
        )
        hedge_leg = LegPlan(
            platform=opp.get("platform_hedge", ""),
            market_id=opp.get("hedge_market_id", ""),
            action="buy", side="no", price=round(hedge_price, 4),
            count=contracts, cost=round(hedge_price * contracts, 2),
        )
        return buy_leg, hedge_leg, contracts

    def preflight(self, opp: Dict[str, Any], notional_usd: float, want_live: bool) -> ExecResult:
        cfg = self.config
        reasons: List[str] = []
        notional_usd = min(float(notional_usd or 0), cfg.max_notional_usd)
        if notional_usd <= 0:
            reasons.append("notional must be > 0")

        edge_pct = float(opp.get("edge_pct", 0.0))
        conf = float(opp.get("confidence", 0.0))
        if edge_pct < cfg.min_edge_pct:
            reasons.append(f"edge {edge_pct*100:.2f}% < min {cfg.min_edge_pct*100:.2f}%")
        if conf < cfg.min_confidence:
            reasons.append(f"confidence {conf:.0%} < min {cfg.min_confidence:.0%}")

        buy_leg, hedge_leg, contracts = self.build_plan(opp, notional_usd)
        if not buy_leg or not hedge_leg:
            reasons.append("could not build a valid leg plan (bad prices)")
            return ExecResult(ok=False, mode=cfg.mode, status="rejected", reasons=reasons)

        if not buy_leg.market_id or not hedge_leg.market_id:
            reasons.append("missing market_id on one or both legs")

        plan = {
            "notional_usd": round(notional_usd, 2),
            "contracts": contracts,
            "total_cost": round(buy_leg.price + hedge_leg.price, 4),
            "expected_profit": round(contracts * (1.0 - (buy_leg.price + hedge_leg.price)), 2),
            "buy_leg": buy_leg.__dict__,
            "hedge_leg": hedge_leg.__dict__,
        }
        gross = plan["expected_profit"]
        fees = self.estimate_fees(buy_leg, hedge_leg)
        net = round(gross - fees, 2)
        plan["estimated_fees"] = fees
        plan["net_expected_profit"] = net
        if cfg.require_net_profit and net <= 0:
            reasons.append(
                f"unprofitable after fees: gross ${gross:.2f} - fees ${fees:.2f} = ${net:.2f}"
            )

        # Live-mode gating (only enforced when the caller actually wants live).
        live_ok = True
        if want_live:
            if cfg.mode != "live":
                live_ok = False
                reasons.append("live disabled (set ARBITRAGE_LIVE_ENABLED=true)")
            if self.kill_switch_active():
                live_ok = False
                reasons.append("kill-switch active (data/arbitrage-kill-switch present)")
            for leg in (buy_leg, hedge_leg):
                ready, why = self.venue_live_ready(leg.platform)
                if not ready:
                    live_ok = False
                    reasons.append(f"{leg.platform} leg not live-ready: {why}")
            # Balance check for legs we can actually price live (Kalshi).
            self._check_balances(buy_leg, hedge_leg, reasons)
            # Slippage vs book depth (best-effort).
            self._check_slippage(opp, buy_leg, hedge_leg, reasons)

        ok = (not reasons) if not want_live else (live_ok and not reasons)
        status = "planned" if ok else "rejected"
        return ExecResult(ok=ok, mode=cfg.mode, status=status, reasons=reasons, plan=plan)

    def _check_balances(self, buy_leg: LegPlan, hedge_leg: LegPlan, reasons: List[str]) -> None:
        kc = self._kalshi()
        # Kalshi
        if kc is not None:
            try:
                bal = kc.get_balance() or {}
                usd = None
                if bal.get("balance_dollars") is not None:
                    usd = float(bal["balance_dollars"])
                elif bal.get("balance") is not None:
                    usd = float(bal["balance"]) / 100.0
                if usd is not None:
                    need = sum(leg.cost for leg in (buy_leg, hedge_leg) if leg.platform.lower() == "kalshi")
                    if need > usd:
                        reasons.append(f"insufficient Kalshi balance: need ${need:.2f}, have ${usd:.2f}")
            except Exception as e:
                _log("debug", "kalshi balance check failed: %s", e)
        # Polymarket
        need_pm = sum(leg.cost for leg in (buy_leg, hedge_leg) if leg.platform.lower() == "polymarket")
        if need_pm > 0:
            pm = self._polymarket_exec()
            if pm is not None:
                try:
                    usdc = pm.get_usdc_balance()
                    if usdc is not None and need_pm > usdc:
                        reasons.append(f"insufficient Polymarket USDC: need ${need_pm:.2f}, have ${usdc:.2f}")
                except Exception as e:
                    _log("debug", "polymarket balance check failed: %s", e)

    def _check_slippage(self, opp: Dict[str, Any], buy_leg: LegPlan, hedge_leg: LegPlan, reasons: List[str]) -> None:
        cfg = self.config
        # depth_buy / depth_hedge summaries (Kalshi) carry yes_ask; a YES buy pays
        # the ask, a NO buy pays 1 - yes_bid.
        db = opp.get("depth_buy") or {}
        dh = opp.get("depth_hedge") or {}
        if buy_leg.platform.lower() == "kalshi" and db.get("yes_ask") is not None:
            if db["yes_ask"] > buy_leg.price + cfg.max_slippage:
                reasons.append(f"buy-leg slippage: book ask {db['yes_ask']:.2f} > {buy_leg.price:.2f}+{cfg.max_slippage:.2f}")
        if hedge_leg.platform.lower() == "kalshi" and dh.get("yes_bid") is not None:
            no_ask = round(1.0 - dh["yes_bid"], 4)
            if no_ask > hedge_leg.price + cfg.max_slippage:
                reasons.append(f"hedge-leg slippage: NO ask {no_ask:.2f} > {hedge_leg.price:.2f}+{cfg.max_slippage:.2f}")

    # ── execution ───────────────────────────────────────────────────
    def execute(self, opp: Dict[str, Any], notional_usd: float, want_live: bool = False) -> ExecResult:
        pf = self.preflight(opp, notional_usd, want_live)
        if not pf.ok:
            return pf

        if not want_live or self.config.mode != "live":
            # DRY RUN: record intended trade, place nothing.
            record = self._record(opp, pf.plan, mode="dry_run", status="open", legs=[])
            return ExecResult(ok=True, mode="dry_run", status="planned",
                              reasons=["dry-run: no real orders placed"],
                              plan=pf.plan, record=record)

        # LIVE: place buy leg first, confirm, then hedge; unwind on failure.
        buy = LegPlan(**pf.plan["buy_leg"])
        hedge = LegPlan(**pf.plan["hedge_leg"])
        legs_out: List[Dict[str, Any]] = []

        try:
            r1 = self._place_leg(buy)
            legs_out.append(r1)
        except Exception as e:
            return ExecResult(ok=False, mode="live", status="error",
                              reasons=[f"buy leg failed to place: {e}"], plan=pf.plan, legs=legs_out)

        if not r1.get("filled"):
            return ExecResult(ok=False, mode="live", status="rejected",
                              reasons=["buy leg did not fill (FOK); no hedge placed"],
                              plan=pf.plan, legs=legs_out)

        try:
            r2 = self._place_leg(hedge)
            legs_out.append(r2)
        except Exception as e:
            unwind = self._unwind(buy, filled=r1.get("fill_count", buy.count))
            legs_out.append(unwind)
            return ExecResult(ok=False, mode="live", status="partial_unwound",
                              reasons=[f"hedge leg failed: {e}; unwound buy leg"],
                              plan=pf.plan, legs=legs_out)

        if not r2.get("filled"):
            unwind = self._unwind(buy, filled=r1.get("fill_count", buy.count))
            legs_out.append(unwind)
            return ExecResult(ok=False, mode="live", status="partial_unwound",
                              reasons=["hedge leg did not fill; unwound buy leg"],
                              plan=pf.plan, legs=legs_out)

        record = self._record(opp, pf.plan, mode="live", status="live_open", legs=legs_out)
        return ExecResult(ok=True, mode="live", status="filled",
                          reasons=["both legs filled"], plan=pf.plan, legs=legs_out, record=record)

    def _place_leg(self, leg: LegPlan) -> Dict[str, Any]:
        platform = leg.platform.lower()
        if platform == "kalshi":
            kc = self._kalshi()
            # fill_or_kill (all-or-nothing): an IOC could partially fill, leaving a
            # naked, un-hedged position that the caller would mis-read as unfilled
            # and never unwind. FOK guarantees fill_count is 0 or the full count.
            resp = kc.create_order(
                ticker=leg.market_id, side=leg.side, action=leg.action,
                count=leg.count, price=leg.price, time_in_force="fill_or_kill",
            )
            fill = float(resp.get("fill_count", 0) or 0)
            return {
                "platform": "kalshi", "market_id": leg.market_id, "side": leg.side,
                "action": leg.action, "price": leg.price, "count": leg.count,
                "order_id": resp.get("order_id"), "fill_count": fill,
                "filled": fill >= leg.count, "raw": resp,
            }
        if platform == "polymarket":
            pm = self._polymarket_exec()
            if pm is None:
                raise NotImplementedError("polymarket execution client unavailable")
            # FOK (fill-or-kill) matches the Kalshi legs: all-or-nothing, no resting leg risk.
            resp = pm.place_order(
                market_id=leg.market_id, side=leg.action, price=leg.price,
                size=leg.count, outcome=leg.side, order_type="FOK",
            )
            raw = resp if isinstance(resp, dict) else {"raw": resp}
            success = bool(raw.get("success", True)) and not raw.get("error")
            # Polymarket returns orderID/status; treat a placed FOK as filled.
            return {
                "platform": "polymarket", "market_id": leg.market_id, "side": leg.side,
                "action": leg.action, "price": leg.price, "count": leg.count,
                "order_id": raw.get("orderID") or raw.get("order_id"),
                "fill_count": leg.count if success else 0,
                "filled": success, "raw": raw,
            }
        # Others: not implemented (preflight blocks this in live mode).
        raise NotImplementedError(f"{platform} live execution not implemented")

    def _unwind(self, leg: LegPlan, filled: float) -> Dict[str, Any]:
        """Best-effort close of a filled buy leg by selling it back (IOC)."""
        platform = leg.platform.lower()
        try:
            if platform == "kalshi" and filled:
                kc = self._kalshi()
                resp = kc.create_order(
                    ticker=leg.market_id, side=leg.side, action="sell",
                    count=int(filled), price=max(0.01, leg.price - 0.05),
                    time_in_force="immediate_or_cancel",
                )
                return {"platform": platform, "unwind": True, "order_id": resp.get("order_id"),
                        "fill_count": float(resp.get("fill_count", 0) or 0), "raw": resp}
            if platform == "polymarket" and filled:
                pm = self._polymarket_exec()
                resp = pm.place_order(
                    market_id=leg.market_id, side="sell", price=max(0.01, leg.price - 0.05),
                    size=filled, outcome=leg.side, order_type="FOK",
                )
                raw = resp if isinstance(resp, dict) else {"raw": resp}
                return {"platform": platform, "unwind": True,
                        "order_id": raw.get("orderID") or raw.get("order_id"), "raw": raw}
        except Exception as e:
            return {"platform": platform, "unwind": True, "error": str(e),
                    "warning": "UNWIND FAILED — manual intervention may be required"}
        return {"platform": platform, "unwind": True, "note": "nothing to unwind"}

    # ── Kalshi internal (single-venue, N-leg) arbitrage ──────────────
    def preflight_internal(self, opp: Dict[str, Any], notional_usd: float,
                           want_live: bool) -> ExecResult:
        cfg = self.config
        reasons: List[str] = []
        notional_usd = min(float(notional_usd or 0), cfg.max_notional_usd)
        if notional_usd <= 0:
            reasons.append("notional must be > 0")

        strategy = str(opp.get("strategy", ""))
        guaranteed = bool(opp.get("guaranteed", False))
        raw_legs = opp.get("legs") or []
        total_cost = float(opp.get("total_cost") or sum(float(l.get("price", 0)) for l in raw_legs))
        if not raw_legs or total_cost <= 0:
            reasons.append("no legs / invalid cost")
            return ExecResult(ok=False, mode=cfg.mode, status="rejected", reasons=reasons)

        edge_pct = float(opp.get("edge_pct", 0.0))
        conf = float(opp.get("confidence", 0.0))
        if edge_pct < cfg.min_edge_pct:
            reasons.append(f"edge {edge_pct*100:.2f}% < min {cfg.min_edge_pct*100:.2f}%")
        if conf < cfg.min_confidence:
            reasons.append(f"confidence {conf:.0%} < min {cfg.min_confidence:.0%}")

        contracts = max(1, int(math.floor(notional_usd / total_cost)))
        n = int(opp.get("n_outcomes", len(raw_legs)))
        # worst-case payout per contract-set: (n-1) for a NO-side mutex lock,
        # 1 for a YES-side (exhaustive-conditional) lock.
        payout_units = (n - 1) if strategy == "mutex_no" else 1

        leg_plans: List[LegPlan] = []
        for l in raw_legs:
            price = round(float(l.get("price", 0.0)), 4)
            leg_plans.append(LegPlan(
                platform="kalshi", market_id=str(l.get("ticker", "")),
                action="buy", side=str(l.get("side", "no")),
                price=price, count=contracts, cost=round(price * contracts, 2),
            ))
        if any(not lp.market_id for lp in leg_plans):
            reasons.append("missing ticker on one or more legs")

        gross = round(contracts * payout_units - total_cost * contracts, 2)
        fees = round(sum(self._leg_fee(lp) for lp in leg_plans), 2)
        net = round(gross - fees, 2)
        plan = {
            "notional_usd": round(notional_usd, 2),
            "contracts": contracts,
            "total_cost": round(total_cost, 4),
            "payout_units": payout_units,
            "n_outcomes": n,
            "strategy": strategy,
            "guaranteed": guaranteed,
            "expected_profit": gross,
            "estimated_fees": fees,
            "net_expected_profit": net,
            "legs": [lp.__dict__ for lp in leg_plans],
        }
        if cfg.require_net_profit and net <= 0:
            reasons.append(f"unprofitable after fees: gross ${gross:.2f} - fees ${fees:.2f} = ${net:.2f}")

        live_ok = True
        if want_live:
            if cfg.mode != "live":
                live_ok = False
                reasons.append("live disabled (set ARBITRAGE_LIVE_ENABLED=true)")
            if not guaranteed:
                live_ok = False
                reasons.append("live blocked: internal-arb requires a GUARANTEED (mutex_no) lock; "
                               "mutex_yes depends on unverified collective exhaustiveness")
            if self.kill_switch_active():
                live_ok = False
                reasons.append("kill-switch active (data/arbitrage-kill-switch present)")
            ready, why = self.venue_live_ready("kalshi")
            if not ready:
                live_ok = False
                reasons.append(f"kalshi not live-ready: {why}")
            # Balance: need full basket cost + fees.
            kc = self._kalshi()
            if kc is not None:
                try:
                    bal = kc.get_balance() or {}
                    usd = float(bal["balance_dollars"]) if bal.get("balance_dollars") is not None \
                        else (float(bal.get("balance", 0)) / 100.0)
                    need = round(total_cost * contracts + fees, 2)
                    if need > usd:
                        reasons.append(f"insufficient Kalshi balance: need ${need:.2f}, have ${usd:.2f}")
                except Exception as e:
                    _log("debug", "internal balance check failed: %s", e)

        ok = (not reasons) if not want_live else (live_ok and not reasons)
        return ExecResult(ok=ok, mode=cfg.mode, status=("planned" if ok else "rejected"),
                          reasons=reasons, plan=plan)

    def execute_internal(self, opp: Dict[str, Any], notional_usd: float,
                         want_live: bool = False) -> ExecResult:
        pf = self.preflight_internal(opp, notional_usd, want_live)
        if not pf.ok:
            return pf

        if not want_live or self.config.mode != "live":
            record = self._record_internal(opp, pf.plan, mode="dry_run", status="open", legs=[])
            return ExecResult(ok=True, mode="dry_run", status="planned",
                              reasons=["dry-run: no real orders placed"],
                              plan=pf.plan, record=record)

        # LIVE: place every leg FOK; if any leg fails, unwind all filled legs.
        legs = [LegPlan(**l) for l in pf.plan["legs"]]
        legs_out: List[Dict[str, Any]] = []
        filled: List[LegPlan] = []
        for leg in legs:
            try:
                r = self._place_leg(leg)
            except Exception as e:
                legs_out.append({"platform": "kalshi", "market_id": leg.market_id, "error": str(e)})
                unwinds = [self._unwind(fl, filled=fl.count) for fl in filled]
                return ExecResult(ok=False, mode="live", status="partial_unwound",
                                  reasons=[f"leg {leg.market_id} failed to place: {e}; unwound {len(filled)} filled legs"],
                                  plan=pf.plan, legs=legs_out + unwinds)
            legs_out.append(r)
            if r.get("filled"):
                filled.append(leg)
            else:
                unwinds = [self._unwind(fl, filled=fl.count) for fl in filled]
                return ExecResult(ok=False, mode="live", status="partial_unwound",
                                  reasons=[f"leg {leg.market_id} did not fill (FOK); unwound {len(filled)} filled legs"],
                                  plan=pf.plan, legs=legs_out + unwinds)

        record = self._record_internal(opp, pf.plan, mode="live", status="live_open", legs=legs_out)
        return ExecResult(ok=True, mode="live", status="filled",
                          reasons=[f"all {len(legs)} legs filled"], plan=pf.plan,
                          legs=legs_out, record=record)

    def _record_internal(self, opp: Dict[str, Any], plan: Dict[str, Any], mode: str,
                         status: str, legs: List[Dict[str, Any]]) -> Dict[str, Any]:
        trades = self._load()
        rec = {
            "id": str(uuid.uuid4()),
            "event_key": opp.get("event_ticker", opp.get("event_key", "")),
            "event_title": opp.get("event_title", ""),
            "category": opp.get("category", ""),
            "type": "kalshi_internal",
            "strategy": plan.get("strategy", ""),
            "guaranteed": plan.get("guaranteed", False),
            "source": "executor",
            "mode": mode,
            "n_outcomes": plan.get("n_outcomes"),
            "payout_units": plan.get("payout_units"),
            "total_cost": plan["total_cost"],
            "edge_pct": float(opp.get("edge_pct", 0.0)),
            "confidence": float(opp.get("confidence", 0.0)),
            "notional": plan["notional_usd"],
            "contracts": plan["contracts"],
            "expected_profit": plan["expected_profit"],
            "estimated_fees": plan.get("estimated_fees", 0.0),
            "net_expected_profit": plan.get("net_expected_profit", plan["expected_profit"]),
            "legs": legs or plan.get("legs", []),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": status,
        }
        trades.append(rec)
        self._save(trades)
        return rec

    # ── persistence ─────────────────────────────────────────────────
    def _record(self, opp: Dict[str, Any], plan: Dict[str, Any], mode: str,
                status: str, legs: List[Dict[str, Any]]) -> Dict[str, Any]:
        trades = self._load()
        rec = {
            "id": str(uuid.uuid4()),
            "event_key": opp.get("event_key", ""),
            "category": opp.get("category", ""),
            "type": "arbitrage",
            "source": "executor",
            "mode": mode,
            "platform_buy": opp.get("platform_buy", ""),
            "platform_hedge": opp.get("platform_hedge", ""),
            "buy_market_id": opp.get("buy_market_id", ""),
            "hedge_market_id": opp.get("hedge_market_id", ""),
            "buy_yes_price": plan["buy_leg"]["price"],
            "hedge_yes_price": opp.get("hedge_yes_price", 0.0),
            "total_cost": plan["total_cost"],
            "guaranteed_payout": 1.0,
            "edge": float(opp.get("edge", 0.0)),
            "edge_pct": float(opp.get("edge_pct", 0.0)),
            "confidence": float(opp.get("confidence", 0.0)),
            "notional": plan["notional_usd"],
            "contracts": plan["contracts"],
            "expected_profit": plan["expected_profit"],
            "estimated_fees": plan.get("estimated_fees", 0.0),
            "net_expected_profit": plan.get("net_expected_profit", plan["expected_profit"]),
            "legs": legs,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": status,
        }
        trades.append(rec)
        self._save(trades)
        return rec

    @staticmethod
    def _load() -> List[Dict[str, Any]]:
        if PAPER_TRADES_PATH.exists():
            try:
                return json.loads(PAPER_TRADES_PATH.read_text())
            except Exception:
                return []
        return []

    @staticmethod
    def _save(trades: List[Dict[str, Any]]) -> None:
        PAPER_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = PAPER_TRADES_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(trades, indent=2))
        tmp.replace(PAPER_TRADES_PATH)

    def status(self) -> Dict[str, Any]:
        cfg = self.config
        buy_ready, buy_why = self.venue_live_ready("kalshi")
        pm_ready, pm_why = self.venue_live_ready("polymarket")
        return {
            "mode": cfg.mode,
            "live_enabled": cfg.mode == "live",
            "kill_switch": self.kill_switch_active(),
            "max_notional_usd": cfg.max_notional_usd,
            "min_edge_pct": cfg.min_edge_pct,
            "max_slippage": cfg.max_slippage,
            "min_confidence": cfg.min_confidence,
            "kalshi_fee_rate": cfg.kalshi_fee_rate,
            "require_net_profit": cfg.require_net_profit,
            "venues": {
                "kalshi": {"live_ready": buy_ready, "reason": buy_why},
                "polymarket": {"live_ready": pm_ready, "reason": pm_why},
            },
        }
