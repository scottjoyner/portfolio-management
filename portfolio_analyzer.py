#!/usr/bin/env python3
"""
Portfolio Analyzer – fetches live Coinbase holdings, analyzes allocation,
identifies tax-loss harvesting candidates, scores strategy fit against the
USDC benchmark (3.5% APY), and recommends fee-tier optimizations.

Usage:
    python3 portfolio_analyzer.py              # Human-readable report
    python3 portfolio_analyzer.py --json       # Machine-readable JSON
    python3 portfolio_analyzer.py --rebalance  # Include rebalancing suggestions
"""

import argparse
import json
import logging
import math
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USDC_BENCHMARK_APY = 3.5  # weekly-compounded USDC yield on Coinbase

SAFE_ASSETS = {"BTC", "USDC", "USDT", "DAI", "ETH"}
GROWTH_ASSETS = {"SOL", "LINK", "MATIC", "AVAX", "DOT", "ADA", "ATOM", "UNI"}
# Everything else → speculative

COINBASE_FEE_TIERS: List[Tuple[float, float, float]] = [
    (0, 0.0060, 0.0120),
    (1_000, 0.0035, 0.0075),
    (10_000, 0.0025, 0.0040),
    (50_000, 0.0015, 0.0025),
    (100_000, 0.0010, 0.0020),
    (1_000_000, 0.0008, 0.0018),
    (20_000_000, 0.0005, 0.0015),
]

TARGET_ALLOCATION = {
    "safe": 0.80,
    "growth": 0.15,
    "speculative": 0.05,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val))
    except (ValueError, TypeError):
        return 0.0


def asset_classification(currency: str) -> str:
    c = currency.upper().replace("-USD", "").replace("USDC", "USDC")
    if c in SAFE_ASSETS:
        return "safe"
    if c in GROWTH_ASSETS:
        return "growth"
    return "speculative"


def get_current_tier(volume_30d: float) -> Tuple[float, float, float]:
    for min_vol, maker, taker in reversed(COINBASE_FEE_TIERS):
        if volume_30d >= min_vol:
            return (min_vol, maker, taker)
    return COINBASE_FEE_TIERS[0]


def volume_to_next_tier(volume_30d: float) -> float:
    current_min = 0
    for min_vol, _maker, _taker in COINBASE_FEE_TIERS:
        if volume_30d >= min_vol:
            current_min = min_vol
        elif min_vol > current_min:
            return max(0.0, min_vol - volume_30d)
    return 0.0


# ---------------------------------------------------------------------------
# Holding dataclass
# ---------------------------------------------------------------------------

@dataclass
class Holding:
    currency: str
    balance: float
    held: float
    price_usd: float
    value_usd: float
    classification: str
    allocation_pct: float = 0.0
    change_24h_pct: Optional[float] = None
    volume_24h: Optional[float] = None
    spread_pct: Optional[float] = None
    cost_basis_usd: Optional[float] = None
    unrealized_pnl_usd: Optional[float] = None
    unrealized_pnl_pct: Optional[float] = None
    tradability_score: Optional[float] = None
    strategy_return_estimate: Optional[float] = None
    beats_benchmark: Optional[bool] = None
    recommendation: str = "hold"

    @property
    def total(self) -> float:
        return self.balance + self.held


# ---------------------------------------------------------------------------
# Portfolio Analyzer
# ---------------------------------------------------------------------------

class PortfolioAnalyzer:
    """Full portfolio analysis against a live Coinbase account."""

    def __init__(self, environment: str = "live", timeout: int = 30, min_value: float = 10.0):
        self.environment = environment
        self.timeout = timeout
        self.min_value = min_value
        self.holdings: List[Holding] = []
        self.total_value_usd: float = 0.0
        self.fee_volume_30d: float = 0.0
        self.fee_tier: Tuple[float, float, float] = (0, 0.006, 0.012)
        self.volume_to_next: float = 0.0
        self._connector = None

    # ── CLI wrapper ──────────────────────────────────────────────────

    def _cli(self, cmd: List[str], parse_json: bool = True):
        import subprocess
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=self.timeout
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr or result.stdout)
            if parse_json:
                return json.loads(result.stdout)
            return result.stdout.strip()
        except FileNotFoundError:
            raise RuntimeError(
                "Coinbase CLI not found. Install: npm install -g @coinbase/coinbase-cli"
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Command timed out after {self.timeout}s")

    def _env_flag(self) -> List[str]:
        return ["-e", self.environment]

    # ── Data fetching ────────────────────────────────────────────────

    def fetch_balances(self) -> List[dict]:
        raw = self._cli(["coinbase", "balance"] + self._env_flag())
        accounts = raw if isinstance(raw, list) else raw.get("accounts", [])
        return accounts

    def fetch_price(self, product_id: str) -> dict:
        try:
            return self._cli(
                ["coinbase", "products", "get", product_id] + self._env_flag()
            )
        except RuntimeError:
            return {}

    def fetch_fees(self) -> dict:
        return self._cli(["coinbase", "fees"] + self._env_flag())

    def fetch_fills(self, product_id: Optional[str] = None) -> List[dict]:
        cmd = ["coinbase", "orders", "fills"] + self._env_flag()
        if product_id:
            cmd.insert(3, f"product_id=={product_id}")
        result = self._cli(cmd)
        if isinstance(result, dict):
            return result.get("fills", [])
        return result if isinstance(result, list) else [result]

    def fetch_candles(self, product_id: str, granularity: str = "1d", limit: int = 30) -> List[list]:
        try:
            result = self._cli(
                [
                    "coinbase", "products", "candles", product_id,
                    f"granularity=={granularity}",
                ] + self._env_flag()
            )
            return result if isinstance(result, list) else [result]
        except RuntimeError:
            return []

    # ── Cost basis estimation ────────────────────────────────────────

    def estimate_all_cost_bases(self) -> Dict[str, float]:
        """Fetch all fills and compute average cost basis per currency."""
        all_fills = self.fetch_fills()
        buys: Dict[str, Tuple[float, float]] = {}
        for fill in all_fills:
            product = fill.get("product_id", "")
            currency = product.replace("-USD", "") if product else fill.get("currency", "")
            side = fill.get("side", "").upper()
            size = _to_float(fill.get("size", 0))
            price = _to_float(fill.get("price", 0))
            if size <= 0 or price <= 0:
                continue
            total_cost, total_size = buys.get(currency, (0.0, 0.0))
            if side == "BUY":
                buys[currency] = (total_cost + size * price, total_size + size)
            elif side == "SELL" and total_size > 0:
                avg = total_cost / total_size
                sell_cost = min(total_cost, size * avg)
                buys[currency] = (total_cost - sell_cost, max(0.0, total_size - size))
        return {cur: cost / size for cur, (cost, size) in buys.items() if size > 0}

    # ── Tradability & strategy estimate ─────────────────────────────

    def _compute_tradability(self, h: Holding, candles: List[list]) -> float:
        """Score 0-1 how tradable this asset is (liquidity, volatility, spread)."""
        if h.classification == "safe" and h.currency.upper() in ("USDC", "USDT", "DAI"):
            return 0.0
        score = 0.5
        if h.spread_pct is not None and h.spread_pct > 0:
            if h.spread_pct < 0.002:
                score += 0.2
            elif h.spread_pct < 0.005:
                score += 0.1
            else:
                score -= 0.1
        if h.volume_24h is not None and h.volume_24h > 0:
            if h.volume_24h > 10_000_000:
                score += 0.15
            elif h.volume_24h > 1_000_000:
                score += 0.05
        if len(candles) >= 5:
            closes = [c[4] for c in candles if isinstance(c, (list, tuple)) and len(c) >= 5]
            if len(closes) >= 5:
                returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes))]
                avg_return = sum(returns) / len(returns)
                vol = math.sqrt(sum((r - avg_return) ** 2 for r in returns) / len(returns))
                annualized_vol = vol * math.sqrt(365)
                if 0.2 <= annualized_vol <= 1.5:
                    score += 0.15
                elif annualized_vol > 1.5:
                    score += 0.05
                else:
                    score -= 0.1
        return max(0.0, min(1.0, score))

    def _strategy_estimate(self, h: Holding, tradability: float) -> Tuple[float, bool]:
        """Estimate expected return from active strategy vs 3.5% APY benchmark.
        
        Returns (estimated_apy, beats_benchmark).
        """
        if h.currency.upper() in ("USDC",):
            return (USDC_BENCHMARK_APY, True)
        if h.currency.upper() in ("USDT", "DAI"):
            return (USDC_BENCHMARK_APY * 0.8, False)
        if tradability < 0.3:
            base = USDC_BENCHMARK_APY * 0.5
            return (base, False)
        if h.classification == "safe":
            base = USDC_BENCHMARK_APY + (tradability * 5.0)
        elif h.classification == "growth":
            base = USDC_BENCHMARK_APY + (tradability * 8.0)
        else:
            base = USDC_BENCHMARK_APY + (tradability * 12.0)
        return (base, base > USDC_BENCHMARK_APY)

    # ── Main analysis ───────────────────────────────────────────────

    def analyze(self, fetch_candles: bool = False) -> dict:
        raw_accounts = self.fetch_balances()
        fees_data = self.fetch_fees()

        self.fee_volume_30d = _to_float(fees_data.get("advanced_trade_only_volume", 0))
        self.fee_tier = get_current_tier(self.fee_volume_30d)
        self.volume_to_next = volume_to_next_tier(self.fee_volume_30d)

        self.holdings = []
        for acct in raw_accounts:
            currency = acct.get("currency", "")
            available = _to_float(acct.get("available_balance", {}).get("value", 0))
            held = _to_float(acct.get("hold", {}).get("value", 0))
            total = available + held
            if total <= 0:
                continue
            product_id = f"{currency}-USD"
            price_info = self.fetch_price(product_id)
            price_usd = _to_float(price_info.get("price", 0))
            if price_usd == 0 and currency == "USDC":
                price_usd = 1.0
            value_usd = total * price_usd
            classification = asset_classification(currency)

            h = Holding(
                currency=currency,
                balance=available,
                held=held,
                price_usd=price_usd,
                value_usd=value_usd,
                classification=classification,
                change_24h_pct=_to_float(price_info.get("price_percentage_change_24h")),
                volume_24h=_to_float(price_info.get("volume_24h")),
            )

            candles = []
            if fetch_candles:
                candles = self.fetch_candles(product_id)
            h.tradability_score = self._compute_tradability(h, candles)
            h.strategy_return_estimate, h.beats_benchmark = self._strategy_estimate(
                h, h.tradability_score
            )
            if h.currency.upper() in ("USDC", "USDT", "DAI"):
                h.recommendation = "hold"
            elif h.beats_benchmark:
                h.recommendation = "active_trade"
            elif h.classification == "safe":
                h.recommendation = "hold"
            else:
                h.recommendation = "consider_sell"

            self.holdings.append(h)

        self.total_value_usd = sum(h.value_usd for h in self.holdings)
        for h in self.holdings:
            if self.total_value_usd > 0:
                h.allocation_pct = (h.value_usd / self.total_value_usd) * 100

        return self._build_report()

    def run_tax_loss_harvesting(self) -> List[dict]:
        """Identify positions with unrealized losses for TLH."""
        cost_bases = self.estimate_all_cost_bases()
        candidates = []
        for h in self.holdings:
            if h.currency.upper() in ("USDC", "USDT", "DAI"):
                continue
            cost = cost_bases.get(h.currency)
            if cost and cost > 0:
                pnl = (h.price_usd - cost) / cost * 100
                h.cost_basis_usd = cost
                h.unrealized_pnl_usd = (h.price_usd - cost) * h.total
                h.unrealized_pnl_pct = pnl
                if pnl < -5:
                    candidates.append({
                        "currency": h.currency,
                        "balance": h.total,
                        "cost_basis": round(cost, 4),
                        "current_price": round(h.price_usd, 4),
                        "unrealized_loss_pct": round(pnl, 2),
                        "unrealized_loss_usd": round(h.unrealized_pnl_usd, 2),
                        "estimated_tax_savings": round(abs(h.unrealized_pnl_usd) * 0.20, 2),
                        "action": "harvest_now" if pnl < -10 else "watch",
                    })
        return candidates

    def _build_report(self) -> dict:
        by_class: Dict[str, List[Holding]] = defaultdict(list)
        for h in self.holdings:
            by_class[h.classification].append(h)

        current = {c: sum(h.value_usd for h in lst) / self.total_value_usd * 100
                   if self.total_value_usd > 0 else 0
                   for c, lst in by_class.items()}
        rebalance_actions = []
        for cls, target_pct in TARGET_ALLOCATION.items():
            current_pct = current.get(cls, 0)
            diff = current_pct - target_pct * 100
            if abs(diff) > 5:
                rebalance_actions.append({
                    "class": cls,
                    "current_pct": round(current_pct, 1),
                    "target_pct": target_pct * 100,
                    "difference_pct": round(diff, 1),
                    "action": "reduce" if diff > 0 else "increase",
                    "estimated_move_usd": round(abs(diff) / 100 * self.total_value_usd, 2),
                })

        beats = [h for h in self.holdings if h.beats_benchmark]
        misses = [h for h in self.holdings if not h.beats_benchmark]

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": self.environment,
            "total_value_usd": round(self.total_value_usd, 2),
            "holding_count": len(self.holdings),
            "holdings": [asdict(h) for h in self.holdings],
            "allocation": {
                "by_class": {c: round(pct, 1) for c, pct in current.items()},
                "target": {c: p * 100 for c, p in TARGET_ALLOCATION.items()},
                "rebalance_actions": rebalance_actions,
            },
            "fee_tier": {
                "volume_30d": round(self.fee_volume_30d, 2),
                "current_min_volume": self.fee_tier[0],
                "maker_rate": self.fee_tier[1],
                "taker_rate": self.fee_tier[2],
                "volume_to_next_tier": round(self.volume_to_next, 2),
                "next_maker_rate": None,
                "next_taker_rate": None,
            },
            "strategy_fit": {
                "benchmark_apy": USDC_BENCHMARK_APY,
                "beats_benchmark": [
                    {"currency": h.currency, "estimated_apy": round(h.strategy_return_estimate, 2), "tradability": round(h.tradability_score, 3) if h.tradability_score is not None else None}
                    for h in beats
                ],
                "below_benchmark": [
                    {"currency": h.currency, "estimated_apy": round(h.strategy_return_estimate, 2) if h.strategy_return_estimate is not None else None, "tradability": round(h.tradability_score, 3) if h.tradability_score is not None else None}
                    for h in misses
                ],
            },
            "recommendations": self._generate_recommendations(rebalance_actions),
        }

    def _generate_recommendations(self, rebalance_actions: List[dict]) -> List[str]:
        recs = []
        if self.volume_to_next > 0 and self.fee_tier[1] > COINBASE_FEE_TIERS[-1][1]:
            savings = (self.fee_tier[1] - COINBASE_FEE_TIERS[-1][1]) * 100
            recs.append(
                f"Fee tier: increase 30d volume by ${self.volume_to_next:,.0f} to reach "
                f"next tier and save up to {savings:.2f}% on fees."
            )
        for a in rebalance_actions:
            recs.append(
                f"Rebalance: {a['action'].title()} {a['class']} exposure by "
                f"${a['estimated_move_usd']:,.0f} (current {a['current_pct']:.0f}% → "
                f"target {a['target_pct']:.0f}%)."
            )
        meaningful = [h for h in self.holdings if h.value_usd >= self.min_value and h.recommendation != "hold"]
        for h in sorted(meaningful, key=lambda x: x.value_usd, reverse=True):
            action = "sell" if h.recommendation == "consider_sell" else "trade"
            recs.append(
                f"{action.title()} ${h.value_usd:,.0f} {h.currency} – "
                f"{h.strategy_return_estimate:.1f}% APY vs {USDC_BENCHMARK_APY}% benchmark  "
                f"({h.allocation_pct:.1f}% of portfolio)."
            )
        return recs


# ---------------------------------------------------------------------------
# Human-readable printer
# ---------------------------------------------------------------------------

def print_report(report: dict):
    ts = report.get("timestamp", "")[:19].replace("T", " ")
    print(f"\n{'='*72}")
    print(f"  PORTFOLIO ANALYSIS  @  {ts}")
    print(f"{'='*72}")
    print(f"  Total Value:  ${report['total_value_usd']:>12,.2f}")
    print(f"  Holdings:     {report['holding_count']:>12}")
    print()

    # Holdings table
    print(f"  {'Asset':<10} {'Balance':>12} {'Price':>12} {'Value':>12} {'Alloc':>7} {'Class':<12} {'Est.APY':>8} {'Rec':<14}")
    print(f"  {'-'*9} {'-'*11} {'-'*11} {'-'*11} {'-'*6} {'-'*11} {'-'*7} {'-'*13}")
    for h in report["holdings"]:
        est = f"{h['strategy_return_estimate']:.1f}%" if h.get('strategy_return_estimate') else "—"
        rec = h.get('recommendation', 'hold')
        print(f"  {h['currency']:<10} {h['balance']+h['held']:>12.6f} ${h['price_usd']:>9,.4f} "
              f"${h['value_usd']:>9,.2f} {h['allocation_pct']:>6.1f}% "
              f"{h['classification']:<12} {est:>7} {rec:<14}")
    print()

    # Allocation
    alloc = report["allocation"]
    print(f"  {'─'*72}")
    print(f"  ALLOCATION")
    for cls, pct in sorted(alloc["by_class"].items()):
        target = alloc["target"].get(cls, 0)
        bar = "█" * max(1, int(pct / 2))
        print(f"    {cls:<14} {pct:>5.1f}%  (target {target:.0f}%)  {bar}")
    if alloc["rebalance_actions"]:
        print(f"\n  ⚠ Rebalancing needed:")
        for a in alloc["rebalance_actions"]:
            print(f"    {a['action'].title()} {a['class']} by ${a['estimated_move_usd']:>,.0f}")
    print()

    # Fee tier
    ft = report["fee_tier"]
    print(f"  {'─'*72}")
    print(f"  FEE TIER")
    maker_name = f"maker={ft['maker_rate']*100:.2f}%"
    taker_name = f"taker={ft['taker_rate']*100:.2f}%"
    print(f"    30d Volume:  ${ft['volume_30d']:>12,.2f}")
    print(f"    Current:     ${ft['current_min_volume']:>12,.0f}+  {maker_name} / {taker_name}")
    if ft["volume_to_next_tier"] > 0:
        next_tier_vol = ft["current_min_volume"]
        for mv, mk, tk in COINBASE_FEE_TIERS:
            if mv > next_tier_vol:
                print(f"    Next tier:   ${mv:>12,.0f}+  maker={mk*100:.2f}% / taker={tk*100:.2f}%")
                print(f"    Need ${ft['volume_to_next_tier']:>10,.0f} more volume")
                break
    print()

    # Strategy fit
    sf = report["strategy_fit"]
    print(f"  {'─'*72}")
    print(f"  STRATEGY FIT  (benchmark: {sf['benchmark_apy']:.1f}% APY)")
    if sf["beats_benchmark"]:
        print(f"\n    ✅ Beats benchmark:")
        for b in sf["beats_benchmark"]:
            print(f"      {b['currency']:<8}  {b['estimated_apy']:.1f}% APY  (tradability: {b['tradability']:.2f})")
    if sf["below_benchmark"]:
        print(f"\n    ❌ Below benchmark:")
        for b in sf["below_benchmark"]:
            print(f"      {b['currency']:<8}  {b['estimated_apy']:.1f}% APY  (tradability: {b['tradability']:.2f})")
    print()

    # TLH
    print(f"  {'─'*72}")
    print(f"  TAX-LOSS HARVESTING")
    tlh = report.get("tax_loss_harvesting")
    if tlh is not None:
        if tlh:
            for c in tlh:
                print(f"    {c['currency']:<8} loss={c['unrealized_loss_pct']:.1f}%  "
                      f"${c['unrealized_loss_usd']:>7,.2f}  savings ${c['estimated_tax_savings']:>5,.2f}  [{c['action']}]")
            total_savings = sum(c["estimated_tax_savings"] for c in tlh)
            print(f"    Total estimated tax savings: ${total_savings:,.2f}")
        else:
            print(f"    No TLH candidates found (no positions with >5% loss).")
    else:
        print(f"    Run with --tlh to scan for harvesting candidates")
    print()

    # Recommendations
    print(f"  {'─'*72}")
    print(f"  RECOMMENDATIONS")
    for i, rec in enumerate(report["recommendations"], 1):
        print(f"  {i:2d}. {rec}")
    print(f"  {'='*72}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Portfolio Analyzer – Coinbase holdings, TLH, fee tiers, strategy fit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON (default: table)")
    parser.add_argument("--tlh", action="store_true", help="Run tax-loss harvesting scan")
    parser.add_argument("--candles", action="store_true", help="Fetch candle data for volatility analysis")
    parser.add_argument("--min-value", type=float, default=10.0, help="Minimum position value to include in recommendations (default: $10)")
    parser.add_argument("-e", "--environment", default="live", choices=["live", "sandbox"])
    parser.add_argument("--timeout", type=int, default=30, help="CLI timeout in seconds")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")

    try:
        analyzer = PortfolioAnalyzer(environment=args.environment, timeout=args.timeout, min_value=args.min_value)
        report = analyzer.analyze(fetch_candles=args.candles)

        tlh_candidates = []
        if args.tlh:
            tlh_candidates = analyzer.run_tax_loss_harvesting()
        report["tax_loss_harvesting"] = tlh_candidates

        if args.json:
            print(json.dumps(report, indent=2, default=str))
        else:
            print_report(report)

        sys.exit(0)

    except RuntimeError as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏸  Interrupted")
        sys.exit(130)


if __name__ == "__main__":
    main()
