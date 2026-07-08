"""Latency and execution performance model for Coinbase scanning/trading.

This module turns measured environment timings into actionable estimates:
- detection delay
- processing delay
- round-trip order latency
- fill/exit delay
- latency-adjusted priority multipliers

It is intentionally lightweight so it can be used in both the scanner and the
optimizer without pulling in heavy dependencies.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from typing import Iterable, List, Optional


@dataclass
class LatencyProfile:
    """Observed or assumed latency profile for one environment."""

    network_rtt_ms: float = 75.0
    candle_fetch_ms: float = 100.0
    rust_eval_us: float = 75.0
    backtest_us: float = 2.0
    order_preview_ms: float = 140.0
    order_submit_ms: float = 160.0
    order_exit_poll_s: float = 5.0
    spread_bps: float = 10.0
    slippage_bps: float = 5.0


# Average wait time for periodic scan = scan_interval * 1000 / 2 = 500 ms per second
DETECTION_DELAY_MS_PER_SECOND = 500.0


def expected_detection_delay_ms(scan_interval_s: float) -> float:
    """Average delay from a periodic scan interval.
    
    For a scan running every `scan_interval_s` seconds, the average detection
    delay is half the interval (uniform distribution), converted to milliseconds.
    """
    return max(0.0, scan_interval_s * DETECTION_DELAY_MS_PER_SECOND)


def expected_round_trip_ms(profile: LatencyProfile, workers: int = 12, products: int = 1) -> float:
    """Estimate round-trip time for fetch + compute for a universe slice.

    The model assumes candle fetches are parallelized across `workers`.
    """

    workers = max(1, int(workers))
    products = max(1, int(products))
    fetch_batches = math.ceil(products / workers)
    fetch_ms = fetch_batches * profile.candle_fetch_ms
    compute_ms = (products * profile.rust_eval_us + profile.backtest_us) / 1000.0
    return profile.network_rtt_ms + fetch_ms + compute_ms


def expected_fill_delay_ms(
    profile: LatencyProfile,
    liquidity_score: float = 1.0,
    crossing_spread: bool = True,
) -> float:
    """Estimate expected fill delay for a marketable order.

    `liquidity_score` is clipped to [0.1, 1.0]. Lower liquidity increases delay.
    """

    liq = max(0.1, min(1.0, liquidity_score))
    spread_penalty = (profile.spread_bps / 100.0) * (12.0 if crossing_spread else 4.0)
    slippage_penalty = (profile.slippage_bps / 100.0) * (10.0 / liq)
    return profile.order_submit_ms + profile.network_rtt_ms + spread_penalty + slippage_penalty


def expected_exit_delay_ms(profile: LatencyProfile) -> float:
    """Expected average delay to exit when using periodic bracket polling."""

    return profile.network_rtt_ms + expected_detection_delay_ms(profile.order_exit_poll_s)


def strategy_horizon_minutes(strategy_name: str, trade_style: str = "momentum") -> float:
    """Default trade horizon used for latency sensitivity."""

    name = (strategy_name or "").lower()
    style = (trade_style or "momentum").lower()

    if style == "arbitrage" or name in {"arb", "cross_exchange_arb"}:
        return 5.0
    if style == "event":
        return 30.0
    if style == "mean_reversion":
        return 120.0
    if style == "cycle":
        return 240.0
    if style == "tax_loss":
        return 24 * 60.0
    if style == "prediction_market":
        return 240.0
    return 60.0


def latency_priority_multiplier(
    base_priority: float,
    *,
    expected_delay_ms: float,
    horizon_minutes: float,
    sensitivity: float = 1.0,
    floor: float = 0.5,
    ceiling: float = 1.15,
) -> float:
    """Scale a priority by how much expected delay eats into the trade horizon."""

    horizon_ms = max(60_000.0, horizon_minutes * 60_000.0)
    delay_ratio = max(0.0, expected_delay_ms / horizon_ms)
    decay = math.exp(-delay_ratio * max(0.1, sensitivity))
    adjusted = base_priority * decay
    return max(floor, min(ceiling, adjusted))


def latency_tuned_priority(
    base_priority: float,
    *,
    strategy_name: str = "",
    trade_style: str = "momentum",
    expected_delay_ms: float = 0.0,
    sensitivity: float = 1.0,
) -> float:
    """Convenience wrapper for strategy-aware priority tuning."""

    horizon = strategy_horizon_minutes(strategy_name, trade_style)
    return latency_priority_multiplier(
        base_priority,
        expected_delay_ms=expected_delay_ms,
        horizon_minutes=horizon,
        sensitivity=sensitivity,
    )


def summarize_profile(profile: LatencyProfile, products: int, workers: int) -> dict:
    """Return a compact environment summary for docs and dashboards."""

    round_trip_ms = expected_round_trip_ms(profile, workers=workers, products=products)
    fill_ms = expected_fill_delay_ms(profile)
    exit_ms = expected_exit_delay_ms(profile)
    return {
        "network_rtt_ms": round(profile.network_rtt_ms, 2),
        "round_trip_ms": round(round_trip_ms, 2),
        "fill_delay_ms": round(fill_ms, 2),
        "exit_delay_ms": round(exit_ms, 2),
        "detection_delay_ms": round(expected_detection_delay_ms(profile.order_exit_poll_s), 2),
        "products": int(products),
        "workers": int(workers),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Estimate Coinbase latency and priority tuning")
    p.add_argument("--network-rtt-ms", type=float, default=75.0)
    p.add_argument("--candle-fetch-ms", type=float, default=100.0)
    p.add_argument("--rust-eval-us", type=float, default=75.0)
    p.add_argument("--backtest-us", type=float, default=2.0)
    p.add_argument("--order-preview-ms", type=float, default=140.0)
    p.add_argument("--order-submit-ms", type=float, default=160.0)
    p.add_argument("--order-exit-poll-s", type=float, default=5.0)
    p.add_argument("--spread-bps", type=float, default=10.0)
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--products", type=int, default=50)
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--scan-interval-s", type=float, default=300.0)
    p.add_argument("--strategy-name", type=str, default="ema_cross")
    p.add_argument("--trade-style", type=str, default="momentum")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    profile = LatencyProfile(
        network_rtt_ms=args.network_rtt_ms,
        candle_fetch_ms=args.candle_fetch_ms,
        rust_eval_us=args.rust_eval_us,
        backtest_us=args.backtest_us,
        order_preview_ms=args.order_preview_ms,
        order_submit_ms=args.order_submit_ms,
        order_exit_poll_s=args.order_exit_poll_s,
        spread_bps=args.spread_bps,
        slippage_bps=args.slippage_bps,
    )

    round_trip = expected_round_trip_ms(profile, workers=args.workers, products=args.products)
    fill_delay = expected_fill_delay_ms(profile)
    exit_delay = expected_exit_delay_ms(profile)
    detect_delay = expected_detection_delay_ms(args.scan_interval_s)
    tuned = latency_tuned_priority(
        1.0,
        strategy_name=args.strategy_name,
        trade_style=args.trade_style,
        expected_delay_ms=round_trip + detect_delay,
    )

    print("Latency profile")
    print(f"  network_rtt_ms      = {profile.network_rtt_ms:.1f}")
    print(f"  candle_fetch_ms     = {profile.candle_fetch_ms:.1f}")
    print(f"  rust_eval_us        = {profile.rust_eval_us:.1f}")
    print(f"  backtest_us         = {profile.backtest_us:.1f}")
    print(f"  order_preview_ms    = {profile.order_preview_ms:.1f}")
    print(f"  order_submit_ms     = {profile.order_submit_ms:.1f}")
    print(f"  order_exit_poll_s   = {profile.order_exit_poll_s:.1f}")
    print("")
    print("Estimated timings")
    print(f"  scan_delay_ms       = {detect_delay:.1f}")
    print(f"  round_trip_ms       = {round_trip:.1f}")
    print(f"  fill_delay_ms       = {fill_delay:.1f}")
    print(f"  exit_delay_ms       = {exit_delay:.1f}")
    print(f"  latency_priority    = {tuned:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
