# Performance Document

## Purpose

This system is now optimized around two questions:

1. How fast can we detect a tradable edge?
2. How do we rank edges when latency eats into the expected move?

This document captures the measured timings, the scan plan, the route planner, and the knobs used to bias strategy priority by latency.

## Measured Baseline

Environment used for measurements:
- Coinbase Exchange API over LAN/WAN routing
- Rust strategy engine via `rust_core.evaluate_all_py()`
- Parallel REST candle fetches via `fetch_candles_batch()`

Observed timings:

| Component | Measured |
|---|---:|
| API ping to Coinbase `/time` | avg 73.3 ms |
| API ping p50 / p95 | 69.0 ms / 126.2 ms |
| Single product candle fetch (100 hourly bars) | avg 96.9 ms |
| Rust `evaluate_all_py()` per product | avg 73.3 us |
| Full 34-product eval + trend pass | ~0.8 ms total |
| Parallel 12-wide full 34-product scan | ~275 ms total |

Universe snapshot:
- 829 Coinbase products returned by `/products`
- 526 active products after status/quote filtering
- 401 USD pairs
- 24 BTC pairs
- 6 ETH pairs
- 5 USDC pairs

## Latency Model

### 1. Detection Delay

Average delay from a periodic scan:

`detection_delay_ms = scan_interval_s * 500`

Examples:
- 5 minute scan: 150,000 ms average delay
- 1 hour scan: 1,800,000 ms average delay

### 2. Round Trip

For a universe slice, approximate round trip is:

`round_trip_ms = network_rtt_ms + candle_fetch_ms * ceil(products / workers) + compute_ms`

Where `compute_ms` is Rust eval + backtest overhead, which is negligible relative to network.

### 3. Fill Delay

Expected order fill delay for a marketable order:

`fill_delay_ms = order_submit_ms + network_rtt_ms + spread_penalty + slippage_penalty`

This increases when liquidity is thin or the spread is wide.

### 4. Exit Delay

If exits are managed by polling:

`exit_delay_ms = network_rtt_ms + (poll_interval_s * 500)`

## Scan Plan

Implemented scan tiers:

### Fast Tier

- Runs every 5 minutes
- Scans the top N liquid pairs
- Default: `--scan-top 50`
- Used for hot opportunities where recency matters most

### Full Tier

- Runs every hour
- Scans the full active Coinbase universe across USD, USDC, BTC, and ETH quote pairs
- Used to discover neglected opportunities, rotations, and tax-loss candidates

Recommended launch:

```bash
python3 -m coinbase.src.run_trader_v4 \
  --mode paper \
  --scan-interval 300 \
  --scan-top 50 \
  --scan-min-vol 500000 \
  --full-scan-interval 3600
```

## Latency-Aware Priority Tuning

Strategy priority is adjusted with a latency decay factor so short-horizon strategies get penalized when the environment is slow.

Helper:

`trading_system.core.performance_model.latency_tuned_priority()`

Conceptually:

`adjusted_priority = base_priority * exp(-expected_delay_ms / horizon_ms)`

Horizon defaults:
- Arbitrage: 5 min
- Event: 30 min
- Momentum: 60 min
- Mean reversion: 120 min
- Cycle / tax loss: much longer

Current optimizer integration:
- `portfolio_optimizer.py` applies latency-aware priority to strategy and aggregator opportunities
- short-horizon trades are down-weighted when tick interval or execution delay rises

Useful knobs:
- `scan_interval` controls average detection delay
- `scan_top_n` controls how much of the hot set is refreshed frequently
- `full_scan_interval` controls full-universe discovery cadence
- `--workers` in the performance model controls fetch parallelism assumptions

## Multi-Hop Trading

Route planning now exists in `coinbase/src/multi_hop.py`.

It builds a currency graph from active Coinbase products and can score routes like:

- `BTC -> USD`
- `BTC -> ETH -> USD`
- `USD -> BTC -> ETH`

This is useful when a direct pair is thin or missing, or when a bridge conversion is cheaper after fees/spread.

### Route Decision Factors

The planner now has a scoring layer, not just a shortest-path layer. It can rank a route using:

- route efficiency
- active opportunities on the path
- tax impact from realized gains/losses
- drawdown context
- regime context
- hop penalty
- core/stable asset preference

The public helper is `find_best_decision(source, target_candidates, products, context=...)`.

## Bear-Market Accumulation

The optimizer now applies a bear-market overlay when portfolio drawdown deepens:

- reserves shrink
- core crypto allocation grows
- core minimum allocation rises
- BTC downtrends bias the overlay more aggressively toward accumulation

Goal:
- realize tax losses where appropriate
- keep more capital in core crypto during prolonged drawdowns
- be positioned for the eventual upside phase

## Environment-Specific Profiling

Run the built-in estimator:

```bash
python3 -m trading_system.core.performance_model \
  --network-rtt-ms 73.3 \
  --candle-fetch-ms 96.9 \
  --rust-eval-us 73.3 \
  --backtest-us 2.0 \
  --products 526 \
  --workers 24 \
  --scan-interval-s 300 \
  --strategy-name ema_cross \
  --trade-style momentum
```

What it prints:
- scan delay
- round trip latency
- fill delay estimate
- exit delay estimate
- latency-adjusted priority for the chosen strategy

## Interpretation

- If network RTT is the dominant term, prioritize fewer, more liquid pairs and widen scan intervals only for deep full-universe sweeps.
- If candle fetch dominates, increase worker count or reduce full-scan cadence.
- If detection delay dominates, keep the fast scan focused on the hot set.
- If exit delay dominates, tighten bracket polling or manage exits event-driven instead of polling.

## Bottom Line

- Rust compute is not the bottleneck.
- Network and candle retrieval are the bottlenecks.
- Full-universe hourly scans plus 5-minute hot-set scans is the right shape.
- Latency-aware priority tuning lets the system prefer trades whose edge survives the delay.
