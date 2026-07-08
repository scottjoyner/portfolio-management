# V4 Paper Trader Performance Improvement Plan

> Diagnostic-based overhaul targeting the #1 killer (realistic fee model) and 9 secondary fixes. Based on live analysis of 415 trades, 33% win rate, net -$195.59 after fees.

## Current Performance (Live Data)

| Metric | Value | Implication |
|--------|-------|-------------|
| Win rate | 33% | 2:1 loss ratio — unsustainable |
| Realized P&L | +$204.74 | Gross profitable on paper |
| Fees + slippage | -$400.33 | **Fees exceed gross P&L by 2×** |
| Net P&L | **-$195.59** | Actual net loss |
| Open positions | 27 of 30 max | Capital spread too thin |
| BTC/ETH/SOL exposure | 0 of 27 | No safe anchor |
| Avg trade notional | ~$965 | Many micro-trades |
| Repeat signal max | 7× on ALGO-USD | Pulse noise degrades signal quality |
| Strategies with data | 4 of 50 | Analytics not persisted — lost on restart |

## Fee Model Reality

Coinbase Advanced tiered fees (trailing 30-day volume):

| Tier | Volume | Taker | Maker | Round Trip |
|------|--------|-------|-------|------------|
| 1 | $0–$10k | 0.60% | 0.40% | 1.20% / 0.80% |
| 2 | $10k–$50k | 0.40% | 0.25% | 0.80% / 0.50% |
| 3 | $50k–$100k | 0.25% | 0.15% | 0.50% / 0.30% |
| 4 | $100k–$500k | 0.20% | 0.10% | 0.40% / 0.20% |
| 5 | $500k–$1M | 0.15% | 0.05% | 0.30% / 0.10% |
| 6 | $1M–$5M | 0.10% | 0.00% | 0.20% / 0.00% |

Current paper trader uses 10bps fee + 5bps slippage = 15bps/leg (0.30% round trip) — equivalent to **Tier 6**, not Tier 1 where a $10k portfolio actually sits. The paper trader is profitable only because it simulates fees 4× lower than real.

## Changes

### 1. Dynamic Fee Model (trailing volume + tier calculation)

**Problem**: Static `paper_fee_bps=10` and `paper_slippage_bps=5` simulate Tier-6 fees regardless of actual trading volume.

**Solution**: Replace with `_fee_tier()` method that computes current fee tier from `paper_trailing_volume_30d`:

```python
COINBASE_FEE_TIERS = [
    (0, 0.60, 0.40),           # Tier 1
    (10_000, 0.40, 0.25),      # Tier 2
    (50_000, 0.25, 0.15),      # Tier 3
    (100_000, 0.20, 0.10),     # Tier 4
    (500_000, 0.15, 0.05),     # Tier 5
    (1_000_000, 0.10, 0.00),   # Tier 6
    (5_000_000, 0.08, 0.00),   # Tier 7
    (10_000_000, 0.05, 0.00),  # Tier 8
    (50_000_000, 0.03, 0.00),  # Tier 9
    (250_000_000, 0.01, 0.00), # Tier 10
    (500_000_000, 0.00, 0.00), # Tier 11
]
```

- `taker_bps` and `maker_bps` returned for current trailing volume
- `paper_trailing_volume_30d` incremented on every open and close (notional × 2 for round trip)
- Default `maker_pct = 0.50` (50% of orders fill as maker) — adjustable
- Effective rate = `maker_pct * maker_bps + (1 - maker_pct) * taker_bps`
- Track trailing volume in `_save_paper_state` / `_load_paper_state` so it persists

### 2. Limit / Maker Order Simulation

**Problem**: All orders simulated as taker (market orders). Real traders can use limit orders to pay maker fees (0.40% Tier 1 → 0% Tier 6).

**Solution**:
- Add `paper_maker_pct` parameter (default 0.50)
- On entry: `paper_maker_pct` chance of using maker rate; log the order type
- On exit: same
- Fee = notional × (maker_bps if maker else taker_bps) / 10_000
- ATR-based stop/target logic unchanged (market exit for stops, limit for targets)

### 3. Raise Entry Bar

**Problem**: Low confidence (0.40) and edge (5bps) trades flood the portfolio with noise.

**Changes**:
- `paper_min_confidence`: 0.40 → **0.55**
- `paper_min_edge_bps`: 5.0 → **15.0**
- `paper_min_trade_usd`: $25 → **$100**
- `paper_min_win_rate`: 0.55 → **0.60**
- `paper_min_sharpe`: 0.5 → **0.8**

### 4. Widen Stops (improve win rate)

**Problem**: Stop at ATR × 1.5 gives tight stops that get hit frequently (67% loss rate). Fixed RR of 2.67 not enough to overcome 33% WR + fees.

**Change**: `atr_val * 1.5` → **`atr_val * 2.5`** in both `_paper_close_position` exit logic and `_paper_open_position` entry stop calculation. Keep target at ATR × 4.0 (RR drops to 1.6x but win rate should rise to ~45-50%).

### 5. Reduce Max Positions

**Problem**: 30 max positions with $10k capital → avg $333 per position. Each position carries same fee burden.

**Change**: `paper_max_new_positions`: 30 → **12**

### 6. Skip Unknown-Regime Products

**Problem**: Products with regime="unknown" have <30 data points, no ATR, fallback 3% stops. Trading blind on illiquid pairs.

**Change**: In `_paper_execute`, if `best.get("regime") == "unknown"` or `atr_val <= 0`, skip the product. Log the skip reason.

### 7. Fix Repeat Signals

**Problem**: Pulse-aware gate missing — same product+strategy fires 7+ times.

**Changes**:
- `paper_product_cooldown_s`: 900 → **1800** (30 min)
- Add pulse-aware confidence penalty in `_paper_execute`:
  ```python
  pulse_key = f"{product_id}:{best['strategy']}:{best['action']}"
  pulse = self._signal_pulses.get(pulse_key)
  if pulse and pulse.pulse_count >= 3 and pulse.age_s < 1800:
      best["confidence"] *= 0.5  # halve confidence for repeat pulses
  ```

### 8. Persist Strategy Analytics

**Problem**: `strategy_stats` is in-memory only, resets on restart. `/analytics` shows 14 trades instead of 415.

**Solution**: Serialize `strategy_stats` and `_signal_type_counts` in `_paper_state_snapshot()` and restore in `_load_paper_state()`.

### 9. Add Coinbase Fee Tier to Health Endpoint

**Problem**: Can't see current fee tier in real-time.

**Solution**: Add to `health_status["paper"]`:
- `fee_tier`: current tier (1-11)
- `trailing_volume_30d`: trailing volume for fee calculation
- `taker_bps`: current taker fee
- `maker_bps`: current maker fee
- `effective_fee_bps`: blended rate based on maker_pct

## Implementation Order

| # | Change | Effort | Risk | Impact |
|---|--------|--------|------|--------|
| 1 | Dynamic fee model | 2h | Medium — changes P&L math | High |
| 2 | Limit order simulation | 1h | Low — additive | Medium |
| 3 | Raise entry bar | 15min | Low — just parameter changes | High |
| 4 | Widen stops | 15min | Low — one multiplier change | High |
| 5 | Reduce max positions | 5min | Low — parameter change | Medium |
| 6 | Skip unknown regime | 15min | Low — new filter | Medium |
| 7 | Fix repeat signals | 1h | Low — cooldown + gate | Medium |
| 8 | Persist analytics | 30min | Low — serialization | Medium |
| 9 | Fee tier in health | 15min | Low — data exposure | Low |

Total: ~6h of dev time, all changes in `coinbase/src/run_trader_v4.py`.

## Expected Outcome

- Realistic P&L showing Tier-1 drag (motivates volume growth)
- Path to Tier 4-5 where net P&L turns positive
- Win rate improving from 33% → ~40-45%
- Fewer, higher-conviction positions (12 instead of 27)
- No trades on illiquid pairs
- Repeat signal noise reduced 50-70%
- Analytics surviving restarts
- Visible fee tier progression in health endpoint
