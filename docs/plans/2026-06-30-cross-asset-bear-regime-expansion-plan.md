# Cross-Asset Bear Regime Expansion Plan

> Goal: make the system robust in declining markets by treating BTC and broad equity weakness as a first-class regime signal, then expand execution beyond crypto spot into short-capable and cross-asset instruments.

## Problem Statement

The current paper trader is crypto-spot focused. It can buy/sell crypto, but it cannot yet:

- trade a bearish BTC trend as a directional short,
- hedge crypto exposure against SPX / QQQ weakness,
- allocate into defensive instruments when risk assets are rolling over,
- express market-wide decline as a regime instead of a single-asset signal.

That means the system can miss the most important edge in a down market: avoiding long risk and actively harvesting downside trends.

## Design Principles

1. Regime first, asset second.
2. Preserve capital when BTC and equities are both weak.
3. Prefer explicit short/hedge instruments over synthetic hacks.
4. Keep paper and live scope separate until every adapter is certified.
5. Expand execution only after signal quality and risk controls are proven in paper.

## Target State

The trading stack should support these buckets:

- Crypto spot long-only (current baseline).
- Crypto perpetual / margin short-capable execution.
- Equity index proxies (SPY, QQQ, VTI) as trend/hedge references.
- Inverse ETFs / index futures for bearish exposure.
- Cash / stablecoin / defensive allocation when no high-conviction edge exists.

## Phases

### Phase 0 - Regime Intelligence Layer

Create a cross-asset regime service that consumes:

- BTC trend state (50/200 SMA, slope, ADX, volatility, drawdown).
- SPX / QQQ / VIX / DXY / 10Y yield state.
- Correlation regime (BTC vs SPX, BTC vs NASDAQ, crypto beta compression/expansion).
- Market breadth / risk-on vs risk-off classification.

Outputs:

- `regime`: `risk_on | risk_off | crash | rebound | mixed`
- `trend_bias`: `bullish | bearish | neutral`
- `hedge_bias`: `on | off`
- `allowed_actions`: list of permitted trade types
- `risk_multiplier`: portfolio sizing scalar

Acceptance criteria:

- BTC and SPX declining together produce `risk_off` or `crash`.
- Regime output is cached, observable, and included in health.
- No execution changes yet; this is signal infrastructure only.

### Phase 1 - Bearish Signal Layer

Add signals that explicitly express downside conditions:

- Trend breakdown on BTC, ETH, SOL, and broad market proxies.
- Death-cross / below-200DMA filters.
- Breakdown confirmation after failed retests.
- Volatility expansion after compression.
- Macro risk-off confirmation from DXY / VIX / yields.
- Correlation breakdown / beta spike detection.

Bearish decisions should be able to:

- block new longs,
- reduce existing risk,
- request hedge entries,
- prefer short-capable instruments when available.

Acceptance criteria:

- A BTC downtrend can suppress all long-only crypto entries.
- A confirmed market-wide risk-off state can generate defensive opportunities.
- Signal reasons must clearly label the bearish regime trigger.

### Phase 2 - Portfolio / Risk Policy

Add a policy layer that translates regime into portfolio behavior:

- `risk_on`: normal sizing.
- `risk_off`: smaller size, fewer positions, stricter confirmation.
- `crash`: new longs blocked, only hedges/shorts/defensive allocation.
- `rebound`: allow mean-reversion only after confirmation.

Risk controls:

- Max portfolio beta.
- Max correlated exposure by regime.
- Max drawdown by regime.
- Mandatory cooldown after regime flips.
- Minimum edge threshold increases during high volatility.

Acceptance criteria:

- Position sizing changes with regime, not just confidence.
- Long exposure is explicitly capped or blocked in `crash`.
- Any hedge/short path is gated behind stronger evidence than a normal long.

### Phase 3 - Execution Adapter Expansion

Current paper trader is crypto-spot only. Future execution adapters should be added in this order:

1. Crypto spot (already exists).
2. Crypto perpetuals / margin short-capable adapter.
3. Index ETF adapter for paper-only exploration.
4. Futures / CFD adapter where compliant and supported.

Adapters must expose:

- buy
- sell
- short open / short close
- hedge open / hedge close
- order preview
- fill reconciliation

Acceptance criteria:

- The strategy layer can emit `LONG`, `SHORT`, `HEDGE`, and `FLAT` intents.
- Adapters reject unsupported intent types instead of silently coercing them.
- Paper execution and live execution stay behaviorally aligned.

### Phase 4 - Validation and Certification

Validation gates before live use:

- Bear-market replay backtests.
- BTC + SPX dual-downtrend simulation.
- High-volatility crash scenario.
- Rebound scenario after capitulation.
- Hedge effectiveness and slippage validation.
- Regime flip latency and false-positive analysis.

Acceptance criteria:

- Bear regime logic improves net P&L versus the current long-only baseline.
- False bearish flips are measured and within tolerance.
- Every new adapter has a paper simulation before live exposure.

## Immediate Constraints

- Do not expand live execution until the new regime layer has paper evidence.
- Keep the current trader crypto-only until the adapter layer is certified.
- The first live extension should likely be crypto perpetual shorts, not equities.

## Operational Deliverables

- Cross-asset regime service.
- Bearish signal catalogue.
- Portfolio policy matrix by regime.
- Adapter contract for short-capable execution.
- Paper replay harness for BTC/SPX downtrend periods.
- Health endpoint exposure for regime and hedge state.
