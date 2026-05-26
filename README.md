# Prediction-Market Arbitrage System (Kalshi + Polymarket)

## Safety Warning
Paper trading is the default. Live trading is disabled by default and requires explicit configuration plus runtime confirmation.

## Quickstart (Mock/Paper)
- `pnpm install`
- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`
- `pnpm build`
- `pnpm cli doctor --mode mock`
- `pnpm cli discover --mode mock`
- `pnpm cli match:propose --mode mock`
- `pnpm cli arb:scan --mode mock`
- `pnpm cli arb:paper --mode mock`

## Live Trading Checklist
1. `PAPER_TRADING=false`
2. `LIVE_TRADING=true`
3. `REQUIRE_MANUAL_APPROVAL=true`
4. Market pair status = approved
5. Compliance gate passed
6. Risk checks approved
7. Runtime confirmation supplied

## Known Risks
- Cross-venue execution is non-atomic.
- Partial fills can create temporary unhedged exposure.
- Similar wording markets may still resolve differently.
