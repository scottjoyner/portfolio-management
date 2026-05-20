# Remaining Strategy Risks

## Still experimental / conceptual

- `GenericSpecStrategy` rows are documentation-grade strategy specs, not production signal engines.
- Several CEX families remain threshold stubs and require true model logic for live deployment claims.

## Testing limitations

- Family-specific scenario tests (trend false-break, mean-reversion regime disable, stat-arb breakdown) are still sparse.
- End-to-end risk+execution tests for each strategy are not fully present.

## Realism limitations

- Slippage and fill models remain simplified for some replay/backtest paths.
- Latency and queue effects for microstructure strategies need richer replay calibration.
- Onchain gas/MEV dynamics can diverge from deterministic tests.

## Not live-auto ready

- Ensemble allocator, pairs, basis carry, and liquidity snapback should remain paper/replay-first.

## Recommended next steps

1. Implement per-family sizing + order-intent generators in strategy layer.
2. Connect strategy explainability fields into analytics attribution reports.
3. Expand replay fixtures for stressed liquidity and exchange degradation periods.
4. Add capability flags enforcing manual approvals for non-mature strategies.

