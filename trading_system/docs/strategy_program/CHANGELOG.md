# CHANGELOG

- Added concrete implementation mapping in the 100-strategy catalog.
- Added reserve-aware capital orchestration module with risk-tier hard caps.
- Added tests for orchestrator safety and mapping consistency against registry classes.
- Expanded strategy contract hooks (sizing, order intents, risk hints, replay/analytics tags, state serialization stubs) and strengthened live-safety config validation for expert/research tiers.
- Second-pass hardening: added strict risk-tier/sizing-model schema guards, tier-cap validation, and live-mode stop-loss/warmup safety checks.
