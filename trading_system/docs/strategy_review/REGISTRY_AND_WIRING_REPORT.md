# Registry and Wiring Report

## Current registry state

- Canonical loader: `strategies/registry/registry.py::load_strategies`.
- Base + advanced spec strategies are loaded together.
- This pass added duplicate strategy-id detection and metadata indexing helper.

## Wiring observations

- Strategy registry wiring is functional for discovery/inventory.
- Execution/risk path integration for many CEX strategies remains indirect (strategy emits signal, other layers infer intents).
- Onchain modules are better integrated with explicit planners/policies.

## Fixes applied

- Added uniqueness guard to prevent duplicate `strategy_id` collisions.
- Added `strategy_metadata_index()` for deterministic reporting and operator/UI integration.
- Standardized strategy metadata payloads for non-advanced strategies.

