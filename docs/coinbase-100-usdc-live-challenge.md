# Coinbase 100 USDC Live Challenge Guardrails

This runbook is for a deliberately isolated live-trading challenge account/bucket. It is designed to prevent the automated trader from touching the broader portfolio while still letting the operator test a 100 USDC budget with small orders.

## Safety model

- Budget is tracked through the `challenge` capital bucket.
- New order intent is blocked if `TRADER_KILL_SWITCH=true` or the kill switch file exists.
- `TRADER_LIVE_CHALLENGE_ONLY=true` blocks signals whose bucket is not `challenge`.
- `TRADER_CHALLENGE_MAX_ORDER_USD=10` caps each order to about 10 USD notional.
- `TRADER_MAX_ORDERS_PER_TICK=1` prevents bursts.
- `TRADER_MAX_NOTIONAL_PER_TICK=10` prevents multiple small signals from exceeding the desired per-tick exposure.
- `COINBASE_DRY_RUN=true` remains the safe default. Flip it only after reviewing dashboard/status output and credentials.

## Recommended environment

```bash
export TRADER_MODE=approval
export COINBASE_DRY_RUN=true
export TRADER_EQUITY=100
export TRADER_CHALLENGE_CAPITAL_USDC=100
export TRADER_BUCKET_STATE_PATH=state/coinbase-100usdc-challenge-buckets.json
export TRADER_LIVE_CHALLENGE_ONLY=true
export TRADER_CHALLENGE_MAX_ORDER_USD=10
export TRADER_MAX_ORDERS_PER_TICK=1
export TRADER_MAX_NOTIONAL_PER_TICK=10
export TRADER_KILL_SWITCH=false
export TRADER_KILL_SWITCH_PATH=data/trading_kill_switch
```

Use `TRADER_MODE=approval` before `TRADER_MODE=live`; approval mode writes pending approvals instead of immediately placing live orders.

## Operator checks

1. Keep the kill switch file available:
   ```bash
   mkdir -p data
   touch data/trading_kill_switch   # blocks new orders
   rm data/trading_kill_switch      # allows new orders when env kill switch is false
   ```
2. Verify status includes `execution_guards` before running unattended.
3. Verify `capital_buckets` only shows the challenge bucket/path you intend.
4. Run the safety test before live work:
   ```bash
   python3 tests/test_challenge_live_guards.py
   python3 -m py_compile coinbase/src/orchestrator.py coinbase/src/risk_manager.py coinbase/src/capital_buckets.py
   ```

## Live activation checklist

Do not enable immediate live execution until all items are true:

- Coinbase credentials are explicitly configured and scoped for this challenge.
- `TRADER_BUCKET_STATE_PATH` points to a challenge-specific state file, not shared portfolio state.
- `TRADER_LIVE_CHALLENGE_ONLY=true`.
- `TRADER_CHALLENGE_MAX_ORDER_USD=10` or lower.
- `TRADER_MAX_ORDERS_PER_TICK=1`.
- `TRADER_MAX_NOTIONAL_PER_TICK=10` or lower.
- Kill switch has been tested both ways.
- Dashboard/status confirms the intended guard values.

Only then consider:

```bash
export TRADER_MODE=live
export COINBASE_DRY_RUN=false
python3 -m coinbase.src.run_trader_v2
```

The guardrails do not guarantee profit; they only limit automation blast radius and portfolio interaction.
