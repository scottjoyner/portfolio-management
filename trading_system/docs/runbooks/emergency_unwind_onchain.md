# Emergency Unwind Runbook

1. Trigger chain/protocol emergency pause in control plane.
2. Revoke active allowances for affected spender set.
3. Flatten onchain token deltas through safest allowlisted route.
4. Execute Coinbase hedge flattening for residual inventory.
5. Sweep realized proceeds to CASH_BUFFER and LOCKED_RESERVE per policy.
6. Export incident bundle with calldata, simulation hash, and wallet snapshot.
