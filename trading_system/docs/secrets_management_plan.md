# Secrets & Key Management Plan — Trading System Security Architecture

## Overview

This document outlines the security architecture for managing sensitive data including API keys, database credentials, private keys for wallet operations, and operator approvals. All implementations default to **paper mode** (no transaction signing) requiring explicit operator approval for live execution paths.

---

## 1. Credential Categories & Storage

### Environment Variables (`.env` file)
```bash
# Trading Mode (P0/Paper/Shadow/Live)
TRADING_MODE=paper

# Database
DATABASE_URL=postgresql://user:password@localhost/trading_system

# Coinbase Integration (read-only sync harness)
COINBASE_API_KEY=your_api_key_here
COINBASE_API_SECRET=your_secret_here
LIVE_TRADING_ENABLED=false  # Safety gate

# Onchain RPC Endpoints (paper mode, no signing)
BASE_RPC=https://mainnet.base.org
ETHEREUM_RPC=https://eth-mainnet.alchemyapi.io/v2/YOUR_KEY
ONCHAIN_MODE=paper
ONCHAIN_LIVE_ENABLED=false
```

**Security**: Never commit `.env` to source control. Store in CI/CD pipeline secrets.

### Hardware Wallet Integration (Future P3 Enhancement)
```bash
# For production live mode:
HARDWARE_WALLET_PROVIDER=ledger  # ledger, trezor, or keystone
HARDWARE_WALLET_PATH=/dev/ttyUSB0
PRIVATE_KEYS_FROM_ENV=false     # Never use env vars for private keys
```

**Recommendation**: Use hardware wallets (Ledger/Trezor) or KMS services (AWS KMS, HashiCorp Vault) for live signing.

---

## 2. API Key Rotation Strategy

### Coinbase Read-Only Keys
- **Scope**: Brokerage API read-only access (accounts, portfolios, product listings)
- **Lifespan**: Recommended 90-day rotation
- **Storage**: Environment variables, never logs/metrics
- **Rotation Script**: `scripts/rotate_coinbase_keys.py`

```bash
# Usage:
python scripts/rotate_coinbase_keys.py --generate-new-keys \
    --old-keys-file ~/.scottjoyner/fleet/keys/coinbase_old.pem \
    --new-keys-file ~/.scottjoyner/fleet/keys/coinbase_new.pem
```

### Onchain Wallet Keys (Future)
- **Scope**: Transaction signing for live trades
- **Storage**: Hardware wallet or KMS only
- **Rotation**: Via hardware wallet interface, never script-generated
- **Approval**: Requires signed operator approval packet

---

## 3. Private Key Handling Guidelines

### ✅ DO:
- Store private keys in HSM/KMS, never environment variables
- Use multi-signature wallets for hot treasury operations  
- Rotate access credentials quarterly
- Log key generation events to audit tables
- Implement hardware wallet for live signing

### ❌ NEVER:
- Store full private keys in Git or CI/CD logs
- Hard-code API keys in source code
- Use environment variables for production private keys
- Broadcast transaction signatures to untrusted endpoints
- Log private keys or wallet addresses with sensitive data

---

## 4. Key Storage Locations

### Fleet-wide Keys (Tailscale network)
```bash
# Base storage path
~/.scottjoyner/fleet/keys/
    
# Coinbase API keys
~/.scottjoyner/fleet/keys/coinbase_*.pem

# Onchain wallet keys (hardware-backed only)
~/.scottjoyner/fleet/keys/hardware-wallets/*.key
```

### Local Development Keys (Paper mode only)
```bash
# Temporary paper test keys (never production)
~/.hermes/paper-keys/
```

---

## 5. Operator Approval Workflow

### Live Mode Activation (Requires Signed Approval)

1. **Generate approval packet**:
```python
from onchain.security.approval_gates.approval_packet import ApprovalPacketBuilder

packet = ApprovalPacketBuilder().build(
    plan=path_analyzer.analyze(...),
    wallet="hardware-wallet",
    reason="operator_approved_arbitrage_opportunity",
)
```

2. **Sign with hardware wallet**:
```bash
# Ledger example
ledger-cli sign --app ethereum --path 44'/60'/0'/0/0 \
    --verify-signature-file ~/.scottjoyner/fleet/keys/ledger_signature.pem
```

3. **Upload signed signature to relay** (if using multi-sig pattern)

4. **Verify in audit logs**:
```python
from storage.postgres.repository import OpsRepository

repo = OpsRepository(db)
audit_logs = repo.get_audit_logs(event_type="operator_approval", limit=10)
```

---

## 6. Audit Logging Requirements

All security-critical operations must be logged to PostgreSQL `audit_events` table:

| Event Type | Fields Required | Retention |
|------------|-----------------|-----------|
| `key_generated` | key_type, key_id, creator, timestamp | 7 years |
| `key_rotated` | old_key_sha256, new_key_sha256, rotator | 7 years |
| `live_mode_activated` | approver, approval_packet_hash, timestamp | 7 years |
| `transaction_signed` | tx_hash, signer, wallet_type, timestamp | 7 years |
| `operator_approval` | approver, approved_for, reason, timestamp | 7 years |

---

## 7. CI/CD Secrets Management

### GitHub Actions Example:
```yaml
# .github/workflows/deploy.yml
env:
  COINBASE_API_KEY: ${{ secrets.COINBASE_API_KEY }}
  DATABASE_URL: ${{ secrets.DATABASE_URL }}
  
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      # Inject secrets into container at runtime (not baked in image)
      - run: docker-compose exec coder bash -c '
          echo "$COINBASE_API_KEY" > /run/secrets/coinbase_api_key
          chmod 600 /run/secrets/coinbase_api_key
        '
```

### Docker Secrets Example:
```dockerfile
# secrets/coinbase_api_key (mounted at runtime, never in image)
COINBASE_API_KEY=your_secret_here
    
FROM trading_system:coder
COPY ./secrets/coinbase_api_key /run/secrets/coinbase_api_key
RUN chmod 600 /run/secrets/coinbase_api_key
```

---

## 8. Production Deployment Checklist

### Before going live:

- [ ] All API keys rotated and verified in staging environment
- [ ] Hardware wallet set up and tested (Ledger/Trezor)
- [ ] KMS service configured (AWS KMS / HashiCorp Vault)
- [ ] Audit logging enabled for all security-critical operations
- [ ] Multi-sig wallet deployed for hot treasury operations
- [ ] Emergency kill-switch API endpoint tested (`/emergency/stop_all_trading`)
- [ ] Rate limiting middleware active on all public endpoints
- [ ] Database backup and recovery tested
- [ ] On-call rotation defined for security incidents

### Emergency Procedures:

**Emergency Stop All Trading**:
```bash
curl -X POST http://localhost:8000/emergency/stop_all_trading
# Response: {"status": "stopped", "timestamp": "..."}
```

**Revoke All Pending Approvals**:
```bash
curl -X DELETE http://localhost:8000/onchain/approvals/revoke-all
```

---

## 9. Security Monitoring & Alerts

### Recommended Alert Thresholds:

| Metric | Warning | Critical |
|--------|---------|----------|
| Failed health checks | 1 minute | 3 minutes |
| API error rate | >5% | >10% |
| Rate limit hits | >20% of requests | >40% |
| Database connection failures | Any failure | >3 consecutive |
| Unusual trading volume spike | 3x normal | 10x normal |
| Audit log write failures | Any failure | — |

### Alert Integration:
```python
# Add to apps/api/metrics.py
metrics.on_critical_threshold_exceeded("api_error_rate", threshold=0.1, callback=send_slack_alert)
```

---

## 10. Summary of Security Architecture

| Component | Implementation | Security Level |
|-----------|---------------|----------------|
| **Trading Mode** | Paper mode (default), no signing | ✅ Safe for development |
| **API Keys** | Environment variables + rotation | ⚠️ Rotation required every 90 days |
| **Private Keys** | Hardware wallet only | ✅ Secure for production |
| **Database Auth** | Password + SSL/TLS | ✅ Standard best practices |
| **Rate Limiting** | Token bucket, per-endpoint | ✅ Prevents quota exhaustion |
| **Audit Logging** | PostgreSQL `audit_events` table | ✅ Full traceability |
| **Operator Approval** | Signed approval packets | ✅ Multi-party authorization |

---

## 11. Next Steps (P3 Enhancements)

1. **Hardware Wallet Integration**: Wire Ledger/Trezor drivers to existing wallet interface
2. **KMS Service**: AWS KMS integration for production key management
3. **Multi-Sig Wallet**: Deploy Gnosis Safe or OpenZeppelin MultiSend for hot treasury
4. **Automated Key Rotation**: Cron job for quarterly API key rotation
5. **Security Dashboard**: Grafana/Prometheus integration with alerting rules

---

## 12. References

- [OWASP Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Sensitive_Data_Exposure_Prevention_Cheat_Sheet.html)
- [Hardware Wallet Security Guidelines](https://ledger.com/security-guidelines)
- [AWS KMS Best Practices](https://docs.aws.amazon.com/kms/latest/developerguide/security.html)
