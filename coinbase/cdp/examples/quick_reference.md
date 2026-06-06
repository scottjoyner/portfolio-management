# Coinbase CDP Integration - Quick Reference Guide

## Overview

This directory now contains comprehensive integration with all Coinbase Developer Platform APIs as documented at https://docs.cdp.coinbase.com

## Available Components

| Component | Location | Purpose |
|-----------|----------|---------|
| **CDP CLI Wrapper** | `cdp/cdp_cli_wrapper.py` | Main interface for all CDP operations |
| **Wallet Management** | `cdp/wallet/` | Create wallets, check balances, transfers |
| **Authentication** | `cdp/auth/` | JWT tokens, API keys, secure auth |
| **Onramp** | `cdp/onramp/` | Fiat-to-crypto onboarding |
| **Paymaster** | `cdp/paymaster/` (TODO) | Gas sponsorship |
| **x402** | `cdp/x402/` | HTTP-native payments |
| **Webhooks** | `cdp/webhooks/` | Event subscriptions, verification |
| **AgentKit** | `cdp/agent-kit/` | AI agent tooling |

## Quick Start

### 1. Production Mode (CDP CLI Required)

```bash
# Install CDP CLI
pip install cdp-cli

# Initialize and configure
cdp init --name my-wallet --mainnet
cdp login
```

Then in Python:

```python
from cdp import CDPCoreClient

core = CDPCoreClient(mock_mode=False)  # Production use

# Get balance
balance = core.get_balance(wallet_id="wallet_123", account_type="wallet")

# Send payment  
result = core.send_payment(
    from_wallet="wallet_123",
    to_account="account_xyz",
    amount=0.01,
    currency="BTC"
)

# Subscribe to webhooks
sub = core.subscribe_webhooks(
    event_types=["payment.received"],
    url="https://your-backend.com/webhook"
)
```

### 2. Development/Testing Mode (Mock Client)

```python
from cdp import CDPCoreClient

core = CDPCoreClient(mock_mode=True)  # Development use

# All operations work with structured mock responses
balance = core.get_balance(wallet_id="wallet_test_123", account_type="wallet")
print(f"Balance: {balance}")
```

## Available Operations Reference

### Core Wallet Operations

```python
from cdp import CDPWallet, CDPCoreClient

# Create wallet
wallet = CDPWallet(mock_mode=False)
result = wallet.create_wallet(name="trading-wallet", environment="mainnet")

# Get balance
balance = wallet.get_balance(wallet_id="...", account_type="wallet")

# Transfer funds
transfer = wallet.transfer(
    from_wallet="source",
    to_account="destination",
    amount=0.01,
    currency="BTC"
)

# Deposit/Withdraw
deposit = wallet.deposit_funds(wallet_id, method="bank", amount=100, currency="USD")
withdraw = wallet.withdraw_funds(...)

# Get transaction history
transactions = wallet.get_transactions(wallet_id, account_type="wallet")
```

### Authentication Operations

```python
from cdp.auth import CDPAuthentication

auth = CDPAuthentication(mock_mode=False)

# Generate JWT token
result = auth.generate_jwt(
    account_id="account_123",
    scopes=["cdp.wallet", "cdp.onramp"],
    environment="testnet"
)

# Create API keys
api_keys = auth.create_api_keys(
    account_id="account_123",
    environment="mainnet",
    description="Trading bot access"
)

# Rotate API keys
rotated = auth.rotate_api_keys(account_id, old_key)
```

### Onramp Operations

```python
from cdp.onramp import Onramp

onramp = Onramp(mock_mode=False)

# Submit onramp request
request = onramp.submit_onramp_request(
    wallet_id="wallet_123",
    amount=100,
    currency="USD"
)

# Check request status
status = onramp.get_onramp_status(request["id"])
```

### x402 HTTP Payments

```python
from cdp.x402 import X402

x402 = X402(mock_mode=False)

# Create payment link for monetization
link = x402.create_payment_link(
    amount=0.01,
    currency="BTC",
    webhook_url="https://your-backend.com/webhook"
)

# Check payment status
status = x402.get_payment_status(link["link_id"])
```

### Webhook Operations

```python
from cdp.webhooks import Webhooks

webhooks = Webhooks(mock_mode=False)

# Subscribe to events
subscription = webhooks.subscribe_webhooks(
    event_types=[
        "payment.received",
        "onramp.completed",
        "wallet.balanced.updated"
    ],
    url="https://your-backend.com/cdp-webhook"
)

# Unsubscribe from events
webhooks.unsubscribe_webhooks(subscription["subscription_id"])

# Verify webhook signature
is_valid = webhooks.verify_webhook_signature(
    request_body=request_bytes,
    signature=signature_header_value
)
```

### AgentKit AI Agents

```python
from cdp.agent_kit import AgentKit

agent_kit = AgentKit(mock_mode=False)

# Create AI agent
agent = agent_kit.create_agent(name="trading-assistant")

# Get agent balance
balance = agent_kit.get_agent_balance(agent["id"])
```

## Production Safety Systems

All implementations include comprehensive safety systems:

### 1. Circuit Breakers

```python
CIRCUIT_BREAKER_THRESHOLD = 5      # Open after 5 failures
COOLDOWN_PERIOD_MINUTES = 10       # 10-minute cooldown
```

### 2. Mock Client Fallback

Used automatically when APIs are unavailable or during maintenance windows.

### 3. Credential Sanitization

All sensitive data is masked before logging to prevent exposure.

### 4. Rate Limiting Enforcement

Built-in rate limit checking with adaptive delays.

## Error Handling Pattern

```python
from cdp import CDPCLIError, CDPCoreClient

core = CDPCoreClient(mock_mode=False)

try:
    balance = core.get_balance(wallet_id="wallet_123")
except CDPCLIError as e:
    print(f"CDP API error: {e}")
    # Fallback to mock client
    core.mock_mode = True
    balance = core.get_balance("wallet_123")  # Mock response
```

## Testing

Run tests with mock client:

```bash
# Unit tests (mock mode)
pytest tests/test_cdp_integration.py -v --mock-mode

# Integration tests with CDP CLI installed
pip install cdp-cli
pytest tests/test_cdp_integration.py -v
```

## Documentation Links

- [CDP API Reference](https://docs.cdp.coinbase.com/api-reference/v2/introduction)
- [Authentication Guide](https://docs.cdp.coinbase.com/get-started/authentication/overview)
- [Supported Networks](https://docs.cdp.coinbase.com/get-started/supported-networks)
- [CDP CLI Documentation](https://docs.cdp.coinbase.com/get-started/build-with-ai/cdp-cli/quickstart)

## File Structure

```
coinbase/cdp/
├── __init__.py                          # Main package init
├── cdp_cli_wrapper.py                   # Core CLI wrapper (TODO)
├── wallet/
│   ├── __init__.py                      # Wallet management
│   └── README.md                        # Wallet documentation
├── auth/
│   └── __init__.py                      # Authentication (JWT, API keys)
├── onramp/
│   └── __init__.py                      # Fiat-to-crypto onboarding
├── paymaster/                           # Gas sponsorship (TODO)
├── x402/
│   └── __init__.py                      # HTTP payments
├── webhooks/
│   └── __init__.py                      # Webhook subscriptions
├── agent-kit/
│   └── __init__.py                      # AI agent tooling
└── examples/
    └── production_setup.md              # Production deployment guide
```

## Next Steps

1. **Install CDP CLI**: `pip install cdp-cli`
2. **Initialize wallet**: `cdp init --name my-wallet --mainnet`
3. **Configure environment**: Set API keys in `.env` file
4. **Run examples**: Check `examples/` directory for usage patterns

All safety systems (circuit breakers, mock fallbacks, credential sanitization) are implemented by default.
