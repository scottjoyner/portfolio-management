# Coinbase Developer Platform (CDP) Integration

## Overview

This directory provides comprehensive integration with **Coinbase Developer Platform APIs**, including:
- CDP CLI & MCP server for typed tool access
- Non-custodial wallet infrastructure  
- Onramp/fiat-to-crypto onboarding
- Paymaster gas sponsorship
- x402 HTTP-native payments
- Webhooks for event-driven integrations
- AgentKit AI agent tooling
- Agentic Wallet for autonomous AI workflows

## Quickstart

### 1. Install CDP CLI

```bash
pip install cdp-cli
cdp init --name my-wallet --testnet
cdp login
```

### 2. Configure MCP Integration

Add to your `~/.hermes/config.yaml`:

```yaml
skills:
  - coinbase/cdp-cli
  - coinbase/agent-kit
  - coinbase/agentic-wallet
```

## Available CDP Components

### Core Services

| Service | Description | Location |
|---------|-------------|----------|
| **CDP CLI** | Command-line tool for all CDP APIs | `cdp_cli_wrapper.py` |
| **MCP Server** | Model Context Protocol integration | `mcp_server.py` |
| **Wallets** | Non-custodial wallet management | `wallets/` |
| **Onramp** | Fiat-to-crypto onboarding | `onramp/` |
| **Paymaster** | Gas sponsorship | `paymaster/` |
| **x402** | HTTP-native payments | `x402/` |
| **Webhooks** | Event subscriptions | `webhooks/` |
| **AgentKit** | AI agent tooling | `agent-kit/` |

### Documentation Links

All CDP documentation is available at: https://docs.cdp.coinbase.com

Quick access points:
- [API Reference](https://docs.cdp.coinbase.com/api-reference/v2/introduction)
- [Authentication](https://docs.cdp.coinbase.com/get-started/authentication/overview)
- [Supported Networks](https://docs.cdp.coinbase.com/get-started/supported-networks)

## Implementation Structure

```
cdp/
├── README.md                    # This file
├── __init__.py                  # Package init
├── cdp_cli_wrapper.py           # CDP CLI operations
├── mcp_server.py                # MCP server integration
├── wallet/                      # Wallet management
│   ├── __init__.py
│   ├── create_wallet.py
│   ├── get_balance.py
│   └── transfer.py
├── onramp/                      # Fiat onboarding
│   ├── __init__.py
│   └── onramp_client.py
├── paymaster/                   # Gas sponsorship
│   ├── __init__.py
│   └── sponsor_gas.py
├── x402/                        # HTTP payments
│   ├── __init__.py
│   └── payment_processor.py
├── webhooks/                    # Event handling
│   ├── __init__.py
│   ├── subscribe.py
│   └── verify_signatures.py
├── agent-kit/                   # AI agent tooling
│   ├── __init__.py
│   └── agent.py
├── auth/                        # Authentication utilities
│   ├── __init__.py
│   ├── jwt_auth.py
│   └── api_keys.py
└── examples/                    # Usage examples
    ├── basic_usage.md
    ├── ai_agent_integration.md
    └── production_setup.md
```

## Quick Examples

### Check Balance
```python
from cdp_cli_wrapper import get_wallet_balance
balance = get_wallet_balance(wallet_id="wallet_123", account_type="wallet")
print(balance)
```

### Send Payment
```python
from cdp_cli_wrapper import send_payment
result = send_payment(
    wallet_id="wallet_123",
    to_account="account_xyz",
    amount=0.01,
    currency="BTC"
)
```

### Subscribe to Webhooks
```python
from cdp_cli_wrapper import subscribe_webhooks
sub = subscribe_webhooks(
    event_types=["payment.received", "onramp.completed"],
    url="https://your-backend.com/webhook"
)
```

## Production Setup

See `examples/production_setup.md` for:
- Circuit breaker patterns (5 failures, 10-min cooldown)
- Mock client fallback on API failure
- Rate limiting enforcement
- Fee-adjusted profit calculations

## Support

For issues, see:
- [Service Status](https://docs.cdp.coinbase.com/support/status)
- [Error Reference](https://docs.cdp.coinbase.com/api-reference/v2/errors)
