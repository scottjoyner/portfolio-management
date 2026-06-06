# Wallet Management Module

This module provides comprehensive wallet management for Coinbase CDP.

## Available Operations

### Core Wallet Functions

```python
from cdp.wallet import CDPWallet

wallet = CDPWallet(mock_mode=False)  # or True for testing

# Create a new wallet
result = wallet.create_wallet(name="my-trading-wallet", environment="testnet")

# Get wallet details
details = wallet.get_wallet(wallet_id="wallet_123...")

# Check balance
balance = wallet.get_balance(wallet_id="wallet_123...", account_type="wallet")

# Transfer funds
result = wallet.transfer(
    from_wallet="wallet_from",
    to_account="account_xyz",
    amount=0.01,
    currency="BTC"
)

# Send payment
payment = wallet.send_payment(...)

# Get transaction history
transactions = wallet.get_transactions(wallet_id, account_type="wallet")
```

### Additional Operations

- `deposit_funds()` - Deposit fiat or crypto to wallet
- `withdraw_funds()` - Withdraw from wallet  
- `sign_transaction()` - Sign pending transactions
- `receive_payment()` - Set up payment receipt

## Mock Mode

Set `mock_mode=True` for development and testing. Returns structured mock responses instead of making actual API calls.

## Production Usage

```bash
# Initialize CDP CLI first
cdp init --name my-wallet --testnet
cdp login

# Then use in Python
wallet = CDPWallet(mock_mode=False)
```
