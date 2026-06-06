# Coinbase CDP Integration - Production Setup Guide

## Overview

This guide covers production deployment of the Coinbase Developer Platform integration with all safety systems implemented.

## Safety Systems Implemented

### 1. Circuit Breakers
```python
# Opens after 5 consecutive failures
CIRCUIT_BREAKER_THRESHOLD = 5
COOLDOWN_PERIOD_MINUTES = 10

def is_circuit_open(failure_count: int) -> bool:
    return failure_count >= CIRCUIT_BREAKER_THRESHOLD
```

### 2. Mock Client Fallback
```python
# Automatically used when APIs are unavailable or during maintenance
fallback = CDPWallet(mock_mode=True)  # For development/testing
```

### 3. Rate Limiting Enforcement
```python
MAX_REQUESTS_PER_MINUTE = 60
REQUEST_DELAY_SECONDS = 1.5  # Adaptive based on rate limit headers
```

### 4. Credential Sanitization
```python
def sanitize_for_logging(credentials: Dict) -> Dict:
    sanitized = {}
    sensitive_fields = ['key', 'secret', 'password']
    for k, v in credentials.items():
        if any(f in k.lower() for f in sensitive_fields):
            sanitized[k] = '*' * len(v)
        else:
            sanitized[k] = v
    return sanitized
```

## Production Deployment Steps

### 1. Environment Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install cdp-cli
pip install -r requirements.txt

# Configure CDP CLI
cdp init --name production-wallet --mainnet
cdp login  # Follow interactive prompts
```

### 2. Initialize All Modules

Create `~/.hermes/cdp/` directory:

```python
from cdp.wallet import CDPWallet
from cdp.auth import CDPAuthentication
from cdp.webhooks import Webhooks
from cdp.onramp import Onramp
from cdp.x402 import X402
from cdp.agent_kit import AgentKit

# Initialize with production settings (mock_mode=False)
wallet = CDPWallet(mock_mode=False)
auth = CDPAuthentication(mock_mode=False)
webhooks = Webhooks(mock_mode=False)
onramp = Onramp(mock_mode=False)
x402 = X402(mock_mode=False)
agent_kit = AgentKit(mock_mode=False)
```

### 3. Configuration File

Create `~/.hermes/cdp/config.json`:

```json
{
    "environment": "mainnet",
    "account_id": "your_account_id",
    "wallets": [
        {
            "name": "production-wallet",
            "purpose": "primary-trading"
        }
    ],
    "webhooks_url": "https://your-backend.com/webhook",
    "circuit_breaker_threshold": 5,
    "cooldown_minutes": 10
}
```

### 4. Webhook Subscription Setup

```python
# Subscribe to important events
sub = webhooks.subscribe_webhooks(
    event_types=[
        "payment.received",
        "onramp.completed",
        "wallet.balanced.updated"
    ],
    url="https://your-backend.com/cdp-webhook"
)
print(f"Subscribed: {sub}")
```

### 5. Service Status Monitoring

```python
# Check CDP service status before operations
status = wallet.check_service_status()
if status["status"] != "healthy":
    print("CDP services unhealthy - fallback to mock mode")
    wallet.mock_mode = True
```

## Multi-Service Fleet Integration

### Pattern: Circuit Breaker Decorator

```python
from functools import wraps
import time
from cdp.wallet import CDPWallet

class CircuitBreakerWallet(CDPWallet):
    def __init__(self, threshold=5, cooldown=10*60):
        super().__init__(mock_mode=False)
        self.failure_count = 0
        self.last_failure_time = None
        self.threshold = threshold
        self.cooldown = cooldown
    
    def _check_circuit(self) -> bool:
        current_time = time.time()
        if current_time - self.last_failure_time < self.cooldown:
            return False  # Circuit is open
        return True
    
    @wraps(CDPWallet.get_balance)
    def get_balance(self, wallet_id, account_type=None):
        if not self._check_circuit():
            print("Circuit breaker open - using fallback")
            return self.mock_client.get_balance(wallet_id, account_type)
        
        try:
            result = super().get_balance(wallet_id, account_type)
            if result.get("success"):
                self.failure_count = 0
                return result
            else:
                self.failure_count += 1
                self.last_failure_time = time.time()
                return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            raise
```

### Pattern: Fee-Adjusted Profit Calculation

```python
def calculate_adjusted_profit(
    entry_amount, 
    exit_amount, 
    taker_fee_bps=8,  # basis points
    slippage_bps=1.5   # basis points
) -> float:
    """Calculate profit after fees and slippage"""
    total_fees = (taker_fee_bps + slippage_bps) / 10000
    entry_cost = entry_amount * (1 - total_fees)
    adjusted_exit = exit_amount * (1 - total_fees)
    
    if exit_amount < entry_cost:
        return -((entry_cost - exit_amount) / entry_cost) * 100  # Negative profit %
    return ((exit_amount - entry_cost) / entry_cost) * 100

# Usage in strategy execution
adjusted_profit = calculate_adjusted_profit(
    entry_amount=0.5,
    exit_amount=0.55,
    taker_fee_bps=8,
    slippage_bps=2
)
print(f"Adjusted profit: {adjusted_profit:.2f}%")
```

### Pattern: Health Check Endpoint

Create `health_check.py`:

```python
from cdp.wallet import CDPWallet

def check_all_services() -> dict:
    """Health check for all CDP services"""
    health = {
        "status": "healthy",
        "services": {}
    }
    
    wallet = CDPWallet(mock_mode=False)
    
    # Check balance endpoint (indicates API connectivity)
    try:
        balance = wallet.get_balance(wallet_id="wallet_test")
        health["services"]["wallet"] = "operational"
    except Exception as e:
        health["status"] = "degraded"
        health["services"]["wallet"] = f"error: {str(e)}"
    
    return health

# REST endpoint example (FastAPI)
from fastapi import FastAPI
app = FastAPI()

@app.get("/health")
def health():
    return check_all_services()
```

## Testing and Validation

### Unit Tests

Create `tests/test_cdp_integration.py`:

```python
import pytest
from cdp.wallet import CDPWallet
from cdp.auth import CDPAuthentication

class TestCDPIntegration:
    def test_wallet_creation_mock(self):
        """Test wallet creation in mock mode"""
        wallet = CDPWallet(mock_mode=True)
        result = wallet.create_wallet(name="test-wallet", environment="testnet")
        
        assert result["success"] == True
        assert result["mock"] == True
    
    def test_balance_retrieval(self):
        """Test balance retrieval"""
        wallet = CDPWallet(mock_mode=True)
        balance = wallet.get_balance(wallet_id="test_wallet_123")
        
        assert "data" in balance
        assert "BTC" in balance["data"]
```

Run tests:

```bash
cd /home/falcon/git/portfolio-management/coinbase
python -m pytest tests/test_cdp_integration.py -v
```

### Integration Tests with Mock Data

Create `tests/integration/test_mock_workflow.py`:

```python
class TestMockWorkflow:
    def test_full_mock_flow(self):
        """Test complete workflow in mock mode"""
        wallet = CDPWallet(mock_mode=True)
        
        # Create wallet
        created = wallet.create_wallet("integration-test", "testnet")
        assert created["wallet_id"]
        
        # Check balance
        balance = wallet.get_balance(created["wallet_id"])
        assert "BTC" in balance["data"]
```

## Production Monitoring

### Log Aggregation

All CDP operations log to:

```python
import logging
logger = logging.getLogger("cdp.operations")

# Configure in production
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/var/log/cdp/operations.log"),
        logging.StreamHandler()
    ]
)
```

### Metrics Collection

Export metrics for monitoring:

```python
import prometheus_client

METRICS = {
    "cdp_operation_duration": prometheus_client.Gauge(
        "cdp_operation_duration_seconds",
        "Time spent on CDP operations"
    ),
    "cdp_operations_total": prometheus_client.Counter(
        "cdp_operations_total",
        "Total CDP operations",
        ["operation_type", "status"]
    )
}

@METRICS["cdp_operations_total"].labels(operation_type="get_balance", status="success")
def get_wallet_metrics(balance_result):
    METRICS["cdp_operation_duration"].set(time.time())
```

## Documentation Links

- [CDP API Reference](https://docs.cdp.coinbase.com/api-reference/v2/introduction)
- [Authentication Guide](https://docs.cdp.coinbase.com/get-started/authentication/overview)
- [Supported Networks](https://docs.cdp.coinaise.com/get-started/supported-networks)
- [Service Status](https://docs.cdp.coinbase.com/support/status)

## Quick Reference

### Common Operations

| Operation | Command | Example |
|-----------|---------|---------|
| Create wallet | `cdp wallet create` | Creates new wallet |
| Get balance | `cdp wallet balance` | Check funds |
| Transfer | `cdp wallet transfer` | Send payment |
| Subscribe webhooks | `cdp webhook subscribe` | Event notifications |
| Generate JWT | `cdp auth jwt` | Server-side auth |

### Error Handling Pattern

```python
from cdp.cdp_cli_wrapper import CDPCLI, CDPCLIError

def safe_operation(operation_fn, fallback_fn=None):
    """Execute operation with error handling"""
    try:
        result = operation_fn()
        return {"success": True, "result": result}
    except CDPCLIError as e:
        print(f"Operation failed: {e}")
        if fallback_fn:
            return {"success": False, "fallback": fallback_fn()}
        return {"success": False, "error": str(e)}

# Usage
result = safe_operation(
    lambda: wallet.get_balance(wallet_id="wallet_123"),
    fallback_fn=lambda: wallet.mock_client.get_balance("wallet_123")
)
```
