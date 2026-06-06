# Coinbase REST Integration - Refinement Plan

## Current Status: P0 Scaffolding ✅ → P1 Production ❌

**Reason:** Safety patterns are documented, but actual Coinbase v3 API integration is not implemented yet.

---

## Phase 1: Production Coinbase API Client Implementation (4-6 hours)

### Task 1.1: OAuth 2.0 Token Flow (+1 hour)
**Location:** `trading_system/connectors/coinbase/rest/oauth.py`

**Implementation Required:**
```python
from pathlib import Path
from typing import Optional, Dict
import json

class CoinbaseOAuth:
    """Coinbase Advanced Trade OAuth 2.0 token management."""
    
    def __init__(self, redirect_uri: str):
        self.redirect_uri = redirect_uri
        self.token_dir = Path('/home/falcon/git/portfolio-management/.hermes/coinbase')
        
    async def fetch_access_token(
        self, 
        grant_type: str = 'authorization_code',
        code: Optional[str] = None
    ) -> dict:
        """
        Fetch OAuth 2.0 access token from Coinbase.
        
        Args:
            grant_type: 'authorization_code' or 'refresh_token'
            code: Authorization code (for authorization_code flow)
            
        Returns:
            Access token, expires_in, and refresh_token
            
        Raises:
            requests.HTTPError on API errors
        """
        # Implementation needed
        pass
        
    async def refresh_token(self, refresh_token: str) -> dict:
        """Refresh access token using refresh_token."""
        # Implementation needed  
        pass
        
    def get_headers(self) -> dict:
        """Return OAuth headers with authorization from env."""
        auth = Path('/home/falcon/git/portfolio-management/.hermes/coinbase/auth.json').read_text()
        return json.loads(auth)
```

**Safety Features to Include:**
- Input validation on token parameters
- Sanitized logging (mask `fxp_***...****1234`)  
- Circuit breaker integration before OAuth calls
- Retry with exponential backoff for transient errors

---

### Task 1.2: Core Endpoint Integration (+2 hours)

**Location:** `trading_system/connectors/coinbase/rest/api_client.py`

**Endpoints to Implement:**

```python
from typing import List, Dict, Any

class CoinbaseAPIClient:
    """Coinbase Advanced Trade v3 API client."""
    
    def __init__(self, oauth: CoinbaseOAuth):
        self.oauth = oauth
        
    async def fetch_account(self, account_id: str) -> dict:
        """GET /v3/accounts/{id} - Fetch single account balance."""
        # Implementation needed
        
    async def list_accounts(self) -> List[dict]:
        """GET /v3/accounts - List all brokerage accounts."""
        # Implementation needed
        
    async def fetch_transaction_history(
        self, 
        account_id: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """GET /v3/accounts/{id}/ledger - Transaction history."""
        # Implementation needed
        
    async def fetch_holdings(self, account_id: str) -> dict:
        """Fetch current holdings (positions)."""
        # Implementation needed
        
    async def health_check(self) -> Dict[str, Any]:
        """GET /v3/health or auto-detect health endpoint."""
        # Implementation needed
```

**Error Handling Required:**
- Parse rate limit headers from API responses
- Map Coinbase error codes to internal exceptions  
- Implement exponential backoff retry logic
- Circuit breaker integration per instance

---

### Task 1.3: Safety Pattern Wiring (+1 hour)

**Location:** `trading_system/connectors/coinbase/rest/client.py` (existing file, needs expansion)

**Required Changes:**

```python
class CoinbaseRESTClient:
    """Production Coinbase REST client with safety features."""
    
    def __init__(self, config: dict):
        self.oauth = CoinbaseOAuth(config['redirect_uri'])
        self.api_client = CoinbaseAPIClient(self.oauth)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            cooldown_minutes=10.0
        )
        
    async def fetch_balance(self, account_id: str) -> tuple[dict, bool]:
        """
        Fetch balance with circuit breaker protection.
        
        Returns:
            (balance_dict, error_occurred)
        """
        # Wire circuit breaker to OAuth API calls
        pass
        
    async def list_accounts(self) -> tuple[List[dict], bool]:
        """List accounts with safety features."""
        pass
```

**Safety Integration Required:**
- Circuit breaker on all API endpoint calls
- Rate limit header parsing and enforcement
- Input validation before each API call
- Sanitized error logging (mask credentials)

---

### Task 1.4: Testing Infrastructure (+2 hours)

**Location:** `tests/coinbase_rest/`

**Tests to Create:**

```python
# test_oauth_flow.py
- Test token fetching from authorization code
- Test token refresh logic
- Test OAuth headers extraction

# test_account_endpoints.py  
- Test single account balance fetching
- Test list accounts endpoint
- Test transaction history pagination
- Test holdings fetching

# test_safety_patterns.py
- Test circuit breaker opens after 5 failures
- Test cooldown period enforcement
- Test rate limit header parsing
- Test input validation error handling

# conftest.py
- Mock Coinbase API responses
- Set up test fixtures
```

---

## Phase 2: Mock Data Enhancement (1-2 hours)

### Task 2.1: Expand Mock Account Data (+30 min)

**Location:** `trading_system/connectors/coinbase/mock_client.py` (existing, needs expansion)

**Add to CoinbaseMockAccount class:**
```python
@dataclass  
class CoinbaseMockAccount:
    """Mock Coinbase brokerage account with realistic data."""
    id: str
    name: str
    currency: str
    type: str  # 'wallet' or 'trading'
    primary: bool = False
    available: float = 0.0
    holding: float = 0.0
    last_refreshed: datetime = field(default_factory=datetime.now)
    
    # NEW: Add these fields
    cost_basis: float = 0.0         # Cost basis for unrealized P/L
    fees_paid: float = 0.0           # Historical fees
    unrealized_pnl: float = 0.0     # Unrealized profit/loss
```

**Add Transaction Generator:**
```python
class CoinbaseMockTransactionGenerator:
    """Generate realistic historical transaction data."""
    
    def __init__(self, config: dict):
        self.config = config
        
    async def generate_historical_transactions(
        self, 
        account_id: str, 
        days_back: int = 30
    ) -> List[dict]:
        """Generate realistic transaction history."""
        # Generate buy/sell/transfer/dividend transactions
        pass
```

---

### Task 2.2: Add Fee Calculation Logic (+30 min)

**Location:** New file `trading_system/connectors/coinbase/mock_client.py` (fee_calculator module)

**Implementation:**
```python
class CoinbaseFeeCalculator:
    """Coinbase Advanced Trade fee calculations."""
    
    def __init__(self, config: dict):
        self.maker_fee_bps = config.get('maker_fee_bps', 0.50 * 100)  # 0.50%
        self.taker_fee_bps = config.get('taker_fee_bps', 0.50 * 100)
        
    def calculate_order_fees(
        self, 
        order_amount: float,
        order_side: str,
        maker_taker: bool
    ) -> float:
        """Calculate order fees for mock data."""
        pass
        
    def fetch_historical_fees(self, account_id: str) -> List[dict]:
        """Return mock historical fee records."""
        pass
```

---

### Task 2.3: Add Error Code Mapping (+30 min)

**Location:** New file `trading_system/connectors/coinbase/mock_client.py` (error_mapping module)

**Implementation:**
```python
class CoinbaseErrorCodeMapper:
    """Map Coinbase API error codes to internal exceptions."""
    
    COINBASE_ERROR_CODES = {
        'INVALID_API_KEY': {'status': 401, 'message': 'Invalid API key'},
        'MISSING_REQUIRED_FIELD': {'status': 400, 'message': 'Missing required field'},
        'RATE_LIMIT_EXCEEDED': {'status': 429, 'message': 'Rate limit exceeded'},
        'ACCOUNT_NOT_FOUND': {'status': 404, 'message': 'Account not found'},
        'PERMISSION_DENIED': {'status': 403, 'message': 'Permission denied'},
    }
    
    def map_coinbase_error(self, error_code: str) -> Exception:
        """Map Coinbase error to internal exception."""
        pass
```

---

## Phase 3: Testing Integration (2-3 hours)

### Task 3.1: Integration Tests (+1.5 hours)

**Location:** `tests/coinbase_rest/test_integration.py`

**Test Cases:**
```python
- Test OAuth flow integration with circuit breaker
- Test full account balance fetching pipeline
- Test transaction history pagination
- Test error handling for rate limits
- Test input validation on invalid credentials
- Test sanitized logging (no raw API keys in output)
```

---

### Task 3.2: Circuit Breaker Behavior Tests (+1 hour)

**Location:** `tests/coinbase_rest/test_circuit_breaker.py`

**Test Cases:**
```python
- Test circuit breaker opens after exactly 5 failures
- Test cooldown period (should block for 10 minutes)
- Test failure count resets on successful call
- Test circuit breaker doesn't affect new instances
- Test circuit breaker error message is descriptive
```

---

### Task 3.3: Rate Limit Handling Tests (+30 min)

**Location:** `tests/coinbase_rest/test_rate_limiting.py`

**Test Cases:**
```python
- Test rate limit header parsing from response
- Test exponential backoff duration calculation
- Test rate limit enforcement before retry
```

---

## Phase 4: Deployment Hardening (2 hours)

### Task 4.1: Container Entrypoint Script (+30 min)

**Location:** `deploy/coinbase-rest/Dockerfile` (if not exists) or new entrypoint.sh

**Implementation:**
```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create non-root user for security
RUN addgroup --system app && \
    adduser --system --ingroup app app

USER app

CMD ["python", "-m", "coinbase_rest.main"]
```

---

### Task 4.2: Health Check Endpoint (+30 min)

**Location:** New file `trading_system/connectors/coinbase/rest/health_check.py`

**Implementation:**
```python
import json
from pathlib import Path
from datetime import datetime

class CoinbaseHealthCheck:
    """Health check endpoint for monitoring systems."""
    
    @staticmethod
    def get_status() -> dict:
        """Return structured health status."""
        return {
            'status': 'healthy',  # or 'degraded', 'unhealthy'
            'version': '1.0.0',
            'timestamp': datetime.now().isoformat(),
            'components': {
                'oauth_ready': True,
                'api_client_ready': True,
                'circuit_breaker_active': True,
            }
        }
```

---

### Task 4.3: Rollback Procedures (+1 hour)

**Location:** `deploy/coinbase-rest/rollback.sh`

**Implementation:**
```bash
#!/bin/bash
set -euo pipefail

COINBASE_REST_VERSION=$1
DEPLOY_DIR="/home/falcon/git/portfolio-management/deploy/coinbase-rest"

if [ -z "$COINBASE_REST_VERSION" ]; then
    echo "Usage: rollback.sh <version>"
    exit 1
fi

# Implement rollback logic
# ...
```

---

## Final Deliverables Checklist

### Production Code:
- [ ] `trading_system/connectors/coinbase/rest/oauth.py` - OAuth flow implementation
- [ ] `trading_system/connectors/coinbase/rest/api_client.py` - API endpoint integration  
- [ ] `trading_system/connectors/coinbase/rest/client.py` - Enhanced with actual Coinbase v3 calls
- [ ] `trading_system/connectors/coinbase/rest/health_check.py` - Health check endpoint

### Mock Data Enhancement:
- [ ] `trading_system/connectors/coinbase/mock_client.py` - Expanded with fee calculations, historical data, error mapping

### Testing Infrastructure:
- [ ] `tests/coinbase_rest/test_oauth_flow.py`
- [ ] `tests/coinbase_rest/test_account_endpoints.py`
- [ ] `tests/coinboard_rest/test_safety_patterns.py`
- [ ] `tests/coinboard_rest/test_integration.py`

### Deployment:
- [ ] `deploy/coinboard-rest/Dockerfile`
- [ ] `deploy/coinboard-rest/health_check.sh`
- [ ] `deploy/coinboard-rest/rollback.sh`

---

## Total Effort Estimate: 12-16 hours

**P0 Scaffolding:** ✅ Complete  
**P1 Production Ready:** Needs above refinements  
**Estimated Timeline:** 3-5 days part-time or 1-2 days full-time
