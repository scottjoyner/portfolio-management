# Plaid Integration - Implementation Guide (2026-05-27)

## Current State ✓

All scaffolding complete, ready for PlaidClient integration.

---

## Files Requiring PlaidClient Implementation

### 1. `trading_system/plaid/api/plaid_routes.py` (Lines 41-397)

#### Location: `/home/falcon/git/portfolio-management/trading_system/plaid/api/plaid_routes.py`

**Pending:** Replace mock implementations with actual PlaidClient calls

**Dependencies to install first:**
```bash
pip install plaid-client cryptography
```

**PlaidClient Initialization Pattern:**
```python
from plaid import Client, ItemLinkToken2023FlowRequest, PublicTokenExchangeRequest, ItemsGetPublicItemRequest
from plaid.models.link import LinkCreateLinkTokenRequest, LinkCompleteLinkTokenRequest
from datetime import timedelta, datetime, timezone

client = Client(
    client_id=os.getenv("PLAID_CLIENT_ID"),  # e.g., "***"
    environment=os.getenv("PLAID_ENVIRONMENT", "sandbox"),  # or "production"
)

# Create link token
link_request = LinkCreateLinkTokenRequest(
    product=[auth, transactions],
    client_user_agent="portfolio-management/1.0"
)
response = await client.link.create_link_token(link_request)
```

**TODO Locations:**

| Line Range | Endpoint | Current State | Implementation Needed |
|------------|----------|---------------|----------------------|
| 41-54 | `/plaid/items/create-link-token` | Mock token generation | Call `client.link.create_link_token()` with real parameters |
| 66-84 | `/plaid/items/{item_id}/link` | Mock access token | Use `PublicTokenExchangeRequest` to exchange public token for access token |
| 106-127 | `/plaid/items/{item_id}/accounts` | Mock account data | Fetch accounts via `client.accounts.get_accounts()` with pagination |
| 138-147 | `/plaid/items/{item_id}/refresh` | Mock refresh response | Call `client.transactions.getTransactions()` for transactions |

**Complete Implementation Template:**
```python
async def create_link_token_real(client: Client) -> dict[str, Any]:
    """Create link token with PlaidClient."""
    try:
        link_request = LinkCreateLinkTokenRequest(
            product=["auth", "transactions"],
            client_user_agent="portfolio-management/1.0"
        )
        response = await client.link.create_link_token(link_request)
        
        return {
            "link_token": response.link_token,
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=90)).isoformat(),
            "status": "success",
        }
    except Exception as e:
        return {"error": str(e), "status": "failed"}
```

**Note:** The current mock implementations are safe for schema foundation and testing. 
Replace gradually when production deployment is needed.

---

### 2. `trading_system/plaid/services.py` (Lines 136, 173, 203, 225, 244, 397)

#### Location: `/home/falcon/git/portfolio-management/trading_system/plaid/services.py`

**Pending:** Replace mock implementations and complete TODO sections

**TODO Locations:**

| Line | Method | Current State | Implementation Needed |
|------|--------|---------------|----------------------|
| 136 | `create_link_token()` | Mock link token | Call PlaidClient API |
| 173 | `link_item()` | Mock access token exchange | Use PublicTokenExchangeRequest |
| 203 | `get_balances()` | Mock balance data | Fetch from PlaidClient.accounts.getAccounts() |
| 225 | `get_transactions()` | Mock transaction data | Call PlaidClient.transactions.getTransactions() |
| 244 | `get_holdings()` | Mock holding data | Aggregate from positions API |
| 397 | Token encryption | TODO for secure storage | Implement Fernet encryption or Vault integration |

**PlaidClient Service Integration Pattern:**
```python
class PlaidService:
    def __init__(self, client_id: str, environment: str = "sandbox"):
        self.client = Client(
            client_id=client_id,
            environment=environment,
        )
    
    async def create_link_token(self) -> dict[str, Any]:
        # Line 136 - Replace mock implementation
        link_request = LinkCreateLinkTokenRequest(product=["auth", "transactions"])
        response = await self.client.link.create_link_token(link_request)
        return {
            "link_token": response.link_token,
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=90)).isoformat(),
            "status": "success"
        }
    
    async def link_item(self, public_token: str) -> dict[str, Any]:
        # Line 173 - Replace mock implementation
        exchange_request = PublicTokenExchangeRequest(
            public_token=public_token,
            client_secret=None  # Use session's client_secret if available
        )
        response = await self.client.items.get_item(exchange_request)
        
        # Extract access token and refresh token
        return {
            "item_id": response.item.item_id,
            "access_token": response.item.access_token,  # Must encrypt!
            "refresh_token": response.item.refresh_token or None,  # Store separately
            "consent_state": response.item.consent_state,
            "status": "success"
        }
```

**Webhook Signature Verification (Line 275 TODO):**
```python
import hmac
from hashlib import sha256

def verify_webhook_signature(payload: bytes, signature_header: str) -> bool:
    """Verify Plaid webhook HMAC signature."""
    secret = os.getenv("PLAID_WEBHOOK_SECRET")
    
    if not secret:
        raise ValueError("PLAID_WEBHOOK_SECRET not configured")
    
    expected_digest = hmac.new(
        secret.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected_digest, signature_header)

# Usage in handle_plaid_webhook:
def handle_plaid_webhook(payload: bytes, signature: str | None = None):
    if signature is None:
        raise ValueError("Missing webhook signature")
    
    try:
        if not verify_webhook_signature(payload, signature):
            log.error("Webhook signature verification failed")
            return {"status": "error", "message": "Invalid signature"}
    except Exception as e:
        log.error(f"Webhook verification error: {e}")
```

---

## Implementation Timeline (Optional)

### Phase 1a: Schema Foundation (CURRENT - Complete ✓)
- All models and tables defined
- Alembic migrations created
- Integration harness ready

### Phase 1b: PlaidClient Integration (PENDING)
- Install plaid-client package
- Replace mock implementations with real API calls  
- Test against Plaid sandbox environment

### Phase 1c: Production Deployment (PENDING)
- Configure production credentials
- Implement secure token storage (Vault/KMS)
- Enable webhook signature verification
- Deploy to staging environment

---

## Current Usage Options

### Option A: Continue Using Mock Implementations
- Safe for schema foundation and testing
- No PlaidClient required
- Gradual migration to real API as needed

### Option B: Full Implementation Now
- Install dependencies
- Replace TODO sections with PlaidClient calls
- Test against Plaid sandbox

---

## See Also

- `trading_system/docs/PHASE1_PLAID_COMPLETE.md` - Complete documentation
- `trading_system/plaid/api/plaid_routes.py` - API endpoints with TODOs
- `trading_system/plaid/services.py` - Service layer implementation guide
