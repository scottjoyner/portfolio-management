"""
Plaid Integration Package

Provides account aggregation capabilities via Plaid API.

Architecture:
┌───────────────────────────────────────────────────────┐
│              Plaid Package (trading_system.plaid)     │
├───────────────────────────────────────────────────────┤
│                                                       │
│  models.py         │ database_models.py               │
│  ─────────────     │ ─────────────────────────────    │
│  data classes      │ SQLAlchemy tables               │
│  (public API)      │ (database schema)               │
│                   └──────────────────┬───────────────┘
│                                      │
│  services.py       │ plaid_client.py                 │
│  ─────────────     │ ─────────────────────────────    │
│  orchestration     │ API client wrapper              │
│  & security        │ (encapsulates Plaid library)    │
│                   └──────────────────┬───────────────┘
│                                      │
│  api/plaid_routes.py                                    │
│  ─────────────────────                      │
│  REST endpoints for integration              │
│         with FastAPI                          │
│                                      │
└───────────────────────────────────────────────────────┘

Security Model:
- All access tokens encrypted at rest (AES-256 via Fernet)
- Refresh tokens never exposed to API responses
- Webhook signature verification for security events
- Automatic token expiration monitoring and renewal
- Audit trail for all consent changes

Setup (one-time):
1. Generate encryption key:
   from cryptography.fernet import Fernet
   encryption_key = Fernet.generate_key()
   # Store securely (e.g., Vault, AWS KMS)

2. Initialize service:
   from plaid.services import initialize_plaid_service
   
   service = await initialize_plaid_service(
       client_id="your_plaid_client_id",
       environment="sandbox"  # or "production"
   )

Example Integration:
```python
from trading_system.plaid.services import PlaidService
from trading_system.plaid.database_models import PlaidItem, PlaidAccount

# Create link token for user consent
link_token = await service.create_link_token(product_scope=["auth", "transactions"])

# Present link flow to user (using Plaid Web UI or mobile SDK)
# ... after user approves ...

# Link item and get access token
item_result = await service.link_item(item_id="uuid", public_token="pt_user_abc")
access_token_encrypted = item_result["access_token_encrypted"]

# Store in database (already encrypted!)
item = PlaidItem(
    item_id="uuid",
    access_token_encrypted=access_token_encrypted,  # Safe to store!
    consent_state="granted",
    status="active"
)
database.add(item)

# Periodically refresh data
await service.refresh_item(item_id="uuid")
```
"""

from .models import (
    PlaidCredentials,
    PlaidItem,
    PlaidAccount,
    PlaidTransaction,
    ConsentState,
    InstitutionStatus,
)

from .database_models import (
    Base,
    PlaidItem as DatabasePlaidItem,
    PlaidAccount as DatabasePlaidAccount,
    PlaidTransaction as DatabasePlaidTransaction,
    PlaidItemEvent,
    PlaidWebhook,
    PlaidCredentials as DatabasePlaidCredentials,
)

from .services import (
    PlaidService,
    CredentialVault,
    VaultManager,
    initialize_plaid_service,
    generate_encryption_key,
    validate_token_expiration,
)

__version__ = "0.1.0"
__all__ = [
    # Models
    "PlaidCredentials",
    "PlaidItem", 
    "PlaidAccount",
    "PlaidTransaction",
    "ConsentState",
    "InstitutionStatus",
    # Database models (for SQLAlchemy integration)
    "Base",
    "DatabasePlaidItem",
    "DatabasePlaidAccount",
    "DatabasePlaidTransaction",
    "PlaidItemEvent",
    "PlaidWebhook",
    "DatabasePlaidCredentials",
    # Services
    "PlaidService",
    "CredentialVault", 
    "VaultManager",
    "initialize_plaid_service",
    "generate_encryption_key",
    "validate_token_expiration",
]
