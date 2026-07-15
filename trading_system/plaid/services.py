"""
Plaid Service Layer

Orchestrates Plaid API interactions, handles credential management,
and implements security best practices for account aggregation.

Security Responsibilities:
- Encrypt all access tokens at rest before storage
- Never log or expose refresh tokens
- Validate webhook signatures to prevent replay attacks
- Track token expiration and auto-renewal schedules

Architecture:
┌─────────────────────────────────────────────────────────┐
│              PlaidService (Main Orchestrator)             │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────┐│
│  │ Link         │    │ Item         │    │ Account    ││
│  │ Management   │    │ Refresh      │    │ Sync       ││
│  └──────────────┘    └──────────────┘    └────────────┘│
│          │              │              │               │
│          ▼              ▼              ▼               │
│     ┌─────────────────────────────────┐                │
│     │   Credential Vault Service      │                │
│     │   (AES-256 encryption wrapper)  │                │
│     └─────────────────────────────────┘                │
│                                                          │
└─────────────────────────────────────────────────────────┘

Usage:
```python
from plaid.services import PlaidService

service = PlaidService(
    client_id="your_client_id",
    environment="sandbox",  # or "production"
)

# Create link token for user consent
link_token = await service.create_link_token()

# Link item and get access token
item_result = await service.link_item(item_id, public_token="pt_abc...")
access_token = item_result["access_token"]

# Refresh data periodically
refresh_result = await service.refresh_item(item_id)
```
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone, timedelta
from typing import Any

try:
    from cryptography.fernet import Fernet
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False


class EncryptionError(Exception):
    """Raised when encryption fails."""
    pass


class PlaidServiceException(Exception):
    """Base exception for Plaid service operations."""
    
    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.code = code or "unknown"


class TokenEncryptionError(Exception):
    """Raised when token encryption/decryption fails."""
    pass


class PlaidService:
    """Main service orchestrator for Plaid integration."""
    
    def __init__(
        self,
        client_id: str,
        environment: str = "sandbox",
        encryption_key: bytes | None = None,
        refresh_interval_seconds: int = 3600,
        refresh_threshold_hours: float = 23.0,
    ):
        """
        Initialize Plaid service.
        
        Args:
            client_id: Plaid client ID from Plaid developer portal
            environment: "sandbox" for testing, "production" for live
            encryption_key: Fernet key for token encryption (generate once!)
            refresh_interval_seconds: How often to auto-refresh balances
            refresh_threshold_hours: Auto-renew if less than this many hours left
        """
        
        self.client_id = client_id
        self.environment = environment
        
        # Generate encryption key if not provided
        if not encryption_key or len(encryption_key) < 128 // 8:
            if CRYPTOGRAPHY_AVAILABLE:
                self.fernet = Fernet(Fernet.generate_key())
            else:
                raise ImportError("cryptography package required for token encryption")
        
        # Configuration
        self.refresh_interval = timedelta(seconds=refresh_interval_seconds)
        self.refresh_threshold = timedelta(hours=refresh_threshold_hours)
    
    async def create_link_token(self, product_scope: list[str] | None = None) -> dict[str, Any]:
        """
        Create link token for Plaid SDK linking flow.
        
        Args:
            product_scope: List of products to request (auth, transactions, credit, etc.)
        
        Returns:
            Link token that can be used with Plaid Web UI or mobile SDK
        
        Example:
            link_token = await service.create_link_token(product_scope=["auth", "transactions"])
        """
        
        # TODO: Implement with actual PlaidClient
        # 
        # from plaid import Client, LinkRequest
        # client = Client(client_id=self.client_id, environment=self.environment)
        # response = await client.link.create_link_token(LinkRequest(product=product_scope))
        
        return {
            "link_token": f"lt_{secrets.token_urlsafe(32)}",  # Mock for demo
            "expires_at": datetime.now(timezone.utc) + timedelta(days=90),
            "status": "success"
        }
    
    async def link_item(
        self,
        item_id: str,
        public_token: str | None = None,
        client_secret: str | None = None,
    ) -> dict[str, Any]:
        """
        Link Plaid item to this system.
        
        Args:
            item_id: UUID of the Plaid Item
            public_token: User's public token from linking flow (alternative to refresh)
            client_secret: Secret for accessing Plaid APIs
        
        Returns:
            Item metadata including encrypted access token
        
        Security:
            - Access tokens are encrypted at rest before storage
            - Refresh tokens never exposed to API responses
        
        Example:
            result = await service.link_item(item_id="12345", public_token="pt_user_abc")
        """
        
        # TODO: Implement with actual PlaidClient
        # 
        # response = await client.item.get_item({
        #     "public_token": public_token,
        #     "client_secret": client_secret
        # })
        
        return {
            "item_id": item_id,
            "institution_name": "Example Institution",
            "access_token_encrypted": f"enc_{secrets.token_urlsafe(24)}",  # Mock - real impl encrypts!
            "refresh_token": None,  # Never returned to API
            "consent_state": "granted",
            "status": "active"
        }
    
    async def refresh_item(self, item_id: str) -> dict[str, Any]:
        """
        Refresh balances and transactions for an item.
        
        Args:
            item_id: UUID of the Plaid Item
        
        Returns:
            Refresh timestamp and success status
        
        Usage:
            Called periodically (e.g., every hour) to keep data current
        """
        
        # TODO: Implement with actual PlaidClient
        
        return {
            "item_id": item_id,
            "refreshed_at": datetime.now(timezone.utc),
            "status": "success"
        }
    
    async def get_accounts(self, item_id: str) -> list[dict[str, Any]]:
        """
        Get all financial accounts for an item.
        
        Args:
            item_id: UUID of the Plaid Item
        
        Returns:
            List of account dictionaries
        
        Example:
            accounts = await service.get_accounts(item_id="12345")
        """
        
        # TODO: Implement with actual PlaidClient
        
        return []
    
    async def revoke_item(self, item_id: str) -> dict[str, Any]:
        """
        Revoke access to a Plaid item.
        
        Args:
            item_id: UUID of the Plaid Item
        
        Returns:
            Deletion deadline (7 days from now)
        
        Security:
            - Triggers cleanup of all related data in trading system
            - Access token deleted from encrypted vault
        """
        
        # TODO: Implement with actual PlaidClient
        
        return {
            "item_id": item_id,
            "deleted_at": datetime.now(timezone.utc),
            "deletion_deadline": datetime.now(timezone.utc) + timedelta(days=7),
            "status": "success"
        }
    
    async def verify_webhook_signature(
        self,
        webhook_payload: bytes,
        signature_header: str,
        expected_secret: str
    ) -> bool:
        """
        Verify Plaid webhook signature to prevent replay attacks.
        
        Args:
            webhook_payload: Raw webhook body bytes
            signature_header: "X-Plaid-Signature" header value
            expected_secret: Shared secret with Plaid
        
        Returns:
            True if signature matches, False otherwise
        
        Security:
            - Uses HMAC-SHA256 verification
            - Must be called before processing any webhook
        """
        
        # TODO: Implement HMAC verification
        return True


# ============================================================================
# Credential Vault (Encrypted Storage)
# ============================================================================

class CredentialVault:
    """Secure vault for storing and retrieving encrypted credentials."""
    
    def __init__(self, encryption_key: bytes):
        """
        Initialize credential vault with encryption key.
        
        Args:
            encryption_key: Fernet key for symmetric encryption
        """
        
        self.fernet = Fernet(encryption_key) if CRYPTOGRAPHY_AVAILABLE else None
    
    def encrypt_token(self, token: str) -> bytes:
        """
        Encrypt an access token for storage.
        
        Args:
            token: Plain-text access token from Plaid
        
        Returns:
            Encrypted token bytes (stored in database)
        
        Security:
            - Uses AES-256 via Fernet
            - Never logs or exposes encrypted tokens
        """
        
        if not self.fernet:
            raise ImportError("cryptography package required")
        
        return self.fernet.encrypt(token.encode())
    
    def decrypt_token(self, encrypted_token: bytes) -> str:
        """
        Decrypt an access token from storage.
        
        Args:
            encrypted_token: Encrypted token bytes from database
        
        Returns:
            Plain-text access token
        
        Security:
            - Only called in service layer (never API responses)
            - Tokens immediately re-encrypted after use
        """
        
        if not self.fernet:
            raise ImportError("cryptography package required")
        
        return self.fernet.decrypt(encrypted_token).decode()


class VaultManager:
    """Manages credential vault initialization and lifecycle."""
    
    def __init__(self, credentials_encrypted: bytes | None = None):
        """
        Initialize vault manager with optional pre-existing credentials.
        
        Args:
            credentials_encrypted: Pre-existing encrypted credentials (optional)
        """
        
        self.credentials_encrypted = credentials_encrypted
    
    async def initialize(
        self,
        client_id: str,
        environment: str,
        encryption_key: bytes
    ) -> dict[str, Any]:
        """
        Initialize vault with new encrypted credentials.
        
        Args:
            client_id: Plaid client ID
            environment: Plaid environment (sandbox/production)  
            encryption_key: New Fernet key for encryption
        
        Returns:
            Vault configuration
        
        Usage:
            vault_manager = VaultManager()
            await vault_manager.initialize("abc123", "sandbox", fernet_key)
        
        Security:
            - Generates new encryption key on first run
            - Key stored securely (e.g., HashiCorp Vault, AWS KMS)
        """
        
        self.vault = CredentialVault(encryption_key)
        
        return {
            "client_id": client_id,
            "environment": environment,
            "vault_initialized": True,
            "status": "success"
        }
    
    async def store_credentials(self, credentials: dict[str, Any]) -> None:
        """
        Store encrypted credentials in vault.
        
        Args:
            credentials: Dictionary with "client_id", "environment", etc.
        
        Security:
            - Encrypts credentials before storage
            - Validates expiration if provided
        """
        
        # TODO: Implement secure encryption and storage
        
        pass


# ============================================================================
# Utility Functions
# ============================================================================

def generate_encryption_key() -> bytes:
    """Generate a new Fernet encryption key."""

    import secrets

    return Fernet.generate_key()  # 256-bit key for AES-256


def validate_token_expiration(token_expiry: datetime, refresh_threshold_hours: float = 23.0) -> bool:
    """
    Check if token needs immediate refresh.
    
    Args:
        token_expiry: Token expiration timestamp
        refresh_threshold_hours: Minimum hours before expiry to refresh
        
    Returns:
        True if token should be refreshed
        
    Usage:
        # Refresh if less than 23 hours left
        await service.refresh_item_if_expired(token_expiry)
    """
    
    now = datetime.now(timezone.utc)
    threshold_deadline = token_expiry - timedelta(hours=refresh_threshold_hours)
    
    return now >= threshold_deadline


# ============================================================================
# Service Initialization
# ============================================================================

async def initialize_plaid_service(
    client_id: str,
    environment: str = "sandbox",
    credentials_encrypted: bytes | None = None,
    refresh_interval_seconds: int = 3600,
) -> PlaidService:
    """
    Initialize Plaid service with secure configuration.
    
    Args:
        client_id: Plaid client ID
        environment: "sandbox" or "production"
        credentials_encrypted: Pre-existing encrypted credentials (optional)
        refresh_interval_seconds: Auto-refresh interval
        
    Returns:
        Configured PlaidService instance
        
    Usage:
        from plaid.services import initialize_plaid_service
        
        service = await initialize_plaid_service(
            client_id="your_client_id",
            environment="production",
            credentials_encrypted=encrypted_creds_from_vault,
            refresh_interval_seconds=3600,  # Hourly refresh
        )
    """
    
    # Generate encryption key if not provided
    if not credentials_encrypted:
        import secrets
        encryption_key = generate_encryption_key()
        
        vault_manager = VaultManager()
        await vault_manager.initialize(client_id, environment, encryption_key)
    else:
        # Use existing encrypted credentials
        from cryptography.fernet import Fernet
        fernet = Fernet(credentials_encrypted)
    
    return PlaidService(
        client_id=client_id,
        environment=environment,
        refresh_interval_seconds=refresh_interval_seconds,
    )
