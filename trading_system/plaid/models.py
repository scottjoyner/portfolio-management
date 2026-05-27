"""
Plaid Integration Module

Provides secure integration with Plaid API for account aggregation,
including balances, holdings, transactions, and institution metadata.

Features:
- Plaid Item management (linking, refreshing, revoking)
- Account data synchronization (balances, positions, transactions)
- Secure token handling with encryption at rest
- Webhook handling for consent and refresh events
- Audit trail for all access token operations

Architecture:
┌─────────────────────────────────────────────────────┐
│              Plaid Ingestion Layer                    │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌────────┐│
│  │ Webhook      │    │ Polling      │    │ REST   ││
│  │ Handler      │    │ Service      │    │ API    ││
│  │ (consent)    │    │ (refresh)    │    │        ││
│  └──────────────┘    └──────────────┘    └────────┘│
│              │              │              │       │
│              ▼              ▼              ▼       │
│         ┌─────────────────────────────────┐       │
│         │   Event Queue (Redis)           │       │
│         └─────────────────────────────────┘       │
│                              │                    │
│                              ▼                    │
│                   ┌──────────────┐               │
│                   │ SQLAlchemy   │               │
│                   │ Repository   │               │
│                   └──────────────┘               │
│                                                      │
└─────────────────────────────────────────────────────┘

Security Notes:
- Access tokens are encrypted at rest (AES-256)
- Never log access tokens or refresh tokens
- Token expiration is tracked and enforced
- All API calls use secure Plaid sandbox environment in dev

Usage Example:
```python
from plaid.api.plaid_service import PlaidService

# Initialize with secure credentials
service = PlaidService(
    client_id="your_client_id",
    environment="sandbox",  # or "production"
)

# Link account
link_token = await service.create_link_token()
item_result = await service.link_item(link_token, public_token="pt_abc...")
access_token = item_result.access_token

# Fetch balances and holdings
balances = await service.get_balances(access_token)
transactions = await service.get_transactions(access_token)
```
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine


class InstitutionStatus(Enum):
    """Institution access states."""
    
    ACTIVE = "active"  # Fully accessible
    READ_ONLY = "read_only"  # Limited access (compliance/restricted)
    INACTIVE = "inactive"  # Consent revoked or suspended
    
    @classmethod
    def from_plaid(cls, status: str | None) -> InstitutionStatus:
        """Convert Plaid status string to enum."""
        if status is None:
            return cls.ACTIVE
        
        status_lower = status.lower()
        
        if status_lower in ("active", "a"):
            return cls.ACTIVE
        elif status_lower in ("read_only", "r"):
            return cls.READ_ONLY
        else:
            return cls.INACTIVE


class ConsentState(Enum):
    """Plaid consent states."""
    
    PENDING = "pending"  # Link request pending user approval
    GRANTED = "granted"  # User granted access
    REVOKED = "revoked"  # User revoked or expired
    
    @classmethod
    def from_plaid(cls, state: str | None) -> ConsentState:
        """Convert Plaid state string to enum."""
        if state is None:
            return cls.GRANTED
        
        state_lower = state.lower()
        
        if state_lower in ("pending", "p"):
            return cls.PENDING
        elif state_lower in ("granted", "g"):
            return cls.GRANTED
        else:
            return cls.REVOKED


@dataclass(frozen=True)
class PlaidCredentials:
    """Encrypted credentials container."""
    
    client_id: str
    environment: str  # sandbox, staging, or production
    secret: str
    
    def validate(self) -> None:
        """Validate credential format."""
        if not self.client_id:
            raise ValueError("Client ID required")
        if self.environment not in ("sandbox", "staging", "production"):
            raise ValueError(f"Invalid environment: {self.environment}")


@dataclass
class PlaidItem:
    """Represents a Plaid Item (connected institution)."""
    
    item_id: str  # UUID from Plaid
    access_token: str | None = None  # May be null if pending link
    
    # Institution metadata
    institution_name: str | None = None
    institution_website: str | None = None
    institution_number: str | None = None
    
    # Consent state
    consent_state: ConsentState = ConsentState.GRANTED
    
    # Refresh status
    refresh_token: str | None = None  # For token refresh operations
    last_refreshed_at: datetime | None = None
    
    # Status tracking
    status: InstitutionStatus = InstitutionStatus.ACTIVE
    inactive_reason: str | None = None
    
    # Timestamps
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def is_active(self) -> bool:
        """Check if item is fully accessible."""
        return self.status == InstitutionStatus.ACTIVE
    
    @property
    def consent_granted(self) -> bool:
        """Check if user has granted access."""
        return self.consent_state in (ConsentState.GRANTED, ConsentState.PENDING)


@dataclass
class PlaidAccount:
    """Represents a financial account from Plaid."""
    
    account_id: str  # Plaid account ID
    account_type: str  # checking, savings, credit_line, etc.
    sub_type: str | None = None  # More specific classification
    
    # Institution linkage
    institution_id: str | None = None
    institution_name: str | None = None
    
    # Account ownership
    official_name: str | None = None
    owner_types: list[str] = field(default_factory=list)
    
    # Current values (refreshed periodically)
    available_balance_cents: int | None = None
    current_balance_cents: int | None = None
    masked_account_number: str | None = None
    
    # Status tracking
    status: InstitutionStatus = InstitutionStatus.ACTIVE
    
    # Timestamps
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def balance(self) -> int | None:
        """Get available balance in cents (convert to dollars)."""
        return self.available_balance_cents // 100 if self.available_balance_cents else None
    
    @property
    def is_checking(self) -> bool:
        """Check if account is a checking account."""
        return self.account_type.lower() == "checking" or \
               ("checking" in (self.sub_type or "").lower())


@dataclass
class PlaidTransaction:
    """Represents a transaction from Plaid."""
    
    transaction_id: str  # Plaid transaction ID
    pnc_identifier: str | None = None  # PNC-specific ID
    
    # Transaction details
    account_id: str | None = None
    date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    amount_cents: int | None = None
    
    # Classification
    name: str | None = None
    merchant_name: str | None = None
    category: str | None = None
    sub_category: str | None = None
    location_name: str | None = None
    
    # Status
    status: InstitutionStatus = InstitutionStatus.ACTIVE
    
    # Additional metadata
    transaction_code: str | None = None
    transaction_type: str | None = None
    
    @property
    def amount(self) -> int | None:
        """Get amount in cents (convert to dollars)."""
        return self.amount_cents // 100 if self.amount_cents else None
