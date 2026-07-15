"""
Plaid SQLAlchemy Models

Defines database tables for storing account aggregation data from Plaid.

Security Requirements:
- All access tokens are encrypted at rest (AES-256)
- Refresh tokens never logged or exposed via API
- Sensitive fields validated on write
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Mapped, mapped_column, relationship


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


from sqlalchemy import BigInteger, BLOB, ARRAY, DateTime, Enum, JSON, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models. Creates SQLite DB when no server available."""
    pass


class PlaidItem(Base):
    """Plaid Item (connected financial institution)."""
    
    __tablename__ = "plaid_items"
    
    # Primary key
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    item_id: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    
    # Institution metadata (from Plaid)
    institution_name: Mapped[str | None] = mapped_column(String(255))
    institution_website: Mapped[str | None] = mapped_column(String(255))
    institution_number: Mapped[str | None] = mapped_column(String(50))
    institution_logo_url: Mapped[str | None] = mapped_column(String(512))
    
    # Access token (encrypted at rest)
    access_token: Mapped[str | None] = mapped_column(String(512), nullable=True)
    refresh_token: Mapped[str | None] = mapped_column(String(512), nullable=True)
    
    # Consent tracking
    consent_state: Mapped[ConsentState] = mapped_column(Enum(ConsentState))
    consent_granted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consent_revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Status tracking
    status: Mapped[InstitutionStatus] = mapped_column(Enum(InstitutionStatus))
    inactive_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    # Environment configuration
    environment: Mapped[str] = mapped_column(String(20), default="sandbox")
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )
    
    # Relationships
    accounts: Mapped[list["PlaidAccount"]] = relationship(
        "PlaidAccount",
        primaryjoin="foreign(PlaidAccount.item_id) == PlaidItem.id",
        viewonly=True,
        cascade="all, delete-orphan"
    )
    
    events: Mapped[list["PlaidItemEvent"]] = relationship(
        "PlaidItemEvent",
        primaryjoin="foreign(PlaidItemEvent.item_id) == PlaidItem.id",
        viewonly=True,
        cascade="all, delete-orphan"
    )
    
    @property
    def is_active(self) -> bool:
        """Check if item is fully accessible."""
        return self.status == InstitutionStatus.ACTIVE
    
    @classmethod
    def from_plaid_item(cls, **kwargs) -> PlaidItem:
        """Create model from Plaid API response."""
        consent_state = ConsentState.from_plaid(kwargs.get("consent_state"))
        status = InstitutionStatus.from_plaid(kwargs.get("status"))
        
        return cls(
            id=kwargs.get("id", str(uuid.uuid4())),
            item_id=kwargs.get("item_id"),
            institution_name=kwargs.get("institution_name"),
            institution_website=kwargs.get("institution_website"),
            institution_number=kwargs.get("institution_number"),
            access_token=kwargs.get("access_token"),  # Stored encrypted by service layer
            refresh_token=kwargs.get("refresh_token"),  # Never exposed to API
            consent_state=consent_state,
            consent_granted_at=kwargs.get("created_at"),
            consent_revoked_at=kwargs.get("deleted_at"),
            status=status,
            inactive_reason=kwargs.get("inactive_reason") or (status == InstitutionStatus.INACTIVE),
            environment=kwargs.get("environment", "sandbox"),
        )


class PlaidAccount(Base):
    """Financial account from Plaid item."""
    
    __tablename__ = "plaid_accounts"
    
    # Primary key (composite: item + account)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    item_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    
    # Account classification
    account_type: Mapped[str] = mapped_column(String(50))
    sub_type: Mapped[str | None] = mapped_column(String(100))
    
    # Institution linkage
    institution_id: Mapped[str | None] = mapped_column(String(50), index=True)
    institution_name: Mapped[str | None] = mapped_column(String(255))
    
    # Account ownership details
    official_name: Mapped[str | None] = mapped_column(String(512))
    owner_types: Mapped[list[str]] = mapped_column(ARRAY(String(50)), default=list)
    
    # Current balances (refreshed periodically)
    available_balance_cents: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    current_balance_cents: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    masked_account_number: Mapped[str | None] = mapped_column(String(50), nullable=True)
    
    # Status tracking
    status: Mapped[InstitutionStatus] = mapped_column(Enum(InstitutionStatus))
    inactive_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )
    
    # Relationships
    item: Mapped[PlaidItem] = relationship(
        "PlaidItem",
        primaryjoin="foreign(PlaidAccount.item_id) == PlaidItem.id",
        viewonly=True
    )
    transactions: Mapped[list["PlaidTransaction"]] = relationship(
        "PlaidTransaction",
        primaryjoin="foreign(PlaidTransaction.account_id) == PlaidAccount.account_id",
        viewonly=True,
        cascade="all, delete-orphan"
    )
    
    @property
    def balance(self) -> int | None:
        """Get available balance in dollars."""
        return self.available_balance_cents // 100 if self.available_balance_cents else None
    
    @property
    def is_checking(self) -> bool:
        """Check if account is a checking account."""
        acc_type = self.account_type.lower()
        sub_type = (self.sub_type or "").lower()
        return acc_type in ("checking", "demand") or "checking" in sub_type


class PlaidTransaction(Base):
    """Transaction from financial account."""
    
    __tablename__ = "plaid_transactions"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    item_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    pnc_identifier: Mapped[str | None] = mapped_column(String(255), nullable=True)
    
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    amount_cents: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    
    name: Mapped[str | None] = mapped_column(String(512))
    merchant_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    sub_category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    location_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    
    status: Mapped[InstitutionStatus] = mapped_column(Enum(InstitutionStatus))
    transaction_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    transaction_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    account: Mapped[PlaidAccount] = relationship(
        "PlaidAccount",
        primaryjoin="foreign(PlaidTransaction.account_id) == PlaidAccount.account_id",
        viewonly=True
    )
    
    @property
    def amount(self) -> int | None:
        """Get amount in dollars."""
        return self.amount_cents // 100 if self.amount_cents else None


class PlaidItemEvent(Base):
    """Audit trail for Plaid item events."""
    
    __tablename__ = "plaid_item_events"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    item_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(50))
    event_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    source_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    item: Mapped[PlaidItem] = relationship(
        "PlaidItem",
        primaryjoin="foreign(PlaidItemEvent.item_id) == PlaidItem.id",
        viewonly=True
    )


class PlaidWebhook(Base):
    """Plaid webhook event tracking."""
    
    __tablename__ = "plaid_webhooks"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    item_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    webhook_type: Mapped[str] = mapped_column(String(50))
    webhook_payload: Mapped[dict] = mapped_column(JSON)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    processed_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class PlaidCredentials(Base):
    """Secure storage for Plaid credentials."""
    
    __tablename__ = "plaid_credentials"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default="credentials")
    client_id: Mapped[str] = mapped_column(String(100), nullable=False)
    environment: Mapped[str] = mapped_column(String(20), default="sandbox", nullable=False)
    credentials_encrypted: Mapped[bytes] = mapped_column(BLOB, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )
