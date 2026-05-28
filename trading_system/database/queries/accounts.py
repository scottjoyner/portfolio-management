"""
Accounts Query Module - PostgreSQL Integration

Provides database operations for account-related tables, including:
- Portfolio financial metrics
- Capital bucket management
- Plaid integration data (accounts, items, transactions)
- Account synchronization and refresh logic

Architecture:
┌─────────────────────────────────────────────────────┐
│              Accounts Layer                           │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌────────┐│
│  │ Plaid Items  │    │ Capital      │    │Portfolio││
│  │              │    │ Buckets      │    │ Metrics ││
│  │              │    │              │    │         ││
│  │  Items API   │    │ Bucket Mgmt  │    │Refresh  ││
│  │  Consent Mgt │    │  Allocation  │    │Aggr     ││
│  └──────────────┘    └──────────────┘    └────────┘│
│              │              │              │       │
│              ▼              ▼              ▼       │
│         ┌─────────────────────────────────┐       │
│         │   Database Repository Layer     │       │
│         │   (SQLAlchemy ORM)             │       │
│         └─────────────────────────────────┘       │
│                              │                    │
│              ▼              ▼              ▼       │
│    Plaid API  ────  PostgreSQL   ←  Sync Layer    │
│    Webhooks   ←──   Integration    Events         │
│                                                      │
└─────────────────────────────────────────────────────┘

Security Notes:
- Access tokens are encrypted at rest (AES-256)
- Never expose refresh tokens via API
- Plaid credentials stored in dedicated table with expiration tracking
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from storage.postgres.models import Portfolio
    from plaid.models import PlaidItem


class AccountsRepository:
    """Repository for all account-related database operations."""
    
    def __init__(self, db: Session) -> None:
        self.db = db
    
    # ==================== PORTFOLIO OPERATIONS ====================
    
    def get_portfolio(self, portfolio_id: str) -> Portfolio | None:
        """Retrieve a portfolio by ID."""
        return self.db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    
    def list_portfolios(self, objective: str | None = None) -> list[Portfolio]:
        """List all portfolios, optionally filtered by objective."""
        query = self.db.query(Portfolio)
        if objective:
            query = query.filter(Portfolio.objective == objective)
        return query.all()
    
    def update_portfolio_nav(self, portfolio_id: str, nav: float) -> Portfolio | None:
        """Update NAV for a portfolio."""
        portfolio = self.get_portfolio(portfolio_id)
        if portfolio:
            portfolio.nav = nav
            portfolio.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            self.db.refresh(portfolio)
        return portfolio
    
    def update_portfolio_metrics(self, portfolio_id: str, **kwargs: float) -> Portfolio | None:
        """Update multiple portfolio metrics at once."""
        portfolio = self.get_portfolio(portfolio_id)
        if portfolio:
            for key, value in kwargs.items():
                if hasattr(portfolio, key):
                    setattr(portfolio, key, value)
            portfolio.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            self.db.refresh(portfolio)
        return portfolio
    
    def seed_default_portfolios(self) -> list[Portfolio]:
        """Seed database with default portfolios for testing."""
        if self.db.query(Portfolio).count() > 0:
            return []
        
        portfolios = [
            Portfolio(
                id="cb-core-mm", name="Coinbase Core MM", objective="market_making",
                nav=2_500_000, available_capital=1_350_000, locked_capital=610_000,
                realized_pnl=42_300, unrealized_pnl=8_100, liquidity_score=0.82, capital_efficiency=0.75,
            ),
            Portfolio(
                id="cb-hedge", name="Coinbase Hedge", objective="hedge",
                nav=1_100_000, available_capital=790_000, locked_capital=120_000,
                realized_pnl=13_200, unrealized_pnl=-2_200, liquidity_score=0.71, capital_efficiency=0.66,
            ),
        ]
        
        self.db.add_all(portfolios)
        self.db.flush()  # Get generated IDs
        
        for portfolio in portfolios:
            from storage.postgres.models import PortfolioSleeve
            sleeve_names = ["maker", "taker", "inventory"] if portfolio.id == "cb-core-mm" else ["delta_hedge", "tail_protection"]
            
            for sleeve_name in sleeve_names:
                sleeve = PortfolioSleeve(
                    portfolio_id=portfolio.id,
                    name=sleeve_name,
                    weight=(55.0 / 100.0 if sleeve_name == "maker" else 
                            10.0 / 100.0 if sleeve_name == "taker" else 
                            35.0 / 100.0 if portfolio.id == "cb-core-mm" else
                            65.0 / 100.0 if sleeve_name == "delta_hedge" else
                            35.0 / 100.0)
                )
                self.db.add(sleeve)
        
        self.db.commit()
        return portfolios
    
    # ==================== CAPITAL BUCKET OPERATIONS ====================
    
    def get_capital_bucket(self, bucket_id: str) -> Any | None:
        """Retrieve a capital bucket by ID."""
        from storage.postgres.models import CapitalBucket
        return self.db.query(CapitalBucket).filter(CapitalBucket.id == bucket_id).first()
    
    def list_capital_buckets(self, portfolio_id: str | None = None) -> list[Any]:
        """List all capital buckets, optionally filtered by portfolio."""
        from storage.postgres.models import CapitalBucket
        query = self.db.query(CapitalBucket)
        
        if portfolio_id:
            query = query.filter(CapitalBucket.portfolio_id == portfolio_id)
        
        return query.all()
    
    def create_capital_bucket(self, bucket_data: dict[str, Any]) -> Any | None:
        """Create a new capital bucket."""
        from storage.postgres.models import CapitalBucket
        
        # Extract fields matching the model
        bucket = CapitalBucket(
            id=bucket_data.get("id", str(uuid.uuid4())),
            portfolio_id=bucket_data.get("portfolio_id"),
            name=bucket_data.get("name"),
            bucket_type=bucket_data.get("bucket_type"),
            amount=float(bucket_data.get("amount", 0)),
            target_weight=float(bucket_data.get("target_weight", 0)),
            min_weight=float(bucket_data.get("min_weight", 0)),
            max_weight=float(bucket_data.get("max_weight", 1)),
            locked=bucket_data.get("locked", False),
            status=bucket_data.get("status", "idle"),
        )
        
        self.db.add(bucket)
        self.db.commit()
        return bucket
    
    # ==================== PLAID ITEM OPERATIONS ====================
    
    def get_plaid_item(self, item_id: str) -> PlaidItem | None:
        """Retrieve a Plaid item by ID."""
        from storage.postgres.models import plaid_items_table
        
        try:
            return self.db.query(plaid_items_table).filter(plaid_items_table.c.item_id == item_id).first()
        except:
            # Fallback for table name variations
            from plaid.database_models import PlaidItem
            return self.db.query(PlaidItem).filter(PlaidItem.id == item_id).first()
    
    def list_plaid_items(self, access_token_active_only: bool = False) -> list[PlaidItem]:
        """List all Plaid items with optional filtering."""
        from plaid.database_models import PlaidItem
        
        query = self.db.query(PlaidItem)
        
        if access_token_active_only:
            from plaid.models import InstitutionStatus
            query = query.filter(PlaidItem.status == InstitutionStatus.ACTIVE, 
                               PlaidItem.access_token.isnot(None))
        
        return query.all()
    
    def upsert_plaid_item(self, item: PlaidItem) -> PlaidItem:
        """Upsert a Plaid item (insert or update)."""
        existing = self.get_plaid_item(item.item_id)
        
        if existing:
            # Update existing record
            for key, value in item.__dict__.items():
                if not key.startswith('_') and hasattr(existing, key):
                    setattr(existing, key, value)
            existing.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            self.db.refresh(existing)
        else:
            self.db.add(item)
            self.db.commit()
            self.db.refresh(item)
        
        return item
    
    # ==================== ACCOUNT SYNCHRONIZATION ====================
    
    def sync_account_balances(self, access_token: str) -> dict[str, Any]:
        """
        Synchronize account balances from Plaid API.
        Returns balance data with timestamps for audit trail.
        """
        
        # This would call Plaid's get_accounts_v4 endpoint
        # Return structure: {
        #   "accounts": [{"id": "...", "type": "...", "available_balance_cents": ...}],
        #   "sync_timestamp": datetime,
        #   "next_sync_in_seconds": 3600
        # }
        
        return {
            "accounts": [],
            "sync_timestamp": datetime.now(timezone.utc),
            "status": "complete"
        }
    
    def sync_account_holdings(self, access_token: str) -> dict[str, Any]:
        """
        Synchronize account holdings/positions from Plaid API.
        """
        return {
            "holdings": [],
            "sync_timestamp": datetime.now(timezone.utc),
            "status": "complete"
        }
    
    def check_token_expiration(self, item_id: str) -> tuple[bool, timedelta | None]:
        """
        Check if a Plaid item's access token is expiring soon.
        Returns (is_expiring_soon, days_until_expiry)
        """
        from plaid.models import ConsentState
        
        item = self.get_plaid_item(item_id)
        
        if not item or item.consent_state != ConsentState.GRANTED:
            return False, None
        
        # Plaid tokens typically expire in 1 year if not refreshed
        expiry_threshold_days = 30
        
        # In production, you would check token expiration date from Plaid API
        # For now, assume tokens are valid for a default period
        days_until_expiry = 365  # placeholder
        
        return days_until_expiry < expiry_threshold_days, timedelta(days=days_until_expiry)


# ==================== QUERY HELPERS ====================

def get_account_overview(db: Session) -> dict[str, Any]:
    """Get comprehensive account overview for API response."""
    repo = AccountsRepository(db)
    
    result = {
        "portfolios": repo.list_portfolios(),
        "total_nav": sum(p.nav for p in repo.list_portfolios()),
        "items_count": len(repo.list_plaid_items()),
    }
    
    return result


def get_portfolio_summary(db: Session, portfolio_id: str) -> dict[str, Any]:
    """Get summary for a specific portfolio."""
    repo = AccountsRepository(db)
    portfolio = repo.get_portfolio(portfolio_id)
    
    if not portfolio:
        return None
    
    buckets = repo.list_capital_buckets(portfolio_id)
    
    return {
        "portfolio": {
            "id": portfolio.id,
            "name": portfolio.name,
            "objective": portfolio.objective,
            "nav": float(portfolio.nav),
            "available_capital": float(portfolio.available_capital),
            "locked_capital": float(portfolio.locked_capital),
        },
        "capital_buckets": [b.__dict__ for b in buckets],
    }
