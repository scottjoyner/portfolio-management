"""
Database Queries Module - PostgreSQL Integration Layer

This module provides SQLAlchemy-based repository patterns for all 
database operations, integrating with the existing PostgreSQL schema.

Usage:
```python
from trading_system.database.queries import (
    AccountsRepository,
    PositionsRepository,
    TradesRepository,
)

# Initialize with database session
db = get_db_session()
accounts_repo = AccountsRepository(db)
positions_repo = PositionsRepository(db)
trades_repo = TradesRepository(db)

# Example: Get portfolio summary
portfolio = accounts_repo.get_portfolio("cb-core-mm")
```

Architecture:
┌─────────────────────────────────────────────────────┐
│           Database Query Modules                      │
├─────────────────────────────────────────────────────┤
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐│
│  │ Accounts     │ │ Positions    │ │ Trades       ││
│  │ Repository   │ │ Repository   │ │ Repository   ││
│  └──────────────┘ └──────────────┘ └──────────────┘│
│              │              │              │       │
│              ▼              ▼              ▼       │
│         Portfolio     Position    Order/Fill      │
│         Capital      Delta        Lifecycle       │
│                              Management           │
│                                                      │
└─────────────────────────────────────────────────────┘

All repositories follow the same pattern:
- Initialize with SQLAlchemy Session
- Provide get/list/update/create operations
- Return dictionaries for API compatibility
- Include helper query functions at module level
"""

from trading_system.database.queries.accounts import (
    AccountsRepository,
    get_account_overview,
    get_portfolio_summary,
)
from trading_system.database.queries.positions import (
    PositionsRepository,
    get_positions_overview,
    get_position_deltas,
)
from trading_system.database.queries.trades import (
    TradesRepository,
    get_trades_overview,
    get_order_status_feed,
)


__all__ = [
    # Repository classes
    "AccountsRepository",
    "PositionsRepository", 
    "TradesRepository",
    
    # Helper query functions
    "get_account_overview",
    "get_portfolio_summary",
    "get_positions_overview",
    "get_position_deltas",
    "get_trades_overview",
    "get_order_status_feed",
]
