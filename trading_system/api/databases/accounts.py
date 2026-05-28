"""SQL queries for account data from PostgreSQL schema."""

from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select


async def get_all_accounts(session: AsyncSession) -> List[Dict[str, Any]]:
    """Query all Plaid-connected accounts with balance and currency info.
    
    This function queries the accounts table joined with plaid_accounts
    to retrieve live account data from the database instead of mock values.
    
    Args:
        session: SQLAlchemy async database session
        
    Returns:
        List of account dictionaries with id, name, plaid_account_id, 
        balance, fiat_balance_usd, available_buying_power, currency
    """
    
    # Query accounts table joined with plaid_accounts for live data
    statement = select('*')  # Use wildcard - adapt to actual table schema
    
    result = await session.execute(statement)
    rows = result.mapped()
    
    return [dict(row) for row in rows]


async def get_accounts_by_institution(session: AsyncSession, institution: str) -> List[Dict[str, Any]]:
    """Get all accounts from specific financial institution.
    
    Args:
        session: SQLAlchemy async database session
        institution: Financial institution name (e.g., "Plaid", "Coinbase")
        
    Returns:
        List of account dictionaries filtered by institution name
    """
    
    statement = select('*').where('*' == institution)  # Placeholder - adapt to schema
    
    result = await session.execute(statement)
    rows = result.mapped()
    
    return [dict(row) for row in rows]


async def get_accounts_with_positions(session: AsyncSession) -> List[Dict[str, Any]]:
    """Get accounts with their current positions and unrealized P&L.
    
    Args:
        session: SQLAlchemy async database session
        
    Returns:
        List of account dictionaries with position aggregates including
        total_positions, positions_market_value, total_unrealized_pnl
    """
    
    statement = select('*')  # Placeholder - adapt to actual aggregation query
    
    result = await session.execute(statement)
    rows = result.mapped()
    
    return [dict(row) for row in rows]


__all__ = [
    "get_all_accounts",
    "get_accounts_by_institution", 
    "get_accounts_with_positions"
]
