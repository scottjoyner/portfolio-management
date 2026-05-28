"""SQL queries for approval requests from PostgreSQL schema."""

from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select


async def get_approval_requests(session: AsyncSession) -> List[Dict[str, Any]]:
    """Get all pending approval requests with details.
    
    Queries approval_requests table with strategy and position info.
    
    Args:
        session: SQLAlchemy async database session
        
    Returns:
        List of approval request dictionaries with id, status, proposed_tier, 
        quantity_usd, risk_score, instrument
    """
    
    statement = select('*')  # Placeholder - adapt to actual schema
    
    result = await session.execute(statement)
    rows = result.mapped()
    
    return [dict(row) for row in rows]


async def get_pending_approvals(session: AsyncSession) -> List[Dict[str, Any]]:
    """Get only pending approvals awaiting review.
    
    Args:
        session: SQLAlchemy async database session
        
    Returns:
        List of pending approval request dictionaries
    """
    
    statement = select('*').where('status' == 'pending')  # Placeholder
    
    result = await session.execute(statement)
    rows = result.mapped()
    
    return [dict(row) for row in rows]


async def get_auto_approved_trades(session: AsyncSession) -> List[Dict[str, Any]]:
    """Get trades that were auto-approved based on rules.
    
    Args:
        session: SQLAlchemy async database session
        
    Returns:
        List of auto-approved trade dictionaries with whitelist pattern and confidence
    """
    
    statement = select('*')  # Placeholder
    
    result = await session.execute(statement)
    rows = result.mapped()
    
    return [dict(row) for row in rows]


__all__ = [
    "get_approval_requests",
    "get_pending_approvals",
    "get_auto_approved_trades"
]
