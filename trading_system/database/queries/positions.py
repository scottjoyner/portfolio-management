"""
Positions Query Module - PostgreSQL Integration

Provides database operations for position/holdings management:
- Crypto asset positions (product_id based)
- Position sizing and delta tracking
- Unrealized P&L calculations
- Portfolio risk exposure tracking

Architecture:
┌─────────────────────────────────────────────────────┐
│              Positions Layer                          │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌────────┐│
│  │ Position     │    │ Delta        │    │ P&L    ││
│  │ Tracking     │    │ Management   │    │ Calc   ││
│  │              │    │              │    │        ││
│  │  Size Mgmt   │    │ Delta Neutral│    │Risk    ││
│  │  Allocation  │    │ Optimization  │    │Mgmt    ││
│  └──────────────┘    └──────────────┘    └────────┘│
│                              │                    │
│              ▼              ▼              ▼       │
│         ┌─────────────────────────────────┐       │
│         │   Database Repository Layer     │       │
│         │   (SQLAlchemy ORM)             │       │
│         └─────────────────────────────────┘       │
│                              │                    │
│              ▼              ▼              ▼       │
│    Position Updates  ←  Order Flow  ←  Market Events│
│                                                      │
└─────────────────────────────────────────────────────┘

Notes:
- product_id identifies the crypto asset (e.g., BTC-USD)
- side tracks long/short positions
- size is in base currency units (not quoted)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PositionsRepository:
    """Repository for all position-related database operations."""
    
    def __init__(self, db: Session) -> None:
        self.db = db
    
    # ==================== POSITION OPERATIONS ====================
    
    def get_position(self, product_id: str, portfolio_id: str | None = None) -> dict[str, Any] | None:
        """
        Retrieve a position by product ID and optionally portfolio.
        Returns position data with current market value.
        """
        # Query fills to aggregate position
        from storage.postgres.models import Order, Fill
        
        query = (self.db.query(Fill)
                 .filter(Fill.product_id == product_id))
        
        if portfolio_id:
            # Join through orders to filter by portfolio
            query = self.db.query(Fill).join(Order).filter(Order.portfolio_id == portfolio_id)
        
        fills = query.all()
        
        if not fills:
            return None
        
        # Aggregate position from fills
        total_size = sum(f.size for f in fills)
        weighted_avg_price = sum(f.size * f.price for f in fills) / total_size if total_size > 0 else 0
        
        # Get latest fill date
        latest_fill = max(fills, key=lambda f: f.created_at)
        
        return {
            "product_id": product_id,
            "portfolio_id": portfolio_id,
            "size": float(total_size),
            "weighted_avg_price": float(weighted_avg_price),
            "market_value": float(total_size * weighted_avg_price) if weighted_avg_price > 0 else 0,
            "entry_date": latest_fill.created_at,
            "position_type": "long" if total_size > 0 else ("short" if total_size < 0 else "neutral"),
        }
    
    def list_positions(self, portfolio_id: str | None = None) -> list[dict[str, Any]]:
        """List all positions, optionally filtered by portfolio."""
        from storage.postgres.models import Order, Fill
        
        query = self.db.query(Fill).filter(Fill.size != 0)
        
        if portfolio_id:
            query = query.join(Order).filter(Order.portfolio_id == portfolio_id)
        
        fills = query.all()
        
        # Group by product_id
        positions: dict[str, dict[str, Any]] = {}
        
        for fill in fills:
            product_id = fill.product_id
            if product_id not in positions:
                # Get portfolio from order if specified
                portfolio_id = fill.order_id if portfolio_id else None
                positions[product_id] = {
                    "product_id": product_id,
                    "portfolio_id": portfolio_id,
                    "size": 0,
                    "weighted_avg_price": 0,
                    "market_value": 0,
                    "entry_date": fill.created_at,
                    "fill_count": 0,
                }
            
            pos = positions[product_id]
            pos["size"] += abs(fill.size) * (1 if fill.side == "buy" else -1)
            pos["fill_count"] += 1
        
        return list(positions.values())
    
    def update_position_size(self, product_id: str, size: float, price: float | None = None) -> dict[str, Any]:
        """Update position size (for rebalancing or corrections)."""
        # In production, this would create a new fill or adjust existing
        for portfolio in self.db.query("storage.postgres.models.Portfolio"):
            existing_pos = self.get_position(product_id, portfolio.id)
            if existing_pos:
                existing_pos["size"] += size
                existing_pos["market_value"] = existing_pos["size"] * (existing_pos["weighted_avg_price"] or price)
        
        return self.get_position(product_id)
    
    def close_position(self, product_id: str, portfolio_id: str | None = None, market_price: float | None = None) -> dict[str, Any]:
        """Close an entire position at current market price."""
        position = self.get_position(product_id, portfolio_id)
        
        if not position:
            return {"error": "Position not found"}
        
        realized_pnl = position["market_value"] - (position["size"] * position["weighted_avg_price"])
        
        return {
            "product_id": product_id,
            "portfolio_id": portfolio_id,
            "size_closed": position["size"],
            "close_price": market_price or position["weighted_avg_price"],
            "market_value_at_close": position["market_value"],
            "realized_pnl": realized_pnl,
        }
    
    # ==================== PORTFOLIO POSITION SUMMARY ====================
    
    def get_portfolio_position_summary(self, portfolio_id: str) -> dict[str, Any]:
        """Get comprehensive position summary for a portfolio."""
        positions = self.list_positions(portfolio_id)
        
        total_market_value = sum(p["market_value"] for p in positions)
        total_size_by_product = {p["product_id"]: p["size"] for p in positions}
        
        return {
            "portfolio_id": portfolio_id,
            "positions_count": len(positions),
            "total_market_value": total_market_value,
            "positions": positions,
            "concentrations": sorted(
                [{"product_id": k, "allocation_pct": round(v / total_market_value * 100, 2) if total_market_value > 0 else 0}
                 for k, v in total_size_by_product.items()],
                key=lambda x: x["allocation_pct"],
                reverse=True
            )[:5],  # Top 5 concentrations
        }
    
    # ==================== POSITION DELTA TRACKING ====================
    
    def track_delta_change(self, product_id: str, delta: float, portfolio_id: str | None = None) -> dict[str, Any]:
        """
        Track delta change for a position (used in market making).
        Returns updated position info.
        """
        existing_pos = self.get_position(product_id, portfolio_id)
        
        if existing_pos:
            new_size = existing_pos["size"] + delta
            return {
                "product_id": product_id,
                "previous_size": existing_pos["size"],
                "delta_applied": delta,
                "new_size": new_size,
                "position_type": "long" if new_size > 0 else ("short" if new_size < 0 else "neutral"),
            }
        
        return {
            "product_id": product_id,
            "previous_size": 0,
            "delta_applied": delta,
            "new_size": delta,
            "position_type": "long" if delta > 0 else ("short" if delta < 0 else "neutral"),
        }
    
    # ==================== POSITION LIMITS & RISK ====================
    
    def check_position_limit(self, product_id: str, portfolio_id: str, proposed_size: float) -> dict[str, Any]:
        """Check if adding a position would exceed portfolio limits."""
        current_pos = self.get_position(product_id, portfolio_id)
        
        # Get portfolio metrics
        from storage.postgres.models import Portfolio
        portfolio = self.db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        
        if not portfolio:
            return {"allowed": True, "reason": "No portfolio found"}
        
        position_limit_pct = 25.0  # Default 25% position limit
        
        current_value = current_pos["market_value"] if current_pos else 0
        proposed_value = abs(proposed_size) * (current_pos["weighted_avg_price"] if current_pos else 100)
        
        new_total_position = current_value + proposed_value
        max_allowed_position = portfolio.nav * (position_limit_pct / 100)
        
        allowed = new_total_position <= max_allowed_position
        
        return {
            "allowed": allowed,
            "current_position_value": current_value,
            "proposed_position_value": proposed_value,
            "portfolio_nav": float(portfolio.nav),
            "limit_pct": position_limit_pct,
            "max_position_value": float(max_allowed_position),
        }
    
    def get_unrealized_pnl(self, product_id: str, portfolio_id: str | None = None, current_price: float = 100) -> dict[str, Any]:
        """Calculate unrealized P&L for a position at current market price."""
        position = self.get_position(product_id, portfolio_id)
        
        if not position:
            return {"unrealized_pnl": 0, "position_value": 0}
        
        unrealized_pnl = (current_price - position["weighted_avg_price"]) * position["size"]
        
        return {
            "product_id": product_id,
            "portfolio_id": portfolio_id,
            "position_size": position["size"],
            "avg_entry_price": float(position["weighted_avg_price"]),
            "current_market_price": current_price,
            "unrealized_pnl": float(unrealized_pnl),
            "unrealized_pnl_pct": round((current_price / position["weighted_avg_price"] - 1) * 100, 2) if position["weighted_avg_price"] > 0 else 0,
        }


# ==================== QUERIES FOR API LAYER ====================

def get_positions_overview(db: Session) -> dict[str, Any]:
    """Get positions overview for dashboard."""
    repo = PositionsRepository(db)
    
    # Get all portfolios
    from storage.postgres.models import Portfolio
    portfolios = repo.db.query(Portfolio).all()
    
    result = {
        "portfolios": [],
        "total_positions_count": 0,
    }
    
    for portfolio in portfolios:
        summary = repo.get_portfolio_position_summary(portfolio.id)
        if summary:
            result["portfolios"].append({
                "portfolio_id": portfolio.id,
                "name": portfolio.name,
                "positions_count": summary["positions_count"],
                "total_market_value": summary["total_market_value"],
                "top_concentrations": summary["concentrations"],
            })
    
    result["total_positions_count"] = sum(p["positions_count"] for p in result["portfolios"])
    
    return result


def get_position_deltas(db: Session, product_id: str) -> list[dict[str, Any]]:
    """Get open deltas (unmatched orders) for a product."""
    from storage.postgres.models import Order
    
    # Query for orders that are not yet matched/filled
    open_orders = db.query(Order).filter(
        Order.status.in_(["open", "partial"]),
        Order.order_type.in_(["limit", "market"])
    ).all()
    
    return [
        {
            "order_id": o.order_id,
            "product_id": o.product_id,
            "side": o.side,
            "size": float(o.size),
            "price": float(o.price) if o.price else None,
            "status": o.status,
        }
        for o in open_orders
    ]
