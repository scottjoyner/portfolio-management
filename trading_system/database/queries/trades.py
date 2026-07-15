"""
Trades Query Module - PostgreSQL Integration

Provides database operations for trade execution and lifecycle:
- Order management (create, update, cancel)
- Fill aggregation and tracking
- Trade settlement
- P&L calculations across fills

Architecture:
┌─────────────────────────────────────────────────────┐
│              Trades Layer                            │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌────────┐│
│  │ Order Mgmt   │    │ Fill         │    │ Settle ││
│  │              │    │ Aggregation  │    │         ││
│  │  Create      │    │ Tracking     │    │Recovery ││
│  │  Update      │    │ P&L Calc     │    │         ││
│  │  Cancel      │    │              │    │Audit    ││
│  └──────────────┘    └──────────────┘    └────────┘│
│                              │                    │
│              ▼              ▼              ▼       │
│         ┌─────────────────────────────────┐       │
│         │   Database Repository Layer     │       │
│         │   (SQLAlchemy ORM)             │       │
│         └─────────────────────────────────┘       │
│                              │                    │
│              ▼              ▼              ▼       │
│    Order API  ←  Matching Engine  ←  Market Events│
│                                                      │
└─────────────────────────────────────────────────────┘

Notes:
- order_id is the unique identifier for tracking
- status: pending, open, partial, closed, cancelled
- fills are automatically aggregated from multiple executions
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class TradesRepository:
    """Repository for all trade-related database operations."""
    
    def __init__(self, db: Session) -> None:
        self.db = db
    
    # ==================== ORDER OPERATIONS ====================
    
    def create_order(self, order_data: dict[str, Any]) -> dict[str, Any]:
        """Create a new order with unique ID."""
        from storage.postgres.models import Order
        
        # Generate unique order ID if not provided
        order_id = order_data.get("order_id") or str(uuid.uuid4())[:12]
        
        order = Order(
            id=None,  # Let DB auto-generate
            order_id=order_id,
            preview_id=order_data.get("preview_id"),
            strategy_id=order_data.get("strategy_id"),
            portfolio_id=order_data.get("portfolio_id"),
            sleeve_id=order_data.get("sleeve_id"),
            product_id=order_data["product_id"],  # Required
            side=order_data["side"],  # Required
            size=float(order_data["size"]),  # Required
            price=float(order_data.get("price", None)),
            notional=float(order_data.get("notional", None)),
            order_type=order_data.get("order_type", "limit"),
            status=order_data.get("status", "pending"),
            maker_taker_expectation=order_data.get("maker_taker_expectation"),
            queue_age_s=0,
            risk_mode=order_data.get("risk_mode", "NORMAL"),
            reduce_only=order_data.get("reduce_only", False),
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        
        return {
            "order_id": order.order_id,
            "status": order.status,
            "created_at": order.created_at,
            "size": float(order.size),
            "price": float(order.price) if order.price else None,
        }
    
    def get_order(self, order_id: str) -> dict[str, Any] | None:
        """Retrieve an order by ID."""
        from storage.postgres.models import Order
        
        order = self.db.query(Order).filter(Order.order_id == order_id).first()
        
        if not order:
            return None
        
        return {
            "order_id": order.order_id,
            "preview_id": order.preview_id,
            "strategy_id": order.strategy_id,
            "portfolio_id": order.portfolio_id,
            "sleeve_id": order.sleeve_id,
            "product_id": order.product_id,
            "side": order.side,
            "size": float(order.size),
            "remaining_size": float(order.remaining_size),
            "price": float(order.price) if order.price else None,
            "notional": float(order.notional) if order.notional else None,
            "order_type": order.order_type,
            "status": order.status,
            "maker_taker_expectation": order.maker_taker_expectation,
            "queue_age_s": order.queue_age_s,
            "risk_mode": order.risk_mode,
            "reduce_only": order.reduce_only,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
        }
    
    def list_orders(self, portfolio_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        """List orders with optional filtering."""
        from storage.postgres.models import Order
        
        query = self.db.query(Order)
        
        if portfolio_id:
            query = query.filter(Order.portfolio_id == portfolio_id)
        
        if status:
            query = query.filter(Order.status == status)
        
        orders = query.all()
        
        return [self._order_to_dict(o) for o in orders]
    
    def _order_to_dict(self, order: Order) -> dict[str, Any]:
        """Convert Order model to dictionary."""
        return {
            "order_id": order.order_id,
            "preview_id": order.preview_id,
            "strategy_id": order.strategy_id,
            "portfolio_id": order.portfolio_id,
            "sleeve_id": order.sleeve_id,
            "product_id": order.product_id,
            "side": order.side,
            "size": float(order.size),
            "remaining_size": float(order.remaining_size),
            "price": float(order.price) if order.price else None,
            "notional": float(order.notional) if order.notional else None,
            "order_type": order.order_type,
            "status": order.status,
            "maker_taker_expectation": order.maker_taker_expectation,
            "queue_age_s": order.queue_age_s,
            "risk_mode": order.risk_mode,
            "reduce_only": order.reduce_only,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
        }
    
    def update_order_status(self, order_id: str, status: str) -> dict[str, Any] | None:
        """Update order status (e.g., from pending to open)."""
        from storage.postgres.models import Order

        order = self.db.query(Order).filter(Order.order_id == order_id).first()
        
        if not order:
            return None
        
        old_status = order.status
        order.status = status
        order.updated_at = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(order)
        
        return {
            "order_id": order.order_id,
            "old_status": old_status,
            "new_status": status,
            "updated_at": order.updated_at,
        }
    
    def cancel_order(self, order_id: str) -> dict[str, Any] | None:
        """Cancel an open/partial order."""
        from storage.postgres.models import Order

        order = self.db.query(Order).filter(Order.order_id == order_id).first()
        
        if not order or order.status != "open":
            return {
                "success": False,
                "error": f"Cannot cancel order with status: {order.status if order else 'not found'}",
            }
        
        old_status = order.status
        order.status = "cancelled"
        order.updated_at = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(order)
        
        return {
            "order_id": order.order_id,
            "success": True,
            "old_status": old_status,
            "new_status": "cancelled",
            "cancelled_at": datetime.now(timezone.utc),
        }
    
    def partially_fill_order(self, order_id: str, fill_size: float, fill_price: float) -> dict[str, Any]:
        """Partially fill an order (for limit orders)."""
        from storage.postgres.models import Order

        order = self.db.query(Order).filter(Order.order_id == order_id).first()
        
        if not order:
            return {"error": "Order not found"}
        
        # Reduce remaining size
        old_remaining = order.remaining_size
        new_remaining = order.remaining_size - fill_size
        
        order.remaining_size = max(0, new_remaining)
        order.updated_at = datetime.now(timezone.utc)
        
        if new_remaining == 0:
            order.status = "closed"
        else:
            order.status = "partial"
        
        self.db.commit()
        self.db.refresh(order)
        
        return {
            "order_id": order.order_id,
            "fill_size": fill_size,
            "fill_price": fill_price,
            "remaining_size": float(order.remaining_size),
            "new_status": order.status,
        }
    
    # ==================== FILL OPERATIONS ====================
    
    def get_fills_for_order(self, order_id: str) -> list[dict[str, Any]]:
        """Get all fills for an order."""
        from storage.postgres.models import Fill
        
        fills = self.db.query(Fill).filter(Fill.order_id == order_id).all()
        
        return [self._fill_to_dict(f) for f in fills]
    
    def _fill_to_dict(self, fill: Any) -> dict[str, Any]:
        """Convert fill record to dictionary."""
        if hasattr(fill, '__dict__'):
            return {k: v for k, v in fill.__dict__.items() if not k.startswith('_')}
        return fill
    
    def create_fill(self, fill_data: dict[str, Any]) -> dict[str, Any]:
        """Create a new fill record."""
        from storage.postgres.models import Fill
        
        fill_id = fill_data.get("fill_id") or str(uuid.uuid4())[:12]
        
        fill = Fill(
            id=None,  # Let DB auto-generate
            fill_id=fill_id,
            order_id=fill_data["order_id"],  # Required
            product_id=fill_data["product_id"],  # Required
            side=fill_data.get("side"),
            size=float(fill_data["size"]),  # Required
            price=float(fill_data["price"]),  # Required
            notional=float(fill_data.get("notional", None)),
            slippage_bps=float(fill_data.get("slippage_bps", 0)),
            fee=float(fill_data.get("fee", 0)),
            fee_currency=fill_data.get("fee_currency"),
            liquidity=fill_data.get("liquidity"),
            created_at=datetime.now(timezone.utc),
        )
        
        self.db.add(fill)
        self.db.commit()
        self.db.refresh(fill)
        
        return {
            "fill_id": fill.fill_id,
            "order_id": fill.order_id,
            "product_id": fill.product_id,
            "size": float(fill.size),
            "price": float(fill.price),
            "notional": float(fill.notional) if fill.notional else None,
            "slippage_bps": fill.slippage_bps,
            "fee": fill.fee,
        }
    
    def list_fills(self, product_id: str | None = None, order_id: str | None = None) -> list[dict[str, Any]]:
        """List fills with optional filtering."""
        from storage.postgres.models import Fill
        
        query = self.db.query(Fill)
        
        if product_id:
            query = query.filter(Fill.product_id == product_id)
        
        if order_id:
            query = query.filter(Fill.order_id == order_id)
        
        fills = query.all()
        
        return [self._fill_to_dict(f) for f in fills]
    
    # ==================== TRADE SUMMARY & P&L ====================
    
    def get_trade_pnl(self, product_id: str, portfolio_id: str | None = None) -> dict[str, Any]:
        """Calculate realized and unrealized P&L for a trade/product."""
        from storage.postgres.models import Order, Fill
        
        # Get all fills for this product/portfolio
        query = self.db.query(Fill).filter(Fill.product_id == product_id)
        
        if portfolio_id:
            # Join through orders
            query = query.join(Order).filter(Order.portfolio_id == portfolio_id)
        
        fills = query.all()
        
        if not fills:
            return {"realized_pnl": 0, "unrealized_pnl": 0, "total_fills": 0}
        
        # Calculate realized P&L from fees (simplified)
        total_fees = sum(f.fee for f in fills if f.fee)
        realized_pnl = -total_fees  # Fees reduce P&L
        
        # Get weighted average entry price
        total_size = sum(abs(f.size) for f in fills)
        if total_size == 0:
            return {"realized_pnl": realized_pnl, "unrealized_pnl": 0}
        
        avg_entry_price = sum(f.size * f.price for f in fills) / total_size
        
        # Get current market price (placeholder - would come from API)
        current_price = 100.0
        
        # Unrealized P&L = (current_price - avg_entry_price) * position_size
        position_value = abs(sum(f.size * (1 if f.side == "buy" else -1) for f in fills))
        unrealized_pnl = (current_price - avg_entry_price) * position_value
        
        return {
            "product_id": product_id,
            "portfolio_id": portfolio_id,
            "realized_pnl": float(realized_pnl),
            "unrealized_pnl": float(unrealized_pnl),
            "total_fees": float(total_fees),
            "total_fills": len(fills),
            "position_size": float(position_value) if position_value > 0 else 0,
        }
    
    def get_trade_history(self, product_id: str, portfolio_id: str | None = None, 
                          limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """Get trade history for a product/portfolio."""
        from storage.postgres.models import Order
        
        # Query orders and fills together
        query = self.db.query(Order).filter(Order.product_id == product_id)
        
        if portfolio_id:
            query = query.filter(Order.portfolio_id == portfolio_id)
        
        orders = query.order_by(Order.created_at.desc()).limit(limit).offset(offset).all()
        
        return [self._order_to_dict(o) for o in orders]
    
    def calculate_fill_metrics(self, product_id: str, portfolio_id: str | None = None) -> dict[str, Any]:
        """Calculate execution metrics for a product/portfolio."""
        from storage.postgres.models import Fill
        
        fills = self.db.query(Fill).filter(
            Fill.product_id == product_id
        ).all()
        
        if portfolio_id:
            # This would need proper join logic
            pass
        
        if not fills:
            return {
                "total_filled_notional": 0,
                "avg_slippage_bps": 0,
                "total_fees": 0,
                "fill_count": 0,
            }
        
        total_notional = sum(f.notional for f in fills if f.notional)
        avg_slippage = sum(f.slippage_bps * f.size for f in fills) / sum(f.size for f in fills) if fills else 0
        
        return {
            "total_filled_notional": float(total_notional),
            "avg_slippage_bps": float(avg_slippage),
            "total_fees": float(sum(f.fee for f in fills)),
            "fill_count": len(fills),
            "avg_fill_size": float(sum(f.size for f in fills) / len(fills)) if fills else 0,
        }


# ==================== QUERIES FOR API LAYER ====================

def get_trades_overview(db: Session) -> dict[str, Any]:
    """Get trades overview for dashboard."""
    repo = TradesRepository(db)
    
    # Get all portfolios
    from storage.postgres.models import Portfolio
    portfolios = db.query(Portfolio).all()
    
    result = {
        "portfolios": [],
        "total_open_orders": 0,
        "total_fills_count": 0,
    }
    
    for portfolio in portfolios:
        # Get open orders count
        open_orders = repo.list_orders(portfolio.id, status="open")
        
        # Get P&L summary (would need to aggregate across all products)
        result["portfolios"].append({
            "portfolio_id": portfolio.id,
            "name": portfolio.name,
            "open_orders_count": len(open_orders),
        })
    
    result["total_open_orders"] = sum(p["open_orders_count"] for p in result["portfolios"])
    result["total_fills_count"] = 0  # Would need to query total fills
    
    return result


def get_order_status_feed(db: Session) -> dict[str, str]:
    """Get current status for all orders (for monitoring)."""
    from storage.postgres.models import Order
    
    orders = db.query(Order).filter(Order.status.in_(["open", "partial"])).all()
    
    return {
        order.order_id: order.status 
        for order in orders
    }
