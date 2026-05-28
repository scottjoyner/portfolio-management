"""
Auto-Approval Rules Repository - Tiered Approval System

Implements intelligent order approval workflows with:
- Tier 1: Auto-approve within capital allocation limits (no human review)
- Tier 2: Auto-approve up to $50K without escalation (within guidelines)
- Tier 3: Requires analyst review for all other orders
- Whitelist patterns bypass tiers entirely

Architecture:
┌─────────────────────────────────────────────────────┐
│            Auto-Approval Rules Layer                  │
├─────────────────────────────────────────────────────┤
│                                                     │
│    ┌──────────────┐    ┌──────────────┐    ┌──────┐│
│    │ Whitelist   │    │ Capital      │    │ Tiers ││
│    │ Detection   │    │ Limits       │    │       ││
│    │             │    │ Check        │    │Logic  ││
│    └──────────────┘    └──────────────┘    └──────┘│
│              │              │              │       │
│              ▼              ▼              ▼       │
│         ┌─────────────────────────────────┐       │
│         │   Approval Database Table      │       │
│         │   (from 0001_initial.py)      │       │
│         └─────────────────────────────────┘       │
│                                                     │
└─────────────────────────────────────────────────────┘

Approval Flow:
1. Check whitelist patterns → Auto-approve if match
2. Check capital allocation limits → Tier 1 if within
3. Check dollar threshold → Tier 2 if under $50K
4. Otherwise → Tier 3 (requires review)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class AutoApprovalRulesRepository:
    """Repository for auto-approval rule logic and workflows."""
    
    def __init__(self, db: Session) -> None:
        self.db = db
    
    # ==================== WHITELIST PATTERN DETECTION ====================
    
    def check_whitelist_patterns(self, order_data: dict[str, Any], 
                                  product_id: str | None = None) -> dict[str, Any]:
        """
        Check if an order matches whitelist patterns.
        Returns (is_whitelisted, reason, tier=None).
        
        Whitelist patterns:
        - Specific approved products (e.g., BTC-USD, ETH-USD)
        - Approved counterparties (e.g., Coinbase Prime)
        - Approved order types for certain portfolios
        """
        # Define whitelist pattern database
        whitelisted_products = {
            "BTC-USD": {"tier": 1, "reason": "approved_crypto_pair"},
            "ETH-USD": {"tier": 1, "reason": "approved_crypto_pair"},
            "SOL-USD": {"tier": 2, "reason": "approved_crypto_pair"},
        }
        
        whitelisted_counterparties = {
            "coinbase_prime": {"tier": 1, "reason": "approved_counterparty"},
            "kraken_pro": {"tier": 2, "reason": "approved_counterparty"},
        }
        
        # Check product whitelist
        if product_id:
            product_lower = product_id.upper()
            for whitelisted in whitelisted_products:
                if whitelisted.lower() in product_lower.lower():
                    return {
                        "is_whitelisted": True,
                        "product": product_id,
                        "tier": whitelisted_products[whitelisted]["tier"],
                        "reason": whitelisted_products[whitelisted]["reason"],
                        "bypasses_tiers": True,
                    }
        
        # Check counterparty whitelist (from order metadata)
        if order_data:
            counterparties = getattr(order_data.get('metadata', {}), 'counterparties', []) or []
            for counterparty in counterparties:
                cp_lower = counterparty.lower()
                for whitelisted in whitelisted_counterparties:
                    if whitelisted in cp_lower:
                        return {
                            "is_whitelisted": True,
                            "counterparty": counterparty,
                            "tier": whitelisted_counterparties[whitelisted]["tier"],
                            "reason": whitelisted_counterparties[whitelisted]["reason"],
                            "bypasses_tiers": True,
                        }
        
        return {
            "is_whitelisted": False,
            "product": product_id,
            "counterparty": None,
            "tier": None,
            "reason": None,
            "bypasses_tiers": False,
        }
    
    # ==================== CAPITAL ALLOCATION LIMITS ====================
    
    def check_capital_allocation_limits(self, portfolio_id: str, 
                                         product_id: str, proposed_size: float) -> dict[str, Any]:
        """
        Check if an order is within capital allocation limits.
        
        Returns:
        - is_within_limits: bool
        - limit_type: e.g., "position_limit", "allocation_weight"
        - remaining_capacity: amount available
        """
        # Get portfolio NAV (placeholder)
        from storage.postgres.models import Portfolio
        
        portfolio = self.db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        
        if not portfolio:
            return {
                "is_within_limits": True,  # Default allow if no portfolio found
                "limit_type": None,
                "remaining_capacity": float("inf"),
                "portfolio_nav": None,
            }
        
        # Define limit configuration
        position_limit_pct = 25.0  # Max 25% of NAV per position
        allocation_weight_limit = 10.0  # Max 10% weight allocation
        
        # Get current position value (would call positions repository)
        current_position_value = 0.0
        
        # Calculate proposed order value
        market_price = 100.0  # Placeholder - would come from market data
        proposed_order_value = abs(proposed_size) * market_price
        
        # Check position limit
        max_position_value = portfolio.nav * (position_limit_pct / 100)
        
        is_within_limits = current_position_value + proposed_order_value <= max_position_value
        
        remaining_capacity = max_position_value - (current_position_value + proposed_order_value)
        
        return {
            "is_within_limits": is_within_limits,
            "limit_type": "position_limit",
            "remaining_capacity": float(remaining_capacity),
            "portfolio_nav": float(portfolio.nav),
            "max_position_value": float(max_position_value),
            "proposed_order_value": float(proposed_order_value),
        }
    
    # ==================== APPROVAL TIER LOGIC ====================
    
    def determine_approval_tier(self, order_data: dict[str, Any], 
                                  product_id: str | None = None) -> int:
        """
        Determine the approval tier for an order.
        
        Tier 1: Auto-approve within capital limits (no human review)
        Tier 2: Auto-approve up to $50K without escalation
        Tier 3: Requires analyst review for all other orders
        
        Returns: 1, 2, or 3
        """
        # Check whitelist first (bypasses all tiers)
        whitelist_result = self.check_whitelist_patterns(order_data, product_id)
        
        if whitelist_result["is_whitelisted"] and whitelist_result.get("bypasses_tiers"):
            return 1  # Auto-approve whitelisted orders
    
    # Check capital allocation limits
        cap_limit_result = self.check_capital_allocation_limits(
            order_data.get("portfolio_id"),
            product_id or "",
            float(order_data.get("size", 0))
        )
        
        if cap_limit_result["is_within_limits"]:
            return 1  # Within capital limits -> Tier 1
    
    # Check dollar threshold for Tier 2
        order_value = abs(float(order_data.get("notional", order_data.get("size", 0))) * 100)
        
        if order_value <= 50_000:  # $50K threshold
            return 2  # Under $50K -> Tier 2
    
    # Otherwise, Tier 3 requires review
        return 3
    
    def get_tier_requirements(self, tier: int) -> dict[str, Any]:
        """Get requirements and guidelines for each approval tier."""
        tier_configs = {
            1: {
                "name": "Auto-Approve",
                "description": "Orders within capital allocation limits (no human review)",
                "max_order_value": None,  # No dollar limit
                "requires_review": False,
                "reviewed_by": None,
                "allowed_products": ["BTC-USD", "ETH-USD"],
            },
            2: {
                "name": "Low-Escalation", 
                "description": "Orders up to $50K without escalation",
                "max_order_value": 50_000,
                "requires_review": False,
                "reviewed_by": None,
                "allowed_products": ["BTC-USD", "ETH-USD", "SOL-USD"],
            },
            3: {
                "name": "Analyst Review Required",
                "description": "Orders exceeding thresholds require analyst review",
                "max_order_value": None,  # No automatic approval
                "requires_review": True,
                "reviewed_by": ["analyst_001", "analyst_002"],
                "allowed_products": [],  # All products allowed with review
            },
        }
        
        return tier_configs.get(tier, tier_configs[3])  # Default to Tier 3
    
    # ==================== APPROVAL RECORDS ====================
    
    def create_approval_record(self, approval_type: str, summary: str, 
                                capital_affected: float, status: str = "pending",
                                approved_by: str | None = None) -> dict[str, Any]:
        """Create an approval record in the database."""
        from storage.postgres.models import Approval
        
        approval = Approval(
            approval_id=approval_type or str(uuid.uuid4())[:12],
            approval_type=approval_type or "order",
            summary=summary,
            capital_affected=float(capital_affected),
            liquidity_impact=None,  # Would calculate
            risk_impact=None,  # Would calculate
            status=status,
            approved_by=approved_by,
            created_at=datetime.now(timezone.utc),
        )
        
        self.db.add(approval)
        self.db.commit()
        self.db.refresh(approval)
        
        return {
            "approval_id": approval.approval_id,
            "type": approval.approval_type,
            "summary": approval.summary,
            "capital_affected": float(approval.capital_affected),
            "status": approval.status,
            "approved_by": approved_by,
        }
    
    def get_pending_approvals(self) -> list[dict[str, Any]]:
        """Get all pending approvals requiring review."""
        from storage.postgres.models import Approval
        
        approvals = self.db.query(Approval).filter(
            Approval.status.in_(["pending", "in_review"])
        ).all()
        
        return [self._approval_to_dict(a) for a in approvals]
    
    def _approval_to_dict(self, approval: Any) -> dict[str, Any]:
        """Convert approval model to dictionary."""
        if hasattr(approval, '__dict__'):
            return {k: v for k, v in approval.__dict__.items() if not k.startswith('_')}
        return approval
    
    def mark_approval_complete(self, approval_id: str, status: str = "approved", 
                                approved_by: str | None = None) -> dict[str, Any]:
        """Mark an approval as complete (approved or rejected)."""
        from storage.postgres.models import Approval
        
        approval = self.db.query(Approval).filter(
            Approval.approval_id == approval_id
        ).first()
        
        if not approval:
            return {"success": False, "error": "Approval not found"}
        
        old_status = approval.status
        approval.status = status
        approval.approved_by = approved_by
        approval.updated_at = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(approval)
        
        return {
            "approval_id": approval.approval_id,
            "old_status": old_status,
            "new_status": status,
            "approved_by": approved_by,
            "completed_at": datetime.now(timezone.utc),
        }


# ==================== HELPER QUERIES ====================

def get_auto_approve_candidates(db: Session) -> list[dict[str, Any]]:
    """Get orders that can be auto-approved (Tier 1)."""
    repo = AutoApprovalRulesRepository(db)
    
    # This would query open/pending orders and filter by whitelist/capital limits
    return []


def get_orders_needing_review(db: Session) -> list[dict[str, Any]]:
    """Get orders that require analyst review (Tier 3)."""
    repo = AutoApprovalRulesRepository(db)
    
    # Query would filter for non-whitelisted orders outside capital limits
    return []


def get_approvals_summary(db: Session) -> dict[str, Any]:
    """Get summary statistics for approval workflows."""
    from storage.postgres.models import Approval
    
    total = db.query(Approval).count()
    pending = db.query(Approval).filter(Approval.status.in_(["pending", "in_review"])).count()
    approved = db.query(Approval).filter(Approval.status == "approved").count()
    
    return {
        "total_approvals": total,
        "pending": pending,
        "approved": approved,
        "pending_rate": round(pending / total * 100, 2) if total > 0 else 0,
    }
