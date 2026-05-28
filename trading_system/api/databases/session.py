"""Database Queries Module - SQLAlchemy ORM with Session Manager"""


class DatabaseManager:
    """Centralized PostgreSQL connection manager."""
    
    def __init__(self):
        from sqlalchemy.orm import sessionmaker, declarative_base
        
        self.Base = declarative_base()
        self.session_factory = sessionmaker()
        self.Session = sessionmaker()
    
    @property
    def session(self):
        """Get database session."""
        if not hasattr(self, '_session'):
            from sqlalchemy import create_engine
            
            engine = create_engine(
                "postgresql://user:password@localhost:5432/trading_system"
            )
            
            self._session = self.session_factory(bind=engine)
        
        return self._session
    
    def close(self):
        """Close database session."""
        if hasattr(self, '_session'):
            self._session.close()


# Create manager instance
db_manager = DatabaseManager()


def get_accounts():
    """Get active portfolios from PostgreSQL."""
    try:
        return db_manager.session.execute(sa.text(
            "SELECT id, name, type, provider, currency, balance_usd FROM portfolios WHERE type='ACTIVE' ORDER BY created_at DESC"
        )).fetchall()
    except Exception:
        return []


def get_trades(limit=50, offset=0):
    """Get closed trades from PostgreSQL."""
    try:
        return db_manager.session.execute(sa.text(
            "SELECT * FROM orders WHERE status IN ('CLOSED', 'CANCELLED') ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        ), {"limit": limit, "offset": offset}).fetchall()
    except Exception:
        return []


def get_positions():
    """Get open positions from PostgreSQL."""
    try:
        return db_manager.session.execute(sa.text(
            "SELECT o.*, COALESCE(SUM(f.quantity_filled), 0) as total_filled FROM orders o LEFT JOIN fills f ON (o.id = f.order_id AND f.side = o.side) GROUP BY o.id ORDER BY o.created_at DESC"
        )).fetchall()
    except Exception:
        return []


def get_strategies():
    """Get backtested strategies from PostgreSQL."""
    try:
        return db_manager.session.execute(sa.text(
            "SELECT config_key, name, description, category, backtested FROM strategy_configs WHERE backtested = true ORDER BY created_at DESC"
        )).fetchall()
    except Exception:
        return []


def get_performance():
    """Get portfolio performance metrics."""
    try:
        result = db_manager.session.execute(sa.text(
            "SELECT COALESCE(SUM(th.profit_loss), 0) as total_pnl FROM trade_history th INNER JOIN portfolios p ON th.portfolio_id = p.id WHERE p.type='ACTIVE'"
        )).fetchone()
        return {"total_realized_pnl_usd": float(result.total_pnl) if result else 0.0}
    except Exception:
        return {"total_realized_pnl_usd": 0.0}


def get_price_estimates(instrument):
    """Get DCF and technical price estimates for instrument."""
    try:
        result = db_manager.session.execute(sa.text(
            "SELECT current_market_price, dcf_intrinsic_value, technical_score, consensus_vs_current_pct, confidence_score FROM price_estimates WHERE instrument = :instrument ORDER BY timestamp DESC LIMIT 1"
        ), {"instrument": instrument}).fetchone()
        
        if result:
            return {
                "current_price": float(result.current_market_price) or None,
                "price_estimates": {
                    "dcf_intrinsic_value": float(result.dcf_intrinsic_value) if result.dcf_intrinsic_value else None,
                    "technical_score": float(result.technical_score) if result.technical_score else None,
                    "consensus_vs_current_pct": float(result.consensus_vs_current_pct) if result.consensus_vs_current_pct is not None else None,
                    "confidence_score": float(result.confidence_score) or 0.0,
                },
            }
        return {"current_price": None, "price_estimates": {}}
    except Exception:
        return {"current_price": None, "price_estimates": {}}


def get_approvals():
    """Get pending and completed approvals."""
    try:
        result = db_manager.session.execute(sa.text(
            "SELECT status, product_id, side, quantity, estimated_cost FROM approvals ORDER BY created_at DESC"
        )).fetchall()
        
        pending_count = sum(1 for r in result if hasattr(r, 'status') and str(r.status) in ['PENDING', 'IN_REVIEW'])
        completed_count = len(result) - pending_count
        
        return {
            "pending_count": pending_count,
            "completed_count": completed_count,
            "approvals": [],
        }
    except Exception:
        return {"pending_count": 0, "completed_count": 0}


def get_research_hypotheses():
    """Get high-confidence trading hypotheses."""
    try:
        result = db_manager.session.execute(sa.text(
            "SELECT id, product_id, hypothesis_text, confidence_score, expiration_datetime, timestamp FROM research_hypotheses WHERE confidence_score >= 0.5 ORDER BY confidence_score DESC, created_at DESC"
        )).fetchall()
        
        return {
            "hypotheses": [
                {
                    "id": r.id if hasattr(r, 'id') else None,
                    "product_id": r.product_id or None,
                    "hypothesis_text": r.hypothesis_text or None,
                    "confidence_score": float(r.confidence_score) or 0.0,
                    "expiration_datetime": str(r.expiration_datetime) if hasattr(r, 'expiration_datetime') and r.expiration_datetime else None,
                    "timestamp": str(r.timestamp) if hasattr(r, 'timestamp') and r.timestamp else None,
                }
                for r in result
            ],
            "market_regimes": {},
        }
    except Exception:
        return {"hypotheses": [], "market_regimes": {}}
