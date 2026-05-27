<<<<<<< HEAD
"""E2E test fixtures for signal-to-fill workflow.

Provides deterministic fixtures for testing the full paper trading lifecycle:
market fixture -> strategy signal -> risk evaluation -> paper order -> simulated fill -> persistence -> audit event -> websocket/notification event
"""

import asyncio
import os
import tempfile
import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from apps.paper_exchange.engine import PaperExchangeEngine
from apps.paper_exchange.engine import PaperExchangeEngine
from core.events.ws_hub import PubSubHub
from core.models.domain import OrderIntent, RiskMode
from core.config.settings import Settings
from exchange.coinbase.reconciliation.service import ExchangeStateReconciler
from risk.engine import RiskEngine, RiskPolicy
from storage.postgres.models import (
    Order, Portfolio, AuditEvent, Fill, Approval, Alert, Incident,
    ExchangeState, MarketDataFeed, CapitalBucket, StrategyConfig, StrategyRun,
)
from storage.postgres.repository import OpsRepository


@pytest.fixture()
def paper_exchange():
    """Create a deterministic paper exchange engine."""
    return PaperExchangeEngine(
        starting_cash=Decimal("1000000"),
        products=["BTC-USD", "ETH-USD"],
    )


@pytest.fixture()
def risk_engine():
    """Create a risk engine with NORMAL mode."""
    engine = RiskEngine(RiskPolicy())
    engine.enable_mode(RiskMode.NORMAL)
    return engine


@pytest.fixture()
def ws_hub():
    """Create an isolated pub/sub hub for testing."""
    return PubSubHub()


@pytest.fixture()
def test_client(paper_exchange, risk_engine):
    """TestClient with overrides for paper exchange and risk."""
    from apps.api.main import app

    def override_get_paper():
        return paper_exchange

    def override_get_risk():
        return risk_engine

    # Store original dependencies
    original_deps = dict(app.dependency_overrides)

    yield TestClient(app)

    # Restore
    app.dependency_overrides.clear()
    app.dependency_overrides.update(original_deps)


@pytest.fixture()
def deterministic_market_state():
    """Return a fixed market state for reproducibility."""
    return {
        "product_id": "BTC-USD",
        "price": 60000.0,
        "mid_price": 60000.0,
        "timestamp": time.time(),
        "volume_24h": 1_234_567,
        "volatility_1h": 0.015,
    }


@pytest.fixture()
def sample_order_intent():
    """Create a sample order intent for testing."""
    return OrderIntent(
        strategy_id="adaptive_spread_mm",
        product_id="BTC-USD",
        side="buy",
        order_type="limit",
        size=Decimal("0.1"),
        price=Decimal("59900"),
        rationale="test_e2e_order",
        risk_mode=RiskMode.NORMAL,
    )


@pytest.fixture()
def seed_portfolios():
    """Seed portfolios into a test DB session (SQLite)."""
    from storage.postgres.session import _SessionLocal
    # Use in-memory SQLite for seeding
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_engine(f"sqlite:///{path}")
    from storage.postgres.models import Base
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        repo = OpsRepository(db)
        repo.seed_default_portfolios()
        db.commit()
    finally:
        db.close()
        engine.dispose()
        os.unlink(path)
    return True
=======
"""Conftest for E2E Coinbase sync tests."""

import pytest
import requests

# Test client for API running on http://localhost:8001
@pytest.fixture
def app_client() -> requests.Session:
    """Create test client for Coinbase read-only sync API."""
    return requests.Session()
>>>>>>> b5e23b51 (Added falcon updates)
