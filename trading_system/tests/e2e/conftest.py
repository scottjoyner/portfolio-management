"""E2E test fixtures for signal-to-fill workflow.

Provides deterministic fixtures for testing the full paper trading lifecycle:
market fixture -> strategy signal -> risk evaluation -> paper order -> simulated fill -> persistence -> audit event -> websocket/notification event
"""

import os
import tempfile
import time
from collections.abc import Generator
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from apps.paper_exchange.engine import PaperExchangeEngine
from core.events.ws_hub import PubSubHub
from core.models.domain import OrderIntent, RiskMode
from risk.engine import RiskEngine, RiskPolicy
from storage.postgres.models import Base
from storage.postgres.repository import OpsRepository
from storage.postgres.session import get_db


@pytest.fixture(scope="session")
def _test_db():
    """Create a persistent temp SQLite DB for the test session."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_engine(f"sqlite:///{path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine)
    yield engine, TestSession
    engine.dispose()
    os.unlink(path)


@pytest.fixture()
def db_session(_test_db) -> Generator[Session, None, None]:
    """Get a fresh DB session for each test."""
    _engine, TestSession = _test_db
    db = TestSession()
    try:
        yield db
    finally:
        db.rollback()
        db.close()


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
def test_client(paper_exchange, risk_engine, db_session):
    """TestClient with overrides for paper exchange, risk, and DB."""
    from apps.api.main import app

    app.dependency_overrides.clear()

    def override_get_paper():
        return paper_exchange

    def override_get_risk():
        return risk_engine

    def override_get_db() -> Generator[Session, None, None]:
        yield db_session

    app.dependency_overrides["get_paper_exchange"] = override_get_paper
    app.dependency_overrides["get_risk_engine"] = override_get_risk
    app.dependency_overrides[get_db] = override_get_db

    yield TestClient(app)

    app.dependency_overrides.clear()


@pytest.fixture()
def app_client():
    """Client for read-only API endpoints that require no database fixture."""
    from apps.api.main import app

    app.dependency_overrides.clear()
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


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
def seed_portfolios(db_session):
    """Seed portfolios and strategy configs into test DB."""
    repo = OpsRepository(db_session)
    repo.seed_default_portfolios()
    from storage.postgres.models import StrategyConfig
    if not db_session.query(StrategyConfig).first():
        db_session.add_all([
            StrategyConfig(strategy_id="adaptive_spread_mm", strategy_type="market_making", enabled=True),
            StrategyConfig(strategy_id="hybrid_hedge", strategy_type="hedge", enabled=True),
        ])
        db_session.commit()
    return True
