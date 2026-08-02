"""Postgres-backed integration tests for the Ops API.

These tests use the shared integration Postgres database, run migrations,
seed data, and verify API endpoints against a real database.
"""

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.api.main import app
from storage.postgres.models import StrategyConfig
from storage.postgres.repository import OpsRepository
from storage.postgres.session import get_db


@pytest.fixture(scope="module")
def pg_url(postgres_container):
    """Postgres URL for the module."""
    return postgres_container


@pytest.fixture(scope="module")
def pg_engine(pg_url):
    """Create engine and run migrations."""
    engine = create_engine(pg_url, pool_pre_ping=True)
    from alembic import config as alembic_config
    from alembic import command

    alembic_cfg = alembic_config.Config(
        os.path.join(os.path.dirname(__file__), "..", "..", "alembic.ini")
    )
    alembic_cfg.set_main_option("sqlalchemy.url", pg_url)
    command.upgrade(alembic_cfg, "head")
    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(pg_engine):
    """Provide a fresh transactional session per test."""
    conn = pg_engine.connect()
    transaction = conn.begin()
    Session = sessionmaker(bind=conn)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        if transaction.is_active:
            transaction.rollback()
        conn.close()


@pytest.fixture()
def test_client(db_session):
    """TestClient with the actual DB dependency overridden."""
    previous_overrides = dict(app.dependency_overrides)

    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    try:
        with TestClient(app) as client:
            yield client
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous_overrides)


def _seed_supported_strategy(repo: OpsRepository) -> None:
    repo.upsert_strategy_config(
        StrategyConfig(
            strategy_id="adaptive_spread_mm",
            strategy_type="market_making",
            status="implemented",
            enabled=True,
        )
    )


def test_ops_dashboard_with_postgres(test_client, db_session):
    """Dashboard endpoint works with Postgres-backed data."""
    repo = OpsRepository(db_session)
    repo.seed_default_portfolios()

    dash = test_client.get("/ops/dashboard/snapshot")
    assert dash.status_code == 200
    payload = dash.json()
    assert payload["total_nav"] > 0
    assert len(payload["portfolios"]) >= 2


def test_ops_order_lifecycle_with_postgres(test_client, db_session):
    """Order preview/submit/cancel works with Postgres."""
    repo = OpsRepository(db_session)
    repo.seed_default_portfolios()
    _seed_supported_strategy(repo)

    preview = test_client.post(
        "/ops/orders/preview",
        json={
            "portfolio_id": "cb-core-mm",
            "sleeve_id": "maker",
            "strategy_id": "adaptive_spread_mm",
            "product_id": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "size": 0.5,
            "limit_price": 60000,
        },
    )
    assert preview.status_code == 200

    submitted = test_client.post(
        "/ops/orders/submit", json={"preview_id": preview.json()["preview_id"]}
    )
    assert submitted.status_code == 200
    order_id = submitted.json()["order_id"]

    order = db_session.query(
        __import__("storage.postgres.models", fromlist=["Order"]).Order
    ).filter_by(order_id=order_id).first()
    assert order is not None
    assert order.status == "submitted"
