"""Migration validation tests.

Ensures the Alembic migration can run on a fresh database and that
the resulting schema matches the SQLAlchemy models.
"""

import os
import subprocess

import pytest
from sqlalchemy import create_engine, inspect, text


def test_alembic_current_reports_head():
    """Alembic should report 0001 as current head."""
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        db_url = f"sqlite:///{path}"
        result = subprocess.run(
            ["../.venv/bin/alembic", "current"],
            capture_output=True, text=True,
            cwd=os.path.join(os.path.dirname(__file__), "..", ".."),
            env={**os.environ, "DATABASE_URL": db_url},
        )
        assert "0001" in result.stdout, f"Alembic current failed: {result.stderr}"
    finally:
        os.unlink(path)


def test_models_match_migration():
    """Reflect the schema from models and compare with a reflected database."""
    from storage.postgres.models import Base
    engine = create_engine("sqlite:///test_models_migration.db")
    # Create tables from models
    Base.metadata.create_all(engine)
    inspector = inspect(engine)

    expected_tables = set(Base.metadata.tables.keys())
    actual_tables = set(inspector.get_table_names())

    assert expected_tables == actual_tables, f"Tables mismatch: missing={expected_tables - actual_tables}, extra={actual_tables - expected_tables}"

    # Check each table has expected columns
    for table_name in expected_tables:
        expected_cols = {c.name for c in Base.metadata.tables[table_name].columns}
        actual_cols = {c["name"] for c in inspector.get_columns(table_name)}
        assert expected_cols == actual_cols, f"{table_name}: missing={expected_cols - actual_cols}, extra={actual_cols - expected_cols}"


def test_alembic_upgrade_downgrade_roundtrip():
    """Migration should be upgradable and downgradable."""
    # Create a temp DB
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    db_url = f"sqlite:///{path}"
    engine = create_engine(db_url)

    try:
        # Create from models
        from storage.postgres.models import Base
        Base.metadata.create_all(engine)

        # Verify tables exist
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert "portfolios" in tables
        assert "orders" in tables
        assert "fills" in tables
        assert "strategy_configs" in tables
        assert "approvals" in tables
        assert "audit_events" in tables
        assert "alerts" in tables
        assert "incidents" in tables
        assert "exchange_states" in tables
        assert "market_data_feeds" in tables
        assert "capital_buckets" in tables
        assert "portfolio_sleeves" in tables
        assert "strategy_allocations" in tables
        assert "strategy_runs" in tables
    finally:
        os.unlink(path)
