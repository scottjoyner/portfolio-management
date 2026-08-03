"""Migration smoke tests that require an explicitly provisioned Postgres DB."""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, inspect


@pytest.fixture(scope="module")
def fresh_postgres_connection():
    """Connect only when a migration smoke database is explicitly configured."""

    database_url = os.getenv("MIGRATION_SMOKE_DATABASE_URL")
    if not database_url:
        pytest.skip("Set MIGRATION_SMOKE_DATABASE_URL to run Postgres migration smoke tests")

    engine = create_engine(database_url, pool_pre_ping=True)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


def test_migration_completes(fresh_postgres_connection):
    result = fresh_postgres_connection.exec_driver_sql("SELECT 1")
    assert result.scalar_one() == 1


def test_tables_exist(fresh_postgres_connection):
    expected_tables = {
        "portfolios",
        "portfolio_sleeves",
        "strategy_configs",
        "strategy_runs",
        "orders",
        "strategy_allocations",
        "fills",
        "approvals",
        "audit_events",
        "alerts",
        "incidents",
        "exchange_states",
        "market_data_feeds",
        "capital_buckets",
    }
    inspector = inspect(fresh_postgres_connection)
    existing_tables = set(inspector.get_table_names())
    assert expected_tables.issubset(existing_tables), (
        f"Missing migrated tables: {expected_tables - existing_tables}"
    )


def test_primary_keys_exist(fresh_postgres_connection):
    inspector = inspect(fresh_postgres_connection)
    tables = inspector.get_table_names()
    missing = [
        table
        for table in tables
        if table != "alembic_version" and not inspector.get_pk_constraint(table).get("constrained_columns")
    ]
    assert not missing, f"Tables without primary keys: {missing}"


def test_foreign_keys_exist(fresh_postgres_connection):
    inspector = inspect(fresh_postgres_connection)
    foreign_keys = [
        foreign_key
        for table in inspector.get_table_names()
        for foreign_key in inspector.get_foreign_keys(table)
    ]
    assert foreign_keys, "No foreign key constraints found"
