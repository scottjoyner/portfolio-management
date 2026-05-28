"""Shared fixtures for DB-backed integration tests.

Uses a real Postgres container for tests that need database-backed
API coverage, migration validation, and repository persistence checks.
"""

import os
import subprocess
import tempfile

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from storage.postgres.models import Base

# In-memory SQLite for tests that don't need Postgres
_sqlite_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_sqlite_path = _sqlite_db.name
_sqlite_db.close()

sqlite_engine = create_engine(f"sqlite:///{_sqlite_path}", connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)
Base.metadata.create_all(bind=sqlite_engine)


@pytest.fixture(scope="session")
def sqlite_engine():
    """Session-scoped SQLite engine for tests that don't need Postgres."""
    yield sqlite_engine


@pytest.fixture(scope="session")
def testing_db_url():
    """Return a test database URL. Tries Postgres first, falls back to SQLite."""
    pg_url = os.environ.get("TEST_DATABASE_URL")
    if pg_url:
        return pg_url
    return f"sqlite:///{_sqlite_path}"


@pytest.fixture()
def db_session():
    """Provide a transactional rollback per test (SQLite)."""
    conn = sqlite_engine.connect()
    transaction = conn.begin()
    session = sessionmaker(bind=conn)()
    yield session
    session.close()
    transaction.rollback()
    conn.close()


@pytest.fixture(scope="session")
def postgres_container():
    """Start a Postgres container for the session, clean up after."""
    # Check if we already have one
    existing = subprocess.run(
        ["docker", "ps", "-q", "--filter", "name=trading-integration-test"],
        capture_output=True, text=True
    )
    if existing.stdout.strip():
        yield "postgresql://postgres:testpass@localhost:54433/trading_int"
        return

    # Start container
    result = subprocess.run(
        ["docker", "run", "-d", "--name", "trading-integration-test",
         "-e", "POSTGRES_USER=postgres",
         "-e", "POSTGRES_PASSWORD=testpass",
         "-e", "POSTGRES_DB=trading_int",
         "-p", "54433:5432", "postgres:16-alpine"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        pytest.skip("Docker not available: " + result.stderr)
        return

    # Wait for ready
    import time
    for _ in range(20):
        r = subprocess.run(
            ["docker", "exec", "trading-integration-test", "pg_isready"],
            capture_output=True
        )
        if r.returncode == 0:
            break
        time.sleep(0.5)
    else:
        pytest.skip("Postgres container failed to start")
        return

    yield "postgresql://postgres:testpass@localhost:54433/trading_int"

    # Cleanup
    subprocess.run(["docker", "stop", "trading-integration-test"], capture_output=True)
    subprocess.run(["docker", "rm", "trading-integration-test"], capture_output=True)
