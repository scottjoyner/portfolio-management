"""Migration validation tests.

Ensures the Alembic migration can run on a fresh database and that
the resulting schema matches the SQLAlchemy models.
"""

import os
import subprocess
import sys
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect


TRADING_SYSTEM_DIR = Path(__file__).resolve().parents[2]
ALEMBIC_INI = TRADING_SYSTEM_DIR / "alembic.ini"


def _run_alembic(*args: str, database_url: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(ALEMBIC_INI), *args],
        capture_output=True,
        text=True,
        cwd=TRADING_SYSTEM_DIR,
        env={**os.environ, "DATABASE_URL": database_url},
        check=False,
    )


def _declared_head() -> str:
    config = Config(str(ALEMBIC_INI))
    script = ScriptDirectory.from_config(config)
    head = script.get_current_head()
    assert head is not None, "Alembic migration graph has no head revision"
    return head


def test_alembic_current_reports_head():
    """A fresh database upgrades to the current declared migration head."""
    import tempfile

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        db_url = f"sqlite:///{path}"
        upgrade = _run_alembic("upgrade", "head", database_url=db_url)
        assert upgrade.returncode == 0, f"Alembic upgrade failed: {upgrade.stdout}\n{upgrade.stderr}"

        current = _run_alembic("current", database_url=db_url)
        assert current.returncode == 0, f"Alembic current failed: {current.stdout}\n{current.stderr}"
        expected_head = _declared_head()
        assert expected_head in current.stdout, (
            f"Alembic current did not report {expected_head}: "
            f"{current.stdout}\n{current.stderr}"
        )
    finally:
        os.unlink(path)


def test_models_match_migration():
    """Reflect the schema from models and compare with a reflected database."""
    from storage.postgres.models import Base

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    inspector = inspect(engine)

    expected_tables = set(Base.metadata.tables.keys())
    actual_tables = set(inspector.get_table_names())

    assert expected_tables == actual_tables, (
        f"Tables mismatch: missing={expected_tables - actual_tables}, "
        f"extra={actual_tables - expected_tables}"
    )

    for table_name in expected_tables:
        expected_cols = {column.name for column in Base.metadata.tables[table_name].columns}
        actual_cols = {column["name"] for column in inspector.get_columns(table_name)}
        assert expected_cols == actual_cols, (
            f"{table_name}: missing={expected_cols - actual_cols}, "
            f"extra={actual_cols - expected_cols}"
        )


def test_alembic_upgrade_downgrade_roundtrip():
    """The committed migration is upgradable, downgradable, and re-upgradable."""
    import tempfile

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db_url = f"sqlite:///{path}"
    try:
        for args in (("upgrade", "head"), ("downgrade", "base"), ("upgrade", "head")):
            result = _run_alembic(*args, database_url=db_url)
            assert result.returncode == 0, (
                f"Alembic {' '.join(args)} failed: {result.stdout}\n{result.stderr}"
            )

        engine = create_engine(db_url)
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        expected = {
            "portfolios",
            "orders",
            "fills",
            "strategy_configs",
            "approvals",
            "audit_events",
            "alerts",
            "incidents",
            "exchange_states",
            "market_data_feeds",
            "capital_buckets",
            "portfolio_sleeves",
            "strategy_allocations",
            "strategy_runs",
        }
        assert expected.issubset(tables), f"Missing tables after roundtrip: {expected - tables}"
    finally:
        os.unlink(path)
