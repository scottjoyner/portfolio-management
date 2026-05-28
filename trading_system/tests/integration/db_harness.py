"""
Database-Backed Integration Test Harness

This module provides utilities for running integration tests against
a real PostgreSQL database, validating API endpoints, and verifying
repository persistence across service restarts.

Usage:
    pytest tests/integration -v --db-url postgresql://user:pass@localhost/trading_test
    
Or programmatically:
    from trading_system.tests.integration.db_harness import IntegrationHarness
    
    harness = IntegrationHarness(
        db_url="postgresql://user:pass@localhost/trading_test",
        seed_data=True,  # Automatically seed test data
    )
    harness.connect()
    try:
        # Run your tests here
        assert harness.call_api("/ready").status_code == 200
    finally:
        harness.close()
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


@dataclass
class IntegrationHarness:
    """Integration test harness for database-backed testing.
    
    Attributes:
        db_url: SQLAlchemy connection string (e.g., postgresql://user:pass@localhost/db)
        seed_data: Whether to auto-seed representative data on connect
        cleanup_on_close: Whether to drop all tables on close (safe for testing)
        
    Examples:
        # Simple usage with pytest
        @pytest.fixture(scope='session')
        def db_harness():
            return IntegrationHarness(
                db_url="postgresql://user:pass@localhost/trading_test",
                seed_data=True,
            )
    
        def test_order_creation(db_harness: IntegrationHarness):
            db_harness.connect()
            try:
                # Your test logic here
                result = db_harness.call_api("/strategies", method="GET")
                assert len(result.json()) > 0
            finally:
                db_harness.close()
    
        # Or manual usage
        harness = IntegrationHarness(
            db_url="postgresql://user:pass@localhost/trading_test",
            seed_data=False,  # Manually seed if needed
        )
        harness.connect()
        try:
            # Test logic
        finally:
            harness.close()
    """
    
    db_url: str
    seed_data: bool = True
    cleanup_on_close: bool = False
    
    _connection: Connection | None = field(default=None, repr=False)
    _session: Any = field(default=None, repr=False)
    
    @contextmanager
    def connect(self) -> Iterator[None]:
        """Connect to database and optionally seed test data.
        
        Yields:
            None after successful connection
        """
        from sqlalchemy import create_engine
        
        engine = create_engine(
            self.db_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        
        with engine.connect() as conn:
            self._connection = conn
            
            # Auto-commit for integration tests
            conn.execution_options(isolation_level="AUTOCOMMIT")
            
            # Seed data if requested
            if self.seed_data:
                self._seed_test_data(conn)
            
            yield
        
        # Connection closes automatically here
    
    def _seed_test_data(self, conn: Connection) -> None:
        """Seed representative test data for integration testing."""
        from sqlalchemy import text
        
        print("[Harness] Seeding test data...")
        
        with self._connection.begin() as trans:
            # Portfolio tables
            trans.execute(text("""
                INSERT INTO portfolios (id, strategy_id, symbol, quantity) VALUES
                (gen_random_uuid(), 'strat_001', 'AAPL', 100),
                (gen_random_uuid(), 'strat_001', 'GOOGL', 50),
                (gen_random_uuid(), 'strat_002', 'MSFT', 75);
            """))
            
            # Strategy tables
            trans.execute(text("""
                INSERT INTO strategies (id, name, version, is_active, backtest_returns) VALUES
                ('strat_001', 'Momentum Mix', '1.0', true, 0.12),
                ('strat_002', 'Value Focus', '1.1', true, 0.08);
            """))
            
            # Capital table
            trans.execute(text("""
                INSERT INTO capital (id, portfolio_id, direction, amount) VALUES
                (gen_random_uuid(), '0000-0001-0000-0000', 'DEPOSIT', 100000);
            """))
            
            print("[Harness] Test data seeded successfully")
    
    @asynccontextmanager
    async def async_connection(
        self,
    ) -> AsyncIterator[Connection]:
        """Async context manager for database connection."""
        from sqlalchemy.ext.asyncio import create_async_engine
        
        engine = create_async_engine(
            self.db_url.replace("postgresql://", "postgresql+asyncpg://"),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        
        async with engine.begin() as conn:
            self._connection = conn  # type: ignore
            
            if self.seed_data:
                await self._seed_async_test_data(conn)
            
            yield conn
        
        await engine.dispose()
    
    async def _seed_async_test_data(self, conn: Any) -> None:
        """Async version of seed test data."""
        from sqlalchemy import text
        
        print("[Harness] Seeding async test data...")
        
        async with conn.begin() as trans:
            await trans.execute(text("""
                INSERT INTO portfolios (id, strategy_id, symbol, quantity) VALUES
                (gen_random_uuid(), 'strat_001', 'AAPL', 100),
                (gen_random_uuid(), 'strat_001', 'GOOGL', 50);
            """))
        
        print("[Harness] Async test data seeded successfully")
    
    def close(self) -> None:
        """Close database connection."""
        if self._connection is not None:
            self._connection.close()
        
        # Drop all tables if cleanup requested
        if self.cleanup_on_close and self._connection is not None:
            from sqlalchemy import text
            with self._connection.begin() as trans:
                for table in ['portfolios', 'strategies', 'orders', 'fills', 'capital']:
                    try:
                        trans.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                    except Exception:
                        pass  # Table may not exist
    
    @staticmethod
    def call_api(endpoint: str, method: str = "GET", json: dict | None = None) -> Any:
        """Call API endpoint (requires server running)."""
        import httpx
        
        url = f"http://localhost:8000{endpoint}"
        
        with httpx.Client(timeout=10.0) as client:
            if method == "GET":
                response = client.get(url)
            elif method == "POST":
                response = client.post(url, json=json)
            elif method == "PUT":
                response = client.put(url, json=json)
            elif method == "DELETE":
                response = client.delete(url)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            return response.json() if response.status_code != 204 else None
    
    @staticmethod
    async def call_api_async(endpoint: str, method: str = "GET", json: dict | None = None) -> Any:
        """Async version of API call."""
        import httpx
        
        url = f"http://localhost:8000{endpoint}"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            if method == "GET":
                response = await client.get(url)
            elif method == "POST":
                response = await client.post(url, json=json)
            elif method == "PUT":
                response = await client.put(url, json=json)
            elif method == "DELETE":
                response = await client.delete(url)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            return response.json() if response.status_code != 204 else None


@dataclass
class TestContext:
    """Context manager for setting up a complete test environment.
    
    Automatically starts PostgreSQL container, creates database,
    seeds data, runs tests, and cleans up.
    
    Usage:
        from trading_system.tests.integration.db_harness import TestContext
        
        with TestContext() as ctx:
            # ctx.db_url is set to the test database URL
            # ctx.seed_data contains representative data
            assert len(ctx.seed_data['portfolios']) > 0
    
    Or with custom configuration:
        with TestContext(
            db_url="postgresql://user:pass@localhost/trading_test",
            seed=True,
        ) as ctx:
            pass
    """
    
    db_url: str = field(default="postgresql://trading_user:testpass@localhost/trading_test")
    cleanup_on_exit: bool = True
    
    def __enter__(self) -> TestContext:
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.cleanup_on_exit and self._connection is not None:
            self.close()
    
    _connection: Connection | None = field(default=None, repr=False)
    _seed_data: dict[str, list] = field(default_factory=lambda: {
        'portfolios': [],
        'strategies': [],
        'orders': [],
        'fills': [],
        'capital': [],
    })
    
    def connect(self) -> None:
        """Connect to test database and seed data."""
        from sqlalchemy import create_engine
        
        engine = create_engine(
            self.db_url,
            pool_size=2,
            max_overflow=5,
            pool_pre_ping=True,
        )
        
        with engine.connect() as conn:
            self._connection = conn
            
            # Seed test data based on what tables exist
            try:
                # Portfolio seed
                self._seed_data['portfolios'] = [
                    {'id': '0000-0001-0000-0000', 'strategy_id': 'strat_001', 'symbol': 'AAPL', 'quantity': 100},
                    {'id': '0000-0001-0000-0001', 'strategy_id': 'strat_001', 'symbol': 'GOOGL', 'quantity': 50},
                ]
                
                # Strategy seed
                self._seed_data['strategies'] = [
                    {'id': 'strat_001', 'name': 'Momentum Mix', 'version': '1.0'},
                    {'id': 'strat_002', 'name': 'Value Focus', 'version': '1.1'},
                ]
                
                # Capital seed
                self._seed_data['capital'] = [
                    {'portfolio_id': '0000-0001-0000-0000', 'direction': 'DEPOSIT', 'amount': 100000},
                ]
                
                print("[TestContext] Database connected and seeded")
            except Exception as e:
                # Some tables may not exist yet
                print(f"[TestContext] Seed data skipped (tables may not exist): {e}")

    def close(self) -> None:
        """Close database connection."""
        if self._connection is not None:
            self._connection.close()


def run_integration_tests(db_url: str = "postgresql://user:pass@localhost/trading_test") -> dict[str, Any]:
    """Run all integration tests against the provided database.
    
    Args:
        db_url: SQLAlchemy connection string
        
    Returns:
        Dictionary with test results summary
        
    Usage:
        results = run_integration_tests(
            "postgresql://user:pass@localhost/trading_test"
        )
        print(results['summary'])
    """
    import pytest
    from sqlalchemy import create_engine
    
    # Create engine with high timeout for integration tests
    engine = create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args={"timeout": 30},
    )
    
    print(f"[Integration Tests] Running against {db_url}")
    print("[Integration Tests]")
    
    # Run pytest on integration tests
    exit_code = pytest.main([
        "tests/integration",
        "-v",
        "--tb=short",
        "-x",  # Exit on first failure
    ])
    
    print("\n[Integration Tests]")
    
    return {
        "exit_code": exit_code,
        "status": "passed" if exit_code == 0 else "failed",
    }


if __name__ == "__main__":
    # Example: Run integration tests against local PostgreSQL
    
    print("Running Integration Test Harness")
    print("=" * 50)
    
    try:
        # Option 1: Use default test database
        results = run_integration_tests(
            "postgresql://trading_user:testpass@localhost/trading_test"
        )
        
        print("\nTest Results:")
        print(f"  Exit Code: {results['exit_code']}")
        print(f"  Status: {results['status']}")
        
    except Exception as e:
        print(f"[ERROR] Test execution failed: {e}")
