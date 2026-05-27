"""Integration test fixtures and database setup for P0.2."""

import pytest
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

try:
    from sqlalchemy import select, text
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    SQLAlchemyError = Exception

try:
    from trading_system.storage.postgres.models import (
        Portfolio, PortfolioSleeve, StrategyConfig, Order, Fill,
        CapitalBucket, Approval, AuditEvent, Alert, Incident,
        MarketDataFeed, ExchangeState, TokenMetadata, PoolSnapshot,
        ContractEvent, FeedHealthRecord
    )
except ImportError:
    from storage.postgres.models import (
        Portfolio, PortfolioSleeve, StrategyConfig, Order, Fill,
        CapitalBucket, Approval, AuditEvent, Alert, Incident,
        MarketDataFeed, ExchangeState, TokenMetadata, PoolSnapshot,
        ContractEvent, FeedHealthRecord
    )


@pytest.fixture(scope="session")
def test_config() -> Dict[str, Any]:
    """Configuration for integration test database."""
    return {
        "test_db_name": "trading_system_test",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres" if not pytest.config.getoption("--live") else "LIVE_DB_PASSWORD",
    }


@pytest.fixture(scope="function")
def clean_db(test_config: Dict[str, Any]) -> None:
    """Drop and recreate database for each test."""
    # This runs before each test function
    async def _cleanup_session(session):
        # Get current DB name from connection info
        await session.execute(text(f"""
            DROP DATABASE IF EXISTS {test_config['test_db_name']} CASCADE;
            CREATE DATABASE {test_config['test_db_name']};
            GRANT ALL PRIVILEGES ON SCHEMA PUBLIC TO postgres;
        """))
    
    pytest.loop.on_event(_cleanup_session)


@pytest.fixture
def db_session(test_config: Dict[str, Any]) -> AsyncSession:
    """Get async database session for integration tests."""
    try:
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
        from sqlalchemy.orm import sessionmaker
        
        engine_url = f"postgresql+asyncpg://{test_config['user']}:{test_config['password']}@{test_config['host']}:{test_config['port']}/{test_config['test_db_name']}"
        engine = create_async_engine(engine_url, echo=False)
        
        async_session_factory = sessionmaker(
            engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        
        @pytest.fixture(scope="function")
        def session():
            """Return a fresh session for each test."""
            async def _get_session():
                async with engine.begin() as conn:
                    # Auto-create all tables (only for integration tests)
                    await conn.run_sync(Base.metadata.create_all)
                
                db = async_session_factory()
                try:
                    yield db
                    await db.rollback()
                except Exception as e:
                    await db.rollback()
                    raise
            return asyncio.run(_get_session())
        
        session()
    except ImportError:
        # Fallback to regular sessionmaker if needed
        pytest.skip("Async PostgreSQL driver not installed")


@pytest.fixture
def sample_portfolio(db_session) -> Portfolio:
    """Create a sample portfolio for tests."""
    portfolio = Portfolio(
        id="test-portfolio-001",
        name="Alpha Growth Fund",
        objective="GROWTH",
        nav=50000.0,
        available_capital=48000.0,
        locked_capital=2000.0,
        realized_pnl=750.0,
        unrealized_pnl=125.0,
        liquidity_score=0.95,
        capital_efficiency=0.88,
    )
    db.add(portfolio)
    return portfolio


@pytest.fixture
def sample_strategy(sample_portfolio: Portfolio) -> StrategyConfig:
    """Create a sample strategy config."""
    strategy = StrategyConfig(
        strategy_id="maker-taker-synth",
        strategy_type="SYNTH",
        status="implemented",
        paper_mode=True,
        live_supported=False,
        replay_supported=True,
        backtest_supported=True,
        risk_mode_hint="NORMAL",
        capital_bucket="ACTIVE_TRADING",
        enabled=True,
    )
    db.add(strategy)
    return strategy


@pytest.fixture
def seed_test_data(db_session):
    """Seed database with comprehensive test data."""
    # Create sample portfolio
    portfolio = Portfolio(
        id="test-port-001",
        name="Demo Growth Portfolio",
        objective="GROWTH",
        nav=25000.0,
        available_capital=24000.0,
        locked_capital=1000.0,
        realized_pnl=50.0,
        unrealized_pnl=8.33,
        liquidity_score=0.92,
        capital_efficiency=0.85,
    )
    
    # Add portfolio
    db.add(portfolio)
    
    return {
        "portfolio": portfolio,
        "id": "test-port-001",
    }
