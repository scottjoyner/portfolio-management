"""Database engine and session lifecycle for the canonical storage package."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

_engine: Engine | None = None
_SessionLocal: Any = None


def init_db(database_url: str) -> Engine:
    global _engine, _SessionLocal

    engine_kwargs: dict[str, Any] = {"pool_pre_ping": True}
    if database_url in {"sqlite://", "sqlite:///:memory:"}:
        engine_kwargs.update(
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    elif database_url.startswith("sqlite:"):
        engine_kwargs.update(connect_args={"check_same_thread": False})
    else:
        engine_kwargs.update(pool_size=5, max_overflow=10)

    _engine = create_engine(database_url, **engine_kwargs)
    _SessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
        bind=_engine,
    )
    return _engine


def get_engine() -> Engine:
    if _engine is None:
        raise RuntimeError("Database not initialized; call init_db() first")
    return _engine


def get_db() -> Generator[Session, None, None]:
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized; call init_db() first")
    db = _SessionLocal()
    try:
        yield db
    finally:
        db.close()


__all__ = ["get_db", "get_engine", "init_db"]
