from __future__ import annotations

import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from trading_system.catalog.strategy_registry import list_all_phase1_strategies
from trading_system.core.exchange.coinbase_service import CoinbaseService
from trading_system.core.runtime.events import EventRecorder
from trading_system.core.runtime.models import RuntimeStatus

app = FastAPI(title="Trading System Control API")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def current_mode() -> str:
    return os.getenv("TRADING_MODE", "paper").lower()


def get_coinbase_service() -> CoinbaseService:
    return CoinbaseService()


def get_event_recorder() -> EventRecorder:
    return EventRecorder()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict:
    return {"status": "ok", "database": "not_required_for_read_only_api"}


@app.get("/mode")
def mode() -> dict:
    return {"mode": current_mode()}


@app.get("/runtime/status")
def runtime_status() -> dict:
    coinbase = get_coinbase_service().get_connection_status()
    recorder = get_event_recorder()
    status = RuntimeStatus(
        mode=current_mode(),
        live_trading_enabled=_env_bool("LIVE_TRADING_ENABLED", False),
        coinbase_connected=bool(coinbase.get("connected")),
        worker_status=os.getenv("WORKER_STATUS", "unknown"),
        event_log_status="available" if recorder.path.exists() else "empty",
    )
    data = status.to_dict()
    data["coinbase_error"] = coinbase.get("error")
    return data


@app.get("/coinbase/status")
def coinbase_status() -> dict:
    return get_coinbase_service().get_connection_status()


@app.get("/coinbase/balances")
def coinbase_balances() -> dict:
    try:
        return get_coinbase_service().get_balances_snapshot().to_dict()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/coinbase/prices/{product_id}")
def coinbase_price(product_id: str) -> dict:
    try:
        return get_coinbase_service().get_price(product_id.upper())
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/strategies/catalog")
def strategy_catalog(category: Optional[str] = None) -> dict:
    strategies = list_all_phase1_strategies()
    if category:
        strategies = [strategy for strategy in strategies if strategy.get("category") == category]
    return {"count": len(strategies), "strategies": strategies}


@app.get("/strategies/status")
def strategies_status() -> dict:
    strategies = list_all_phase1_strategies()
    statuses = []
    for strategy in strategies:
        statuses.append(
            {
                "strategy_id": strategy["name"],
                "name": strategy["name"],
                "category": strategy["category"],
                "enabled": False,
                "mode": "paper",
                "last_tick_at": None,
                "last_signal": None,
                "last_error": None,
                "status": strategy.get("status", "unknown"),
            }
        )
    return {"count": len(statuses), "strategies": statuses}


@app.get("/strategies/{strategy_id}")
def strategy_detail(strategy_id: str) -> dict:
    for strategy in list_all_phase1_strategies():
        if strategy.get("name") == strategy_id:
            return strategy
    raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")


@app.get("/events")
def events(
    limit: int = Query(default=100, ge=1, le=1000),
    strategy_id: Optional[str] = None,
    source: Optional[str] = None,
    event_type: Optional[str] = None,
) -> dict:
    event_list = get_event_recorder().tail(
        limit=limit,
        strategy_id=strategy_id,
        source=source,
        event_type=event_type,
    )
    return {"count": len(event_list), "events": event_list}
