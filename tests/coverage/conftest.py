"""Shared pytest fixtures for isolated coverage of trading_system modules.

Several target modules transitively import broken/absent collaborators
(`storage.postgres` classes that were removed, absolute imports like
`core.config.settings`, `strategies.lifecycle`, etc.). Per the coverage task we
MOCK the DB and external integrations. We inject lightweight stub modules and
constructible model classes into ``sys.modules`` at collection time so the real
target module files execute and get measured, while their broken collaborators
don't crash the import.
"""

from __future__ import annotations

import importlib
import os
import sys
import types

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class _Any:
    """Universal placeholder supporting attribute access, calls, comparisons."""

    def __getattr__(self, name: str) -> "_Any":
        return _Any()

    def __call__(self, *args, **kwargs) -> "_Any":
        return _Any()

    def __eq__(self, other) -> bool:
        return False

    def __ne__(self, other) -> bool:
        return True

    def __hash__(self) -> int:
        return id(self)

    def __getitem__(self, key) -> "_Any":
        return _Any()

    def __setitem__(self, key, value) -> None:
        return None

    def __iter__(self):
        return iter([])

    def in_(self, *args, **kwargs) -> "_Any":
        return _Any()


class _Row:
    """Constructible placeholder ORM model used by target modules."""

    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)

    def __getitem__(self, key):
        return self.__dict__.get(key)


def _stub(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__path__ = [name.replace(".", "/")]
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _stub_real(name: str, **override) -> types.ModuleType:
    """Register the REAL module (if importable), merging `override` attrs only
    for names the real module lacks. Falls back to a plain stub if the real
    module cannot be imported. Used so tests that need real symbols (e.g.
    TradingMode, StrategyRegistry, PubSubHub, PriceTargetModel) get them while
    other tests that rely on a stubbed attr keep working."""
    sys.modules.pop(name, None)
    try:
        mod = importlib.import_module(name)
    except Exception:
        return _stub(name, **override)
    for k, v in override.items():
        if not hasattr(mod, k):
            setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _alias(top: str, real: str) -> None:
    """Alias a top-level name (e.g. `core`) to a real `trading_system.*` package
    so `from core.x import` resolves to the real module."""
    import os
    real_dir = os.path.join(REPO, *real.split("."))
    placeholder = types.ModuleType(top)
    placeholder.__path__ = [real_dir]
    sys.modules[top] = placeholder
    try:
        imported = importlib.import_module(real)
    except Exception:
        return
    sys.modules[top] = imported
    sys.modules[real] = imported


# ---------------------------------------------------------------------------
# Constructible model classes the target modules instantiate.
# ---------------------------------------------------------------------------
class _FeedHealthRecord(_Row):
    __table__ = object()
    address = "address"
    network = "network"


_MODEL_CLASSES = {
    "FeedHealthRecord": _FeedHealthRecord,
    "AuditEvent": _Row,
    "Order": _Row,
    "PortfolioSleeve": _Row,
    "StrategyRun": _Row,
    "AuditEventModel": _Row,
    "OrderModel": _Row,
    "StrategyRunModel": _Row,
}


def _install_stubs() -> None:
    # Parent packages must exist so submodule imports resolve to our stubs
    # instead of the real (broken) top-level `storage` namespace package.
    _stub("storage", __path__=["storage"])
    _stub("storage.postgres", __path__=["storage/postgres"])
    _stub("trading_system.storage", __path__=["trading_system/storage"])
    _stub("trading_system.storage.postgres", __path__=["trading_system/storage/postgres"])

    models = dict(_MODEL_CLASSES)
    models["AnalystRating"] = _Row
    models["PriceEstimate"] = _Row
    models["SentimentAnalysis"] = _Row
    models["StrategyApproval"] = _Row
    models["TradeApproval"] = _Row
    models["StrategyConfig"] = _Row
    models["StrategyHypothesis"] = _Row
    models["MarketDataFeed"] = _Row
    models["Approval"] = _Row
    _stub("storage.postgres.models", **models)
    _stub("trading_system.storage.postgres.models", **models)

    class OpsRepository:
        def __init__(self, db=None):
            self.db = db

        def get_portfolio(self, *a, **k):
            return None

    _stub("storage.postgres.repository", OpsRepository=OpsRepository)
    _stub("trading_system.storage.postgres.repository", OpsRepository=OpsRepository)

    def get_db(*a, **k):
        return None

    _stub("storage.postgres.session", get_db=get_db)
    _stub("trading_system.storage.postgres.session", get_db=get_db)

    # --- external integration stubs ---
    class Settings:
        onchain_mode = "paper"

        @classmethod
        def from_env(cls):
            return cls()

    # Real settings module: load it for real so tests see the real Settings
    # (with app_env/trading_mode) and TradingMode. Importing it is safe.
    _stub_real("trading_system.core.config.settings")
    sys.modules["core.config.settings"] = sys.modules["trading_system.core.config.settings"]

    class _Hub:
        async def subscribe(self, *a, **k):
            return None

        async def publish(self, *a, **k):
            return None

        async def unsubscribe(self, *a, **k):
            return None

        def publish_sync(self, *a, **k):
            return None

    # `core` must alias the real trading_system.core package BEFORE the real
    # ws_hub (and other core.* modules) are imported, since their package
    # __init__ files import via the `core` name.
    _alias("core", "trading_system.core")
    _stub_real("trading_system.core.events.ws_hub", hub=_Hub())
    sys.modules["core.events.ws_hub"] = sys.modules["trading_system.core.events.ws_hub"]

    class _Run:
        def __init__(self, task_id="t", status="queued", queued_at=None):
            self.task_id = task_id
            self.status = status
            self.queued_at = queued_at

    class StrategyLifecycleManager:
        def __init__(self, repo=None):
            self.repo = repo

        def start(self, sid):
            return _Run(status="queued")

        def stop(self, tid):
            return _Run(status="stopped")

        def pause(self, tid):
            return _Run(status="paused")

        def resume(self, tid):
            return _Run(status="running")

        def enable(self, sid):
            return True

        def disable(self, sid):
            return True

    _stub("strategies.lifecycle", StrategyLifecycleManager=StrategyLifecycleManager)
    _stub("trading_system.strategies.lifecycle", StrategyLifecycleManager=StrategyLifecycleManager)

    def list_all_phase1_strategies():
        return []

    _stub_real("trading_system.catalog.strategy_registry",
               list_all_phase1_strategies=list_all_phase1_strategies)
    _stub_real("trading_system.catalog",
               list_all_phase1_strategies=list_all_phase1_strategies)

    class CoinbaseService:
        def get_connection_status(self):
            return {"connected": True, "error": None}

        def get_balances_snapshot(self):
            return None

        def get_price(self, product_id):
            return {"price": 100.0}

    _stub("trading_system.core.exchange.coinbase_service",
          CoinbaseService=CoinbaseService)
    _stub("trading_system.core.exchange", CoinbaseService=CoinbaseService)

    class EventRecorder:
        def __init__(self, *a, **k):
            self.path = _PathStub()

        def tail(self, *a, **k):
            return []

    class _PathStub:
        def exists(self):
            return True

    _stub("trading_system.core.runtime.events", EventRecorder=EventRecorder)
    _stub("trading_system.core.runtime", EventRecorder=EventRecorder)

    class RuntimeStatus:
        def __init__(self, **kw):
            self._d = kw

        def to_dict(self):
            return dict(self._d)

    _stub("trading_system.core.runtime.models", RuntimeStatus=RuntimeStatus)
    _stub("trading_system.core.runtime", RuntimeStatus=RuntimeStatus)

    # evaluation.pricing_models: use the REAL module (it has PriceTargetModel and
    # PriceEstimationEngine); the namespace `evaluation` stub below is removed in
    # favour of a real `trading_system.evaluation` alias set at the end.
    class PriceEstimationEngine:
        def __init__(self, config=None):
            self.config = config or {}

    _stub_real("trading_system.evaluation.pricing_models", PriceEstimationEngine=PriceEstimationEngine)

    # --- top-level aliases so `from core/evaluation/catalog/strategies import`
    # resolves to the real trading_system.* packages (the 9 re-enabled test
    # modules import the real source modules directly) ---
    _alias("evaluation", "trading_system.evaluation")
    _alias("catalog", "trading_system.catalog")
    _alias("strategies", "trading_system.strategies")

    # Local helper modules (strat_helpers, _env) live next to their test files.
    for _sub in ("strategies", "evaluation"):
        _p = os.path.join(REPO, "tests", "coverage", _sub)
        if _p not in sys.path:
            sys.path.insert(0, _p)


_install_stubs()


# ---------------------------------------------------------------------------
# Async test support (no pytest-asyncio available in this environment).
# ---------------------------------------------------------------------------
import asyncio
import inspect


def pytest_pyfunc_call(pyfuncitem):
    func = pyfuncitem.obj
    if inspect.iscoroutinefunction(func):
        asyncio.run(func(**pyfuncitem.funcargs))
        return True
    return None


# ---------------------------------------------------------------------------
# Extra stubs required by the DB-backed target modules under coverage.
# ---------------------------------------------------------------------------

class _Col:
    """Minimal ORM column placeholder supporting == and .in_()."""

    def __init__(self, name: str = ""):
        self.name = name

    def in_(self, *args, **kwargs):
        return _Col()

    def __eq__(self, other):
        return _Col()

    def __ne__(self, other):
        return _Col()


class _Approval(_Row):
    status = _Col("status")
    approval_id = _Col("approval_id")


_M = sys.modules["storage.postgres.models"]
_M2 = sys.modules["trading_system.storage.postgres.models"]
_M.Approval = _Approval
_M2.Approval = _Approval
_M.plaid_items_table = _Any()
_M2.plaid_items_table = _Any()


def _install_plaid_stubs() -> None:
    plaid = types.ModuleType("plaid")
    plaid.__path__ = ["plaid"]
    plaid_models = types.ModuleType("plaid.models")

    class ConsentState:
        GRANTED = "GRANTED"
        REVOKED = "REVOKED"

    plaid_models.ConsentState = ConsentState

    plaid_db = types.ModuleType("plaid.database_models")

    class PlaidItem:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    plaid_db.PlaidItem = PlaidItem

    sys.modules["plaid"] = plaid
    sys.modules["plaid.models"] = plaid_models
    sys.modules["plaid.database_models"] = plaid_db


_install_plaid_stubs()
