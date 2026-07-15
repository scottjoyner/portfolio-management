"""Shared test environment for coverage tests of research/ and evaluation/.

The target modules import several model/repository classes that are not present
in the current codebase (e.g. ``storage.postgres.models.StrategyHypothesis``,
``StrategyApproval``, ``TradeApproval``, ``AnalystRating`` ...) and a broken
``storage.postgres.repository`` module.  To exercise the business logic without a
real database we inject lightweight stub modules into ``sys.modules`` *before*
importing the modules under test.  All DB access is subsequently mocked with
:class:`unittest.mock.MagicMock`.
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_model(name: str, fields=(), **defaults):
    """Build a throwaway ORM-like class that stores arbitrary kwargs.

    ``fields`` are registered as class-level column attributes (MagicMock) so
    that ORM-style expressions such as ``Model.col == value``,
    ``Model.col.is_(True)`` and ``Model.col.desc()`` work during tests.
    """

    class _Model:
        def __init__(self, **kwargs):
            for key, val in defaults.items():
                setattr(self, key, val)
            for key, val in kwargs.items():
                setattr(self, key, val)

        def __repr__(self):
            return f"<{name}>"

    _Model.__name__ = name
    _Model.__qualname__ = name
    for _f in fields:
        setattr(_Model, _f, MagicMock())
    return _Model


def _build_models_module() -> types.ModuleType:
    mod = types.ModuleType("stub_models")
    mod.StrategyConfig = _make_model(
        "StrategyConfig",
        fields=("strategy_id", "hypothesis_id", "status", "config_hash",
                "certification_status", "enabled"),
    )
    mod.StrategyHypothesis = _make_model(
        "StrategyHypothesis",
        fields=("hypothesis_id", "strategy_id", "active", "config_hash",
                "philosophy", "timeframe", "expected_edge", "target_instruments",
                "holding_period", "signal_rules", "exit_rules", "risk_constraints",
                "created_at", "version"),
        active=True, version="1.0", created_at=_now(),
    )
    mod.StrategyApproval = _make_model(
        "StrategyApproval",
        fields=("approval_id", "strategy_id", "hypothesis_id", "status",
                "required_approver", "approved_by", "status_reason", "expires_at",
                "created_at", "config_hash", "philosophy", "target_instruments",
                "signal_rules", "backtest_evidence_json", "expected_return_range",
                "holding_period", "exit_criteria"),
        status="pending", created_at=_now(),
    )
    mod.TradeApproval = _make_model(
        "TradeApproval",
        fields=("approval_id", "strategy_id", "product_id", "status",
                "order_type", "side", "expected_slippage_bps", "fill_risk_score",
                "approved_by", "created_at", "strategy_approval_id"),
        status="pending", created_at=_now(),
    )
    mod.StrategyRun = _make_model(
        "StrategyRun",
        fields=("task_id", "strategy_id", "mode", "status", "started_at",
                "queued_at"),
        created_at=_now(),
    )
    mod.AnalystRating = _make_model(
        "AnalystRating",
        fields=("instrument", "analyst", "rating_text", "price_target", "created_at"),
    )
    mod.PriceEstimate = _make_model(
        "PriceEstimate",
        fields=("instrument", "current_market_price", "dcf_intrinsic_value",
                "technical_score", "consensus_vs_current_pct", "confidence_score",
                "timestamp"),
    )
    mod.SentimentAnalysis = _make_model(
        "SentimentAnalysis",
        fields=("product_id", "regime", "bullish_pct", "bearish_pct",
                "sentiment_score", "timestamp"),
    )
    mod.MarketDataFeed = _make_model(
        "MarketDataFeed",
        fields=("feed_name", "state", "freshness_ms"),
    )
    return mod


def _build_repo_module() -> types.ModuleType:
    mod = types.ModuleType("stub_repo")

    class OpsRepository:
        def __init__(self, db):
            self.db = db

    mod.OpsRepository = OpsRepository
    return mod


def _build_backtest_models_module() -> types.ModuleType:
    mod = types.ModuleType("stub_backtest_models")
    mod.StrategyCertification = _make_model(
        "StrategyCertification",
        fields=("id", "hypothesis_id", "strategy_id", "status",
                "certification_status", "sharpe", "max_drawdown", "total_return",
                "win_rate", "profit_factor", "live_transfer_confidence",
                "fragility_score", "rejection_reason", "certified_at", "created_at"),
        certified_at=None, created_at=_now(),
    )
    mod.StrategyRun = _make_model(
        "StrategyRun",
        fields=("task_id", "strategy_id", "mode", "status", "started_at",
                "queued_at"),
        created_at=_now(),
    )
    return mod


_INSTALLED = False


def install_stubs() -> None:
    """Inject stub modules so the target modules can be imported.

    Must be called *before* importing any of the modules under test.
    """
    global _INSTALLED
    if _INSTALLED:
        return

    models = _build_models_module()
    repo = _build_repo_module()
    backtest_models = _build_backtest_models_module()

    # The same stub model/repo objects are exposed under both the top-level
    # ``storage.postgres.*`` names used by research/* and the
    # ``trading_system.storage.postgres.*`` names used by evaluation/*.
    for name in (
        "storage.postgres.models",
        "trading_system.storage.postgres.models",
    ):
        sys.modules[name] = models

    for name in (
        "storage.postgres.repository",
        "trading_system.storage.postgres.repository",
    ):
        sys.modules[name] = repo

    sys.modules["backtest.models"] = backtest_models

    _INSTALLED = True


def make_db() -> "MagicMock":  # noqa: F821
    from unittest.mock import MagicMock

    return MagicMock(name="db_session")
