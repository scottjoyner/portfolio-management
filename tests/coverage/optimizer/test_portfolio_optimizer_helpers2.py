"""Coverage for miscellaneous PortfolioOptimizer helpers / persistence methods
and the module-level market-regime classifier."""
from __future__ import annotations

import json
from unittest import mock

import portfolio_optimizer as P
from tests.coverage.optimizer.conftest import holding, make_state


# ----------------------------------------------------------- regime classifier
def test_detect_market_regime_short():
    assert P._detect_market_regime([1, 2, 3], [1, 2, 3], [1, 2, 3]) == "neutral"


def test_detect_market_regime_trending():
    with mock.patch.object(P, "_compute_adx", return_value=30.0):
        assert P._detect_market_regime([1] * 40, [1] * 40, list(range(30))) == "trending"


def test_detect_market_regime_volatile():
    with mock.patch.object(P, "_compute_adx", return_value=10.0):
        closes = [100.0 + (i % 2) * 20 for i in range(30)]
        assert P._detect_market_regime([1] * 40, [1] * 40, closes) == "volatile"


def test_detect_market_regime_ranging():
    with mock.patch.object(P, "_compute_adx", return_value=10.0):
        closes = [100.0 + i * 0.001 for i in range(30)]
        assert P._detect_market_regime([1] * 40, [1] * 40, closes) == "ranging"


def test_detect_market_regime_neutral():
    with mock.patch.object(P, "_compute_adx", return_value=22.0):
        assert P._detect_market_regime([1] * 40, [1] * 40, list(range(30))) == "neutral"


# --------------------------------------------------------------- save / refresh
def test_save_state_no_neo4j(opt):
    opt.state = make_state({"SOL": holding("SOL", 1000, "speculative")})
    opt.store = mock.MagicMock()
    opt.position_ages = {}
    opt.neo4j_store = None
    opt._save_state()
    opt.store.save_snapshot.assert_called_once()


def test_save_state_with_neo4j(opt):
    opt.state = make_state({"SOL": holding("SOL", 1000, "speculative")})
    opt.store = mock.MagicMock()
    opt.position_ages = {}
    opt.neo4j_store = mock.MagicMock()
    opt._save_state()
    opt.neo4j_store.save_snapshot.assert_called_once()


def test_refresh_capital_policy_default(opt):
    opt.store = mock.MagicMock()
    opt.store.get_meta.return_value = None
    opt._forced_max_deployable_usd = 0.0
    pol = opt._refresh_capital_policy()
    assert isinstance(pol, dict)


def test_refresh_capital_policy_forced(opt):
    opt.store = mock.MagicMock()
    opt.store.get_meta.return_value = json.dumps({"max_deployable_usd": 500.0})
    opt._forced_max_deployable_usd = 1000.0
    pol = opt._refresh_capital_policy()
    assert pol.get("max_deployable_usd") == 1000.0


# -------------------------------------------------------------------- stop / gc
def test_stop_all_branches(opt):
    opt._bracket_mgr = mock.MagicMock()
    opt._health_server = mock.MagicMock()
    opt._feed_mgr = mock.MagicMock()
    opt.graph_store = mock.MagicMock()
    with mock.patch("fcntl.flock"), mock.patch("os.close"), mock.patch("os.remove"):
        opt._lock_fd = 5
        opt.stop()
    assert opt.running is False
    opt._bracket_mgr.stop_polling.assert_called_once()


def test_stop_minimal(opt):
    opt._bracket_mgr = None
    opt._lock_fd = None
    opt._health_server = None
    opt._feed_mgr = None
    opt.graph_store = None
    opt.stop()
    assert opt.running is False


# ----------------------------------------------------------------- get_candles
# (get_candles lives on CoinbaseCLI, covered in test_portfolio_optimizer_cli.py)
