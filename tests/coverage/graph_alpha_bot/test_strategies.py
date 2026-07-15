from datetime import datetime
from unittest.mock import MagicMock, patch
import pandas as pd

from app.strategies.base import Strategy
from app.strategies.ma_crossover import MovingAverageCrossover
from app.strategies.news_centrality_momentum import NewsCentralityMomentum
from app.strategies.supply_chain_shock_diffusion import SupplyChainShockDiffusion
from app.strategies.insider_cluster_drift import InsiderClusterDrift


def _make_session(run_return_value=None, single_value=None):
    sess = MagicMock()
    # make the session a proper context manager that returns itself,
    # mirroring neo4j's driver.session() (used via `with self.session() as sess`)
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    if single_value is not None:
        run = MagicMock()
        run.single.return_value.value.return_value = single_value
        sess.run.return_value = run
    driver = MagicMock()
    driver.session.return_value = sess
    return driver, sess


def test_base_session():
    class _Concrete(Strategy):
        def generate(self, symbols):
            return 0
    driver, sess = _make_session()
    driver.session.return_value = sess
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        s = _Concrete()
        assert s.session() is sess


def test_ma_crossover_enough_bars():
    bars = [{"d": f"2024-01-{i:02d}", "c": 100.0 + i} for i in range(120)]
    driver, sess = _make_session(single_value=bars)
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        n = MovingAverageCrossover().generate(["BTC-USD"])
    assert n == 1
    # A signal was written
    assert sess.run.called


def test_ma_crossover_too_few_bars():
    driver, sess = _make_session(single_value=[{"d": "2024-01-01", "c": 100.0}])
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        n = MovingAverageCrossover().generate(["BTC-USD"])
    assert n == 0


def test_ma_crossover_empty_bars():
    driver, sess = _make_session(single_value=[])
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        n = MovingAverageCrossover().generate(["BTC-USD"])
    assert n == 0


def test_news_centrality_momentum():
    driver, sess = _make_session()
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        n = NewsCentralityMomentum().generate(["BTC-USD", "ETH-USD"])
    assert n == 2
    assert sess.run.called


def test_supply_chain_shock_diffusion():
    driver, sess = _make_session()
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        n = SupplyChainShockDiffusion().generate(["BTC-USD"])
    assert n == 1


def test_insider_cluster_drift_starts_with_A():
    # Capture the score written via run()
    driver, sess = _make_session()
    captured = []
    def fake_run(q, **kw):
        captured.append(kw.get("score"))
    sess.run.side_effect = fake_run
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        n = InsiderClusterDrift().generate(["AAPL", "MSFT"])
    assert n == 2
    # AAPL starts with 'A' -> +0.1 ; MSFT does not -> -0.02
    assert captured == [0.1, -0.02]


def test_strategy_name_attrs():
    assert MovingAverageCrossover.name == "MA_Crossover"
    assert NewsCentralityMomentum.name == "NewsCentralityMomentum"
    assert SupplyChainShockDiffusion.name == "SupplyChainShockDiffusion"
    assert InsiderClusterDrift.name == "InsiderClusterDrift"
