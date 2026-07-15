"""Coverage for graph_alpha_bot.app.strategies.insider_cluster_drift."""
from unittest.mock import MagicMock, patch

import app.strategies.base as inner_base
from graph_alpha_bot.app.strategies.insider_cluster_drift import InsiderClusterDrift


def _make_driver():
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    driver = MagicMock()
    driver.session.return_value = sess
    return driver, sess


def test_name():
    assert InsiderClusterDrift.name == "InsiderClusterDrift"


def test_generate_scores_both_branches():
    driver, sess = _make_driver()
    captured = []
    sess.run.side_effect = lambda q, **kw: captured.append(kw.get("score"))
    with patch.object(inner_base.GraphDatabase, "driver", return_value=driver):
        n = InsiderClusterDrift().generate(["AAPL", "MSFT"])
    assert n == 2
    # 'A'-prefixed -> +0.1 ; otherwise -> -0.02
    assert captured == [0.1, -0.02]


def test_generate_empty_symbols():
    driver, sess = _make_driver()
    with patch.object(inner_base.GraphDatabase, "driver", return_value=driver):
        n = InsiderClusterDrift().generate([])
    assert n == 0
    assert not sess.run.called
