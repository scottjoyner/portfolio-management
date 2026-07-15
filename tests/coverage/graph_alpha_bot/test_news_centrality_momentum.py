"""Coverage for graph_alpha_bot.app.strategies.news_centrality_momentum."""
from unittest.mock import MagicMock, patch

import app.strategies.base as inner_base
from graph_alpha_bot.app.strategies.news_centrality_momentum import NewsCentralityMomentum


def _make_driver():
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    driver = MagicMock()
    driver.session.return_value = sess
    return driver, sess


def test_name():
    assert NewsCentralityMomentum.name == "NewsCentralityMomentum"


def test_generate_writes_signal_per_symbol():
    driver, sess = _make_driver()
    with patch.object(inner_base.GraphDatabase, "driver", return_value=driver):
        n = NewsCentralityMomentum().generate(["BTC-USD", "ETH-USD"])
    assert n == 2
    assert sess.run.call_count == 2
    scores = [c.kwargs.get("score") for c in sess.run.call_args_list]
    assert scores == [0.05, 0.05]


def test_generate_empty():
    driver, sess = _make_driver()
    with patch.object(inner_base.GraphDatabase, "driver", return_value=driver):
        n = NewsCentralityMomentum().generate([])
    assert n == 0
    assert not sess.run.called
