"""Coverage for graph_alpha_bot.app.strategies.supply_chain_shock_diffusion."""
from unittest.mock import MagicMock, patch

import app.strategies.base as inner_base
from graph_alpha_bot.app.strategies.supply_chain_shock_diffusion import (
    SupplyChainShockDiffusion,
)


def _make_driver():
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    driver = MagicMock()
    driver.session.return_value = sess
    return driver, sess


def test_name():
    assert SupplyChainShockDiffusion.name == "SupplyChainShockDiffusion"


def test_generate_writes_neutral_scores():
    driver, sess = _make_driver()
    with patch.object(inner_base.GraphDatabase, "driver", return_value=driver):
        n = SupplyChainShockDiffusion().generate(["BTC-USD", "ETH-USD", "SOL-USD"])
    assert n == 3
    assert sess.run.call_count == 3
    scores = [c.kwargs.get("score") for c in sess.run.call_args_list]
    assert scores == [0.0, 0.0, 0.0]


def test_generate_empty():
    driver, sess = _make_driver()
    with patch.object(inner_base.GraphDatabase, "driver", return_value=driver):
        n = SupplyChainShockDiffusion().generate([])
    assert n == 0
    assert not sess.run.called
