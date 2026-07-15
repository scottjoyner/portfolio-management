"""Coverage for graph_alpha_bot.app.strategies.base.

The abstract ``Strategy`` base creates a neo4j driver in ``__init__``. We mock
``GraphDatabase.driver`` so no real connection is attempted.
"""
from unittest.mock import MagicMock, patch

import graph_alpha_bot.app.strategies.base as base_mod
from graph_alpha_bot.app.strategies.base import Strategy


class _Concrete(Strategy):
    name = "Concrete"

    def generate(self, symbols):
        return len(symbols)


def _patched_driver():
    driver = MagicMock()
    sess = MagicMock()
    driver.session.return_value = sess
    return driver, sess


def test_init_creates_driver():
    driver, _ = _patched_driver()
    with patch.object(base_mod.GraphDatabase, "driver", return_value=driver) as p:
        s = _Concrete()
    assert s.driver is driver
    p.assert_called_once()


def test_session_returns_driver_session():
    driver, sess = _patched_driver()
    with patch.object(base_mod.GraphDatabase, "driver", return_value=driver):
        s = _Concrete()
    assert s.session() is sess


def test_close_closes_driver():
    driver, _ = _patched_driver()
    with patch.object(base_mod.GraphDatabase, "driver", return_value=driver):
        s = _Concrete()
    s.close()
    driver.close.assert_called_once()


def test_generate_concrete_override():
    driver, _ = _patched_driver()
    with patch.object(base_mod.GraphDatabase, "driver", return_value=driver):
        s = _Concrete()
    assert s.generate(["A", "B"]) == 2


def test_name_default_attribute():
    assert Strategy.name == "Base"
