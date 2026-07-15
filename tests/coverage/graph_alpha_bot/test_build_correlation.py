import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch

import app.data.build_correlation as m
from app.data.build_correlation import get_prices, main


def _sess(closes):
    sess = MagicMock()
    single = MagicMock()
    single.value.return_value = closes
    sess.run.return_value.single.return_value = single
    return sess


def test_get_prices_empty():
    s = _sess(None)
    out = get_prices(s, "BTC", 90)
    assert isinstance(out, pd.Series)
    assert out.empty


def test_get_prices_tail():
    closes = list(range(200))
    s = _sess(closes)
    out = get_prices(s, "BTC", 90)
    assert len(out) == 91


def test_main_writes_edges():
    drv = MagicMock()
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    drv.session.return_value = sess
    # get_prices query returns closes; edges query returns nothing
    single = MagicMock()
    single.value.return_value = list(range(100, 200))
    sess.run.side_effect = [MagicMock(single=single), MagicMock(single=single), MagicMock()]
    with patch("app.data.build_correlation.GraphDatabase.driver", return_value=drv):
        main()


def test_main_insufficient_data(monkeypatch):
    drv = MagicMock()
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    drv.session.return_value = sess
    single = MagicMock()
    single.value.return_value = [1.0, 2.0]  # too few -> warning/continue
    sess.run.return_value.single.return_value = single
    with patch("app.data.build_correlation.GraphDatabase.driver", return_value=drv):
        main()


def test_main_insufficient_series(monkeypatch):
    drv = MagicMock()
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    drv.session.return_value = sess
    # one symbol with enough data, second with too few -> only 1 series -> return
    single_full = MagicMock(); single_full.value.return_value = list(range(100, 200))
    single_short = MagicMock(); single_short.value.return_value = [1.0]
    calls = {"n": 0}
    def run_side(*a, **k):
        calls["n"] += 1
        if calls["n"] % 2 == 1:
            return MagicMock(single=single_full)
        return MagicMock(single=single_short)
    sess.run.side_effect = run_side
    with patch("app.data.build_correlation.GraphDatabase.driver", return_value=drv):
        main()
