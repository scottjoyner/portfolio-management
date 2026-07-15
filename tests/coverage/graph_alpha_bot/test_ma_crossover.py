"""Coverage for graph_alpha_bot.app.strategies.ma_crossover."""
from unittest.mock import MagicMock, patch

import app.strategies.base as inner_base
from graph_alpha_bot.app.strategies.ma_crossover import MovingAverageCrossover


def _make_driver(single_value):
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    run = MagicMock()
    run.single.return_value.value.return_value = single_value
    sess.run.return_value = run
    driver = MagicMock()
    driver.session.return_value = sess
    return driver, sess


def _run(single_value, symbols=None):
    driver, sess = _make_driver(single_value)
    with patch.object(inner_base.GraphDatabase, "driver", return_value=driver):
        n = MovingAverageCrossover().generate(symbols or ["BTC-USD"])
    return n, sess


def test_name():
    assert MovingAverageCrossover.name == "MA_Crossover"


def test_enough_bars_uptrend_sig_positive():
    bars = [{"d": f"2024-{i:04d}", "c": 100.0 + i} for i in range(150)]
    n, sess = _run(bars)
    assert n == 1
    assert sess.run.called
    # capture the score written on the 2nd run call (the _write_signal call)
    scores = [c.kwargs.get("score") for c in sess.run.call_args_list if "score" in c.kwargs]
    assert scores and scores[0] > 0  # uptrend -> sig=+1


def test_enough_bars_downtrend_sig_negative():
    bars = [{"d": f"2024-{i:04d}", "c": 300.0 - i} for i in range(150)]
    n, sess = _run(bars)
    assert n == 1
    scores = [c.kwargs.get("score") for c in sess.run.call_args_list if "score" in c.kwargs]
    assert scores and scores[0] < 0  # downtrend -> sig=-1


def test_zero_prices_entry_equals_stop():
    bars = [{"d": f"2024-{i:04d}", "c": 0.0} for i in range(150)]
    n, sess = _run(bars)
    assert n == 1
    scores = [c.kwargs.get("score") for c in sess.run.call_args_list if "score" in c.kwargs]
    # entry == stop == 0 -> rr = 0.0 -> score = 0.0
    assert scores == [0.0]


def test_too_few_bars_skipped():
    bars = [{"d": f"2024-{i:04d}", "c": 100.0 + i} for i in range(50)]
    n, _ = _run(bars)
    assert n == 0


def test_empty_bars_skipped():
    n, _ = _run([])
    assert n == 0
