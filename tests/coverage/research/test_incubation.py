import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from research.incubation import IncubationService  # noqa: E402


class _Q:
    def __init__(self, first=None, all=None):
        self._first = first
        self._all = all if all is not None else []

    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def first(self):
        return self._first

    def all(self):
        return self._all


class _Fill:
    def __init__(self, fill_id, size, price, slippage_bps):
        self.fill_id = fill_id
        self.size = size
        self.price = price
        self.slippage_bps = slippage_bps


class _EngineWithPositions:
    def __init__(self):
        self.fills = [
            _Fill("f1", 2, 100.0, 5.0),
            _Fill(None, 3, 50.0, 10.0),
        ]
        self.positions = {"BTC-USD": MagicMock(realized_pnl=Decimal("1.5"))}


class _EngineNoPositions:
    def __init__(self):
        self.fills = [_Fill("f1", 1, 200.0, 2.0)]


class TestIncubationService(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(name="db")
        self.store = {}
        self.db.query.side_effect = lambda model: _Q(
            first=self.store.get(model.__name__ + "_first"),
            all=self.store.get(model.__name__ + "_all", []),
        )
        self.svc = IncubationService(self.db)
        self.svc.repo = MagicMock(name="repo")

    # track_run ----------------------------------------------------------
    def test_track_run_with_positions(self):
        run = MagicMock(name="run", strategy_id="s1",
                        started_at=datetime(2024, 1, 1, tzinfo=timezone.utc), queued_at=None)
        self.svc.repo.get_strategy_run.return_value = run
        bt = {
            "backtest_sharpe": 1.2,
            "backtest_max_drawdown": 0.2,
            "backtest_total_return": 0.3,
            "expected_slippage_bps": 3.0,
            "expected_latency_ms": 10.0,
            "expected_fill_rate": 0.95,
        }
        report = self.svc.track_run("t1", _EngineWithPositions(), bt)
        self.assertEqual(report.strategy_id, "s1")
        self.assertEqual(report.total_orders, 2)
        self.assertEqual(report.total_fills, 1)
        self.assertEqual(report.realized_pnl, Decimal("1.5"))
        self.assertEqual(report.backtest_sharpe, 1.2)
        self.assertAlmostEqual(report.fill_quality_ratio, (1 / 2) / 0.95)

    def test_track_run_no_positions(self):
        run = MagicMock(name="run", strategy_id="s1", started_at=None,
                        queued_at=datetime(2024, 1, 2, tzinfo=timezone.utc))
        self.svc.repo.get_strategy_run.return_value = run
        report = self.svc.track_run("t1", _EngineNoPositions(), None)
        self.assertEqual(report.realized_pnl, Decimal("0"))
        self.assertEqual(report.started_at, run.queued_at)
        self.assertEqual(report.backtest_sharpe, None)

    def test_track_run_no_run(self):
        self.svc.repo.get_strategy_run.return_value = None
        with self.assertRaises(ValueError):
            self.svc.track_run("t1", _EngineNoPositions())

    # approve_for_live ---------------------------------------------------
    def test_approve_no_run(self):
        self.svc.repo.get_strategy_run.return_value = None
        ok, msg = self.svc.approve_for_live("t1")
        self.assertFalse(ok)
        self.assertIn("not found", msg)

    def test_approve_wrong_mode(self):
        run = MagicMock(name="run", mode="live")
        self.svc.repo.get_strategy_run.return_value = run
        ok, msg = self.svc.approve_for_live("t1")
        self.assertFalse(ok)
        self.assertIn("mode", msg)

    def test_approve_existing_cfg(self):
        run = MagicMock(name="run", mode="paper", strategy_id="s1")
        self.svc.repo.get_strategy_run.return_value = run
        cfg = MagicMock(name="cfg")
        self.store["StrategyConfig_first"] = cfg
        ok, msg = self.svc.approve_for_live("t1")
        self.assertTrue(ok)
        self.assertEqual(cfg.certification_status, "incubated")
        self.svc.repo.update_strategy_run.assert_called_once()

    def test_approve_new_cfg(self):
        run = MagicMock(name="run", mode="paper", strategy_id="s1")
        self.svc.repo.get_strategy_run.return_value = run
        self.store["StrategyConfig_first"] = None
        ok, msg = self.svc.approve_for_live("t1")
        self.assertTrue(ok)
        self.db.add.assert_called()

    # shadow_payload -----------------------------------------------------
    def test_shadow_payload_no_limit(self):
        md = {"price": 100, "spread_bps": 8, "volume_24h": 1, "timestamp": "t"}
        intent = {"strategy_id": "s1", "product_id": "BTC-USD", "side": "buy",
                  "size": 10}
        out = self.svc.shadow_payload(md, intent)
        self.assertIsNone(out["limit_price"])
        self.assertTrue(out["would_execute"])

    def test_shadow_payload_with_limit(self):
        md = {"price": 100, "spread_bps": 8, "volume_24h": 1, "timestamp": "t"}
        intent = {"strategy_id": "s1", "product_id": "BTC-USD", "side": "buy",
                  "size": 10, "limit_price": 95.0}
        out = self.svc.shadow_payload(md, intent)
        self.assertEqual(out["limit_price"], 95.0)
        self.assertIn("shadow_id", out)


if __name__ == "__main__":
    unittest.main()
