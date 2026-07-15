"""Offline unit tests for coinbase.src.config (TradingConfig + safety validators).

NOTE: in this environment pydantic is not installed, so only the dataclass branch of
TradingConfig is exercised. The pydantic branch (dead here, per AGENTS.md) remains
uncovered by design — see coverage report caveat.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from coinbase.src.config import TradingConfig, LiveSafetyValidator, KillSwitch


class TestTradingConfig(unittest.TestCase):
    def test_defaults(self):
        c = TradingConfig()
        self.assertEqual(c.mode, "paper")
        self.assertTrue(c.kill_switch)
        self.assertEqual(c.max_notional_per_trade_usd, 100.0)
        self.assertEqual(c.min_confidence, 0.40)

    def test_from_env_reads_values(self):
        env = {
            "TRADER_MODE": "live",
            "COINBASE_DRY_RUN": "false",
            "KILL_SWITCH": "false",
            "LIVE_TRADING_ENABLED": "true",
            "MAX_NOTIONAL_PER_TRADE_USD": "250",
            "PAPER_MIN_CONFIDENCE": "0.7",
            "BRACKET_STOP_ATR_MULT": "3.0",
            "PRODUCTS": "BTC-USD,ETH-USD",
        }
        with patch.dict(os.environ, env, clear=False):
            c = TradingConfig.from_env()
        self.assertEqual(c.mode, "live")
        self.assertFalse(c.kill_switch)
        self.assertTrue(c.live_trading_enabled)
        self.assertEqual(c.max_notional_per_trade_usd, 250.0)
        self.assertEqual(c.min_confidence, 0.70)
        self.assertEqual(c.bracket_stop_atr_mult, 3.0)
        self.assertEqual(c.products, "BTC-USD,ETH-USD")

    def test_from_env_bad_mode_raises(self):
        with patch.dict(os.environ, {"TRADER_MODE": "nonsense"}, clear=False):
            with self.assertRaises(ValueError):
                TradingConfig.from_env()


class TestLiveSafetyValidator(unittest.TestCase):
    def _clean_cfg(self):
        c = TradingConfig()
        c.kill_switch = False
        c.live_trading_enabled = True
        c.mode = "live"
        c.coinbase_api_key = "k"
        c.coinbase_api_secret = "s"
        return c

    def test_clean_passes(self):
        issues = LiveSafetyValidator.check(self._clean_cfg())
        self.assertEqual(issues, [])

    def test_kill_switch_flagged(self):
        c = self._clean_cfg()
        c.kill_switch = True
        self.assertTrue(any("KILL_SWITCH" in i for i in LiveSafetyValidator.check(c)))

    def test_live_disabled_flagged(self):
        c = self._clean_cfg()
        c.live_trading_enabled = False
        self.assertTrue(any("LIVE_TRADING_ENABLED" in i for i in LiveSafetyValidator.check(c)))

    def test_dry_run_with_live_flagged(self):
        c = self._clean_cfg()
        c.dry_run = True
        self.assertTrue(any("dry-run" in i.lower() for i in LiveSafetyValidator.check(c)))

    def test_notional_bounds_flagged(self):
        c = self._clean_cfg()
        c.max_notional_per_trade_usd = 0
        self.assertTrue(any("MAX_NOTIONAL" in i for i in LiveSafetyValidator.check(c)))
        c2 = self._clean_cfg()
        c2.risk_per_trade_pct = 0.9
        self.assertTrue(any("RISK_PER_TRADE" in i for i in LiveSafetyValidator.check(c2)))

    def test_daily_loss_bounds_flagged(self):
        c = self._clean_cfg()
        c.max_daily_loss_pct = 0.9
        self.assertTrue(any("MAX_DAILY_LOSS" in i for i in LiveSafetyValidator.check(c)))

    def test_missing_credentials_flagged(self):
        c = self._clean_cfg()
        c.coinbase_api_key = ""
        c.coinbase_api_secret = ""
        issues = LiveSafetyValidator.check(c)
        self.assertTrue(any("COINBASE_API_KEY" in i for i in issues))

    def test_assert_safe_raises(self):
        c = self._clean_cfg()
        c.kill_switch = True
        with self.assertRaises(RuntimeError):
            LiveSafetyValidator.assert_safe(c)


class TestKillSwitch(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "ks")

    def test_env_active(self):
        with patch.dict(os.environ, {"KILL_SWITCH": "true"}, clear=False):
            with patch.object(KillSwitch, "KILL_PATH", __import__("pathlib").Path(self.path)):
                self.assertTrue(KillSwitch.is_active())

    def test_file_active(self):
        with patch.dict(os.environ, {}, clear=False):
            with patch.object(KillSwitch, "KILL_PATH", __import__("pathlib").Path(self.path)):
                self.assertFalse(KillSwitch.is_active())
                KillSwitch.engage()
                self.assertTrue(KillSwitch.is_active())
                KillSwitch.disengage()
                self.assertFalse(KillSwitch.is_active())


if __name__ == "__main__":
    unittest.main(verbosity=2)
