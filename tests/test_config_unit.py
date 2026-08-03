"""Offline unit tests for Coinbase runtime configuration and safety guards."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coinbase.src.config import KillSwitch, LiveSafetyValidator, TradingConfig


class TestTradingConfig(unittest.TestCase):
    def test_defaults(self):
        config = TradingConfig()
        self.assertEqual(config.mode, "paper")
        self.assertTrue(config.kill_switch)
        self.assertEqual(config.max_notional_per_trade_usd, 100.0)
        self.assertEqual(config.min_confidence, 0.40)

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
        with patch.dict(os.environ, env, clear=True):
            config = TradingConfig.from_env()
        self.assertEqual(config.mode, "live")
        self.assertFalse(config.kill_switch)
        self.assertTrue(config.live_trading_enabled)
        self.assertEqual(config.max_notional_per_trade_usd, 250.0)
        self.assertEqual(config.min_confidence, 0.70)
        self.assertEqual(config.bracket_stop_atr_mult, 3.0)
        self.assertEqual(config.products, "BTC-USD,ETH-USD")

    def test_legacy_kill_switch_alias_is_honored(self):
        with patch.dict(
            os.environ,
            {"TRADER_KILL_SWITCH": "true"},
            clear=True,
        ):
            config = TradingConfig.from_env()
            self.assertTrue(config.kill_switch)
            self.assertTrue(KillSwitch.is_active())

    def test_from_env_bad_mode_raises(self):
        with patch.dict(
            os.environ,
            {"TRADER_MODE": "nonsense"},
            clear=True,
        ):
            with self.assertRaises(ValueError):
                TradingConfig.from_env()


class TestLiveSafetyValidator(unittest.TestCase):
    def _clean_cfg(self):
        config = TradingConfig()
        config.kill_switch = False
        config.live_trading_enabled = True
        config.mode = "live"
        config.dry_run = False
        config.coinbase_api_key = "k"
        config.coinbase_api_secret = "s"
        config.coinbase_cli_path = "/bin/true"
        return config

    def test_clean_passes(self):
        issues = LiveSafetyValidator.check(self._clean_cfg())
        self.assertEqual(issues, [])

    def test_kill_switch_flagged(self):
        config = self._clean_cfg()
        config.kill_switch = True
        self.assertTrue(
            any("KILL_SWITCH" in issue for issue in LiveSafetyValidator.check(config))
        )

    def test_live_disabled_flagged(self):
        config = self._clean_cfg()
        config.live_trading_enabled = False
        self.assertTrue(
            any(
                "LIVE_TRADING_ENABLED" in issue
                for issue in LiveSafetyValidator.check(config)
            )
        )

    def test_dry_run_with_live_flagged(self):
        config = self._clean_cfg()
        config.dry_run = True
        self.assertTrue(
            any("dry-run" in issue.lower() for issue in LiveSafetyValidator.check(config))
        )

    def test_notional_bounds_flagged(self):
        config = self._clean_cfg()
        config.max_notional_per_trade_usd = 0
        self.assertTrue(
            any("MAX_NOTIONAL" in issue for issue in LiveSafetyValidator.check(config))
        )

        config = self._clean_cfg()
        config.risk_per_trade_pct = 0.9
        self.assertTrue(
            any(
                "RISK_PER_TRADE" in issue
                for issue in LiveSafetyValidator.check(config)
            )
        )

    def test_daily_loss_bounds_flagged(self):
        config = self._clean_cfg()
        config.max_daily_loss_pct = 0.9
        self.assertTrue(
            any(
                "MAX_DAILY_LOSS" in issue
                for issue in LiveSafetyValidator.check(config)
            )
        )

    def test_missing_credentials_flagged(self):
        config = self._clean_cfg()
        config.coinbase_api_key = ""
        config.coinbase_api_secret = ""
        issues = LiveSafetyValidator.check(config)
        self.assertTrue(any("COINBASE_API_KEY" in issue for issue in issues))

    def test_assert_safe_raises(self):
        config = self._clean_cfg()
        config.kill_switch = True
        with self.assertRaises(RuntimeError):
            LiveSafetyValidator.assert_safe(config)


class TestKillSwitch(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = Path(self.tmp) / "nested" / "ks"

    def test_env_active(self):
        with patch.dict(os.environ, {"KILL_SWITCH": "true"}, clear=True):
            with patch.object(KillSwitch, "KILL_PATH", self.path):
                self.assertTrue(KillSwitch.is_active())

    def test_file_active(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(KillSwitch, "KILL_PATH", self.path):
                self.assertFalse(KillSwitch.is_active())
                KillSwitch.engage()
                self.assertTrue(KillSwitch.is_active())
                KillSwitch.disengage()
                self.assertFalse(KillSwitch.is_active())

    def test_invalid_explicit_value_fails_closed(self):
        with patch.dict(os.environ, {"KILL_SWITCH": "maybe"}, clear=True):
            with patch.object(KillSwitch, "KILL_PATH", self.path):
                self.assertTrue(KillSwitch.is_active())


if __name__ == "__main__":
    unittest.main(verbosity=2)
