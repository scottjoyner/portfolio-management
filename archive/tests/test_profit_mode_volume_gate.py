import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coinbase.src.run_trader_v2 import TraderConfig


class TestProfitModeVolumeGate(unittest.TestCase):
    def test_volume_generator_disabled_by_default_for_profit_mode(self):
        with patch.dict(os.environ, {}, clear=True):
            config = TraderConfig.from_env()
        self.assertFalse(config.enable_volume_generator)

    def test_volume_generator_can_be_enabled_explicitly(self):
        with patch.dict(os.environ, {"TRADER_ENABLE_VOLUME_GENERATOR": "true"}, clear=True):
            config = TraderConfig.from_env()
        self.assertTrue(config.enable_volume_generator)


if __name__ == "__main__":
    unittest.main()
