from __future__ import annotations

from datetime import datetime, timezone
from unittest import TestCase

from onchain.contracts.upgradeability.service import UpgradeInfo, UpgradeabilityService


class TestUpgradeabilityService(TestCase):
    def test_register_and_check_known(self):
        svc = UpgradeabilityService()
        info = UpgradeInfo(is_upgradeable=True, implementation="0xIMPL", admin_address="0xADM",
                           last_upgrade=datetime.now(timezone.utc))
        svc.register("0xABC", "base", info)
        self.assertIs(svc.check("0xABC", "base"), info)

    def test_check_known_lowercase(self):
        svc = UpgradeabilityService()
        info = UpgradeInfo(is_upgradeable=True)
        svc.register("0xABCDEF", "base", info)
        self.assertIs(svc.check("0xabcdef", "base"), info)

    def test_check_unknown_default(self):
        svc = UpgradeabilityService()
        default = svc.check("0xMISSING", "base")
        self.assertIsInstance(default, UpgradeInfo)
        self.assertFalse(default.is_upgradeable)
