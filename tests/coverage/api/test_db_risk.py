import unittest

from trading_system.api.databases import risk as risk_mod


class TestRisk(unittest.IsolatedAsyncioTestCase):
    async def test_get_drawdowns(self):
        self.assertIsInstance(await risk_mod.get_drawdowns(), list)

    async def test_get_risk_metrics(self):
        self.assertIsInstance(await risk_mod.get_risk_metrics(), dict)

    async def test_get_position_limits(self):
        self.assertIsInstance(await risk_mod.get_position_limits(), list)

    async def test_get_compliance_violations(self):
        self.assertIsInstance(await risk_mod.get_compliance_violations(), list)


if __name__ == "__main__":
    unittest.main()
