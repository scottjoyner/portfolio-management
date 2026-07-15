import unittest
from decimal import Decimal

from onchain.dex.position_manager.actions import PositionAction
from onchain.dex.position_manager.collectors import collectable_fee_value
from onchain.dex.position_manager.manager import PositionManager
from onchain.dex.position_manager.validators import validate_action_limits


class TestPositionManager(unittest.TestCase):
    def test_action_dataclass(self):
        a = PositionAction(action_type="rebalance", position_id="p1", amount_usd=Decimal("10"), reason="x")
        self.assertEqual(a.position_id, "p1")

    def test_collectable(self):
        self.assertEqual(collectable_fee_value(Decimal("2"), Decimal("3"), Decimal("10")), Decimal("23"))

    def test_validator_ok(self):
        validate_action_limits(Decimal("5"), Decimal("10"))

    def test_validator_exceeds(self):
        with self.assertRaises(ValueError):
            validate_action_limits(Decimal("20"), Decimal("10"))

    def test_manager_propose_ok(self):
        m = PositionManager(Decimal("100"))
        a = m.propose(PositionAction(action_type="x", position_id="p", amount_usd=Decimal("10"), reason="r"))
        self.assertEqual(a.amount_usd, Decimal("10"))

    def test_manager_propose_exceeds(self):
        m = PositionManager(Decimal("5"))
        with self.assertRaises(ValueError):
            m.propose(PositionAction(action_type="x", position_id="p", amount_usd=Decimal("10"), reason="r"))


if __name__ == "__main__":
    unittest.main()
