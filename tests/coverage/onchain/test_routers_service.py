import unittest
from decimal import Decimal

from onchain.dex.routers.service import RouteStepProtocol, RouteStep, Route, Router


class TestRouters(unittest.TestCase):
    def test_protocol_enum(self):
        self.assertEqual(RouteStepProtocol.UNISWAP_V2.value, "uniswap_v2")

    def test_route_step(self):
        s = RouteStep(protocol=RouteStepProtocol.UNISWAP_V3, pool="0x1", token_in="ETH", token_out="USDC")
        self.assertEqual(s.expected_amount_out, Decimal("0"))

    def test_route(self):
        r = Route(steps=[RouteStep(protocol=RouteStepProtocol.UNISWAP_V2, pool="0x1", token_in="A", token_out="B")],
                  total_expected_out=Decimal("5"))
        self.assertEqual(r.total_expected_out, Decimal("5"))

    def test_router_build(self):
        r = Router().build_route("A", "B", Decimal("1"), ["0x1"])
        self.assertIsInstance(r, Route)
        self.assertEqual(r.steps, [])


if __name__ == "__main__":
    unittest.main()
