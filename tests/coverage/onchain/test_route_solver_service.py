import unittest
from decimal import Decimal

from onchain.dex.route_solver.service import RouteSolution, RouteSolver


class TestRouteSolver(unittest.TestCase):
    def test_solution(self):
        s = RouteSolution(path=["A", "B"], estimated_output=Decimal("5"), estimated_gas=Decimal("1"), score=0.5)
        self.assertEqual(s.score, 0.5)

    def test_solver_none(self):
        self.assertIsNone(RouteSolver().find_best_route("A", "B", Decimal("1")))


if __name__ == "__main__":
    unittest.main()
