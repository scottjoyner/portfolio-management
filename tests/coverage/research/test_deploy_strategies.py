import builtins
import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import trading_system.deployment.deploy_strategies as ds
from trading_system.deployment.deploy_strategies import deploy_all_strategies, main


real_print = builtins.print


def _failing_print(*args, **kwargs):
    text = str(args[0]) if args else ""
    if "DEPLOYED" in text and "trend-macdsignalcrossover" in text:
        raise RuntimeError("boom")
    real_print(*args, **kwargs)


class TestDeployStrategies(unittest.TestCase):
    def test_deploy_all_success(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            results = deploy_all_strategies(profile="default", service_prefix="trading")
        self.assertEqual(results["trend-macdsignalcrossover"]["status"], "DEPLOYED")
        self.assertIn("ALL STRATEGIES DEPLOYED", buf.getvalue())

    def test_deploy_with_failure(self):
        buf = io.StringIO()
        with patch("builtins.print", side_effect=_failing_print), redirect_stdout(buf):
            results = deploy_all_strategies(profile="default", service_prefix="trading")
        self.assertEqual(results["trend-macdsignalcrossover"]["status"], "FAILED")
        out = buf.getvalue()
        self.assertIn("FAILED STRATEGIES", out)

    def test_main(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            main()
        self.assertIn("DEPLOYMENT COMPLETE", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
