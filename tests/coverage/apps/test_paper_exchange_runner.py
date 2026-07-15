import sys
from unittest.mock import patch

from trading_system.apps.paper_exchange import runner


def test_main(capsys):
    with patch.object(sys, "argv", ["runner", "--config", "myconfig.yaml"]):
        runner.main()
    captured = capsys.readouterr()
    assert "paper_exchange_started" in captured.out
    assert "myconfig.yaml" in captured.out
