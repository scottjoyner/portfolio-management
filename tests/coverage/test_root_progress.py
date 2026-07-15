from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

from trading_system import progress_report


class TestProgressReport(TestCase):
    def test_print_progress_report(self):
        with patch("builtins.print"):
            self.assertIsNone(progress_report.print_progress_report())
