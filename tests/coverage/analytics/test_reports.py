import unittest

from analytics.reports.service import Report, ReportService


class TestReports(unittest.TestCase):
    def test_report_add_section_and_to_dict(self):
        r = Report(title="Q1")
        r.add_section("summary", {"pnl": 1})
        self.assertEqual(r.sections["summary"], {"pnl": 1})
        d = r.to_dict()
        self.assertEqual(d["title"], "Q1")
        self.assertIn("summary", d["sections"])

    def test_report_service_generate(self):
        svc = ReportService()
        r = svc.generate("r1", "Report 1")
        self.assertIsInstance(r, Report)
        self.assertIn("r1", svc.reports)
        self.assertEqual(svc.reports["r1"].title, "Report 1")

    def test_report_defaults(self):
        r = Report(title="x")
        self.assertEqual(r.sections, {})


if __name__ == "__main__":
    unittest.main()
