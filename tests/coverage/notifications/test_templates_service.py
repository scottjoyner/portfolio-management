import unittest

from trading_system.notifications.templates.service import (
    NotificationTemplate,
    TemplateService,
)


class TestTemplateService(unittest.TestCase):
    def setUp(self):
        self.svc = TemplateService()
        self.svc.register(NotificationTemplate(
            name="trade",
            subject_template="Trade {action} {symbol}",
            body_template="Pnl {pnl}",
            html_template="<p>{pnl}</p>",
        ))

    def test_register_and_get(self):
        self.assertIsNotNone(self.svc.get("trade"))
        self.assertIsNone(self.svc.get("missing"))

    def test_render_subject(self):
        self.assertEqual(self.svc.render_subject("trade", {"action": "BUY", "symbol": "BTC"}),
                         "Trade BUY BTC")
        self.assertEqual(self.svc.render_subject("missing", {"action": "BUY"}), "")

    def test_render_body(self):
        self.assertEqual(self.svc.render_body("trade", {"pnl": "5.0"}), "Pnl 5.0")
        self.assertEqual(self.svc.render_body("missing", {"pnl": "5.0"}), "")


if __name__ == "__main__":
    unittest.main()
