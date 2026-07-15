import unittest

from trading_system.notifications.email.service import EmailAlert, EmailService


class TestEmailService(unittest.TestCase):
    def test_send(self):
        svc = EmailService(smtp_host="smtp.example", smtp_port=587, from_address="a@b.c", use_tls=True)
        alert = EmailAlert(to=["x@y.z"], subject="subj", body="body", html="<p>body</p>")
        self.assertTrue(svc.send(alert))
        # Defaults
        svc2 = EmailService()
        alert2 = EmailAlert(to=["a@b"], subject="s", body="b")
        self.assertTrue(svc2.send(alert2))


if __name__ == "__main__":
    unittest.main()
