from notifications.email.service import EmailService, EmailAlert
from notifications.push.service import PushService, PushNotification
from notifications.webhook.service import WebhookService, WebhookEndpoint
from notifications.templates.service import TemplateService, NotificationTemplate


def test_email_service():
    svc = EmailService()
    alert = EmailAlert(to=["test@example.com"], subject="Test", body="Hello")
    assert svc.send(alert)


def test_push_service():
    svc = PushService()
    notification = PushNotification(title="Test", body="Hello", device_tokens=["token1"])
    assert svc.send(notification)


def test_webhook_service():
    svc = WebhookService()
    svc.add_endpoint(WebhookEndpoint(url="https://example.com/hook"))
    results = svc.dispatch("order.filled", {"order_id": "123"})
    assert len(results) == 1
    assert results[0] is True


def test_webhook_remove():
    svc = WebhookService()
    svc.add_endpoint(WebhookEndpoint(url="https://example.com/hook"))
    svc.remove_endpoint("https://example.com/hook")
    assert len(svc.endpoints) == 0


def test_template_service():
    svc = TemplateService()
    template = NotificationTemplate(name="test", subject_template="Order {id} filled", body_template="Body")
    svc.register(template)
    subject = svc.render_subject("test", {"id": "123"})
    assert subject == "Order 123 filled"


def test_template_not_found():
    svc = TemplateService()
    assert svc.render_subject("nonexistent", {}) == ""
