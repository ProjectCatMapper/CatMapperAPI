import json

from CM import brevo as brevo_module
from CM.email import sendEmail


class FakeMail:
    def __init__(self):
        self.sent_messages = []

    def send(self, msg):
        self.sent_messages.append(msg)


def test_send_email_uses_custom_trace_header_not_message_id():
    fake_mail = FakeMail()

    result = sendEmail(
        mail=fake_mail,
        subject="Password Reset",
        recipients=["ada@example.org"],
        body="Hello from CatMapper",
        sender="no-reply@catmapper.org",
    )

    assert result == "Email sent successfully"
    assert len(fake_mail.sent_messages) == 1

    message = fake_mail.sent_messages[0]
    headers = message.extra_headers or {}

    assert "Message-ID" not in headers
    assert headers["X-CatMapper-Message-ID"].startswith("<")
    assert headers["X-CatMapper-Message-ID"].endswith("@catmapper.org>")
    assert headers["X-CatMapper-Trace-ID"]
    assert headers["X-CatMapper-Sent-At"]


def test_send_email_uses_brevo_when_key_present(monkeypatch):
    fake_mail = FakeMail()
    captured = {}

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def read(self):
            return self.payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(req, timeout=30):
        captured["headers"] = dict(req.header_items())
        captured["payload"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse(b'{"messageId":"<brevo-message@example.org>"}')

    monkeypatch.setenv("BREVO_API_KEY", "test-brevo-key")
    monkeypatch.setattr(brevo_module.request, "urlopen", fake_urlopen)

    result = sendEmail(
        mail=fake_mail,
        subject="Password Reset",
        recipients=["ada@example.org"],
        body="Hello from CatMapper",
        sender="admin@catmapper.org",
        return_metadata=True,
    )

    assert result["status"] == "Email sent successfully"
    assert result["provider"] == "brevo"
    assert result["provider_message_id"] == "<brevo-message@example.org>"
    assert fake_mail.sent_messages == []
    assert captured["headers"]["Api-key"] == "test-brevo-key"
    assert captured["payload"]["sender"]["email"] == "admin@catmapper.org"
    assert captured["payload"]["subject"] == "Password Reset"
    assert captured["payload"]["to"] == [{"email": "ada@example.org"}]
