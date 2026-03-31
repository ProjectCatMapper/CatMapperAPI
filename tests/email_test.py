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
