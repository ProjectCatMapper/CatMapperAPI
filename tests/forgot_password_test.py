import CMroutes.user_routes as user_routes


def test_forgot_password_request_accepts_email_and_includes_username_in_email(client, monkeypatch):
    sent = {}
    user_routes.PASSWORD_CHANGE_REQUESTS.clear()

    monkeypatch.setattr(
        user_routes,
        "_load_user_by_identifier",
        lambda identifier: {
            "userid": "100",
            "username": "ada",
            "email": "ada@example.org",
        },
    )
    monkeypatch.setattr(user_routes, "password_hash", lambda value: f"hashed::{value}")

    def fake_send_email(**kwargs):
        sent.update(kwargs)
        return "Email sent successfully"

    monkeypatch.setattr(user_routes, "sendEmail", fake_send_email)

    response = client.post(
        "/forgot-password/request",
        json={"email": "ada@example.org", "newPassword": "new-password"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload.get("requestId", "").startswith("forgot_")
    assert payload.get("maskedEmail")
    assert "Username: ada" in sent.get("body", "")


def test_forgot_password_request_unknown_identifier_returns_generic_success(client, monkeypatch):
    monkeypatch.setattr(
        user_routes,
        "_load_user_by_identifier",
        lambda identifier: (_ for _ in ()).throw(Exception("User not found")),
    )

    response = client.post(
        "/forgot-password/request",
        json={"email": "missing@example.org", "newPassword": "new-password"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert "If an account exists" in payload.get("message", "")
    assert "requestId" not in payload


def test_forgot_password_confirm_accepts_email_identifier(client, monkeypatch):
    user_routes.PASSWORD_CHANGE_REQUESTS.clear()
    user_routes.PASSWORD_CHANGE_REQUESTS["forgot_abc123"] = {
        "userid": "100",
        "password_hash": "hashed::new-password",
        "verification_code": "123456",
        "expires_at": user_routes.datetime.utcnow() + user_routes.timedelta(minutes=15),
    }

    monkeypatch.setattr(
        user_routes,
        "_load_user_by_identifier",
        lambda identifier: {"userid": "100", "username": "ada", "email": "ada@example.org"},
    )
    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(
        user_routes,
        "getQuery",
        lambda query, driver=None, params=None, **kwargs: [{"passwordLastChangedAt": "2026-02-16T00:00:00Z"}],
    )

    response = client.post(
        "/forgot-password/confirm",
        json={
            "email": "ada@example.org",
            "requestId": "forgot_abc123",
            "verificationCode": "123456",
        },
    )

    assert response.status_code == 200
    assert response.get_json().get("passwordLastChangedAt")
