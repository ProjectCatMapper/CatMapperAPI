import CMroutes.user_routes as user_routes


def test_forgot_password_request_accepts_email_and_includes_username_in_email(client, monkeypatch):
    sent = {}
    stored_entries = {"pendingPasswordResetRequests": []}

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
    monkeypatch.setattr(
        user_routes,
        "_get_user_entries",
        lambda userid, field_name: list(stored_entries.get(field_name, [])),
    )
    monkeypatch.setattr(
        user_routes,
        "_set_user_entries",
        lambda userid, field_name, entries: stored_entries.__setitem__(field_name, list(entries)),
    )

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
    assert len(stored_entries["pendingPasswordResetRequests"]) == 1
    assert stored_entries["pendingPasswordResetRequests"][0]["request_id"] == payload["requestId"]


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
    stored_entries = {
        "pendingPasswordResetRequests": [
            {
                "request_id": "forgot_abc123",
                "password_hash": "hashed::new-password",
                "verification_code": "123456",
                "expires_at": (user_routes.datetime.utcnow() + user_routes.timedelta(minutes=15)).isoformat() + "Z",
            }
        ]
    }

    monkeypatch.setattr(
        user_routes,
        "_load_user_by_identifier",
        lambda identifier: {"userid": "100", "username": "ada", "email": "ada@example.org"},
    )
    monkeypatch.setattr(
        user_routes,
        "_get_user_entries",
        lambda userid, field_name: list(stored_entries.get(field_name, [])),
    )
    monkeypatch.setattr(
        user_routes,
        "_set_user_entries",
        lambda userid, field_name, entries: stored_entries.__setitem__(field_name, list(entries)),
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
    assert stored_entries["pendingPasswordResetRequests"] == []


def test_forgot_password_confirm_survives_in_memory_request_store_reset(client, monkeypatch):
    stored_entries = {
        "pendingPasswordResetRequests": [
            {
                "request_id": "forgot_restartsafe",
                "password_hash": "hashed::new-password",
                "verification_code": "654321",
                "expires_at": (user_routes.datetime.utcnow() + user_routes.timedelta(minutes=15)).isoformat() + "Z",
            }
        ]
    }
    user_routes.PASSWORD_CHANGE_REQUESTS.clear()

    monkeypatch.setattr(
        user_routes,
        "_load_user_by_identifier",
        lambda identifier: {"userid": "100", "username": "ada", "email": "ada@example.org"},
    )
    monkeypatch.setattr(
        user_routes,
        "_get_user_entries",
        lambda userid, field_name: list(stored_entries.get(field_name, [])),
    )
    monkeypatch.setattr(
        user_routes,
        "_set_user_entries",
        lambda userid, field_name, entries: stored_entries.__setitem__(field_name, list(entries)),
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
            "requestId": "forgot_restartsafe",
            "verificationCode": "654321",
        },
    )

    assert response.status_code == 200
    assert response.get_json().get("passwordLastChangedAt")
