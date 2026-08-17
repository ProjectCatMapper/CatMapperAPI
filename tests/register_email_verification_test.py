import json

import CMroutes.user_routes as user_routes


def test_newuser_creates_email_unverified_and_sends_verification(client, monkeypatch):
    sent = {}
    captured = {}

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "password_hash", lambda password: f"hashed::{password}")
    monkeypatch.setattr(user_routes, "secrets", type("S", (), {"randbelow": staticmethod(lambda n: 123456)})())
    monkeypatch.setattr(user_routes.uuid, "uuid4", lambda: type("U", (), {"hex": "abcdef1234567890"})())

    def fake_get_query(query, driver=None, params=None, **kwargs):
        if "RETURN u.userid as userid, u.username as username" in query:
            return []
        if 'u.access = "email_unverified"' in query:
            captured.update(params)
            return [{"userid": "101"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(user_routes, "getQuery", fake_get_query)
    monkeypatch.setattr(user_routes, "sendEmail", lambda **kwargs: sent.update(kwargs) or "Email sent successfully")

    response = client.post(
        "/newuser",
        json={
            "database": "sociomap",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "email": "ada@example.org",
            "username": "ada",
            "password": "secret1",
            "intendedUse": "Research",
        },
        headers={"Origin": "https://catmapper.org"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "email_unverified"
    assert payload["requestId"] == "register_abcdef123456"
    assert captured["verificationRequests"]
    verification_entry = json.loads(captured["verificationRequests"][0])
    assert verification_entry["verification_code"] == "223456"
    assert sent["recipients"] == ["ada@example.org"]
    assert "223456" in sent["body"]
    assert "https://catmapper.org/sociomap/register/verify" in sent["body"]


def test_newuser_verification_link_uses_dev_frontend_for_dev_api_host(client, monkeypatch):
    sent = {}

    monkeypatch.delenv("CATMAPPER_FRONTEND_URL", raising=False)
    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "password_hash", lambda password: f"hashed::{password}")
    monkeypatch.setattr(user_routes, "sendEmail", lambda **kwargs: sent.update(kwargs) or "Email sent successfully")

    def fake_get_query(query, driver=None, params=None, **kwargs):
        if "RETURN u.userid as userid, u.username as username" in query:
            return []
        if 'u.access = "email_unverified"' in query:
            return [{"userid": "101"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(user_routes, "getQuery", fake_get_query)

    response = client.post(
        "/newuser",
        base_url="https://dev-api.catmapper.org",
        json={
            "database": "sociomap",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "email": "ada@example.org",
            "username": "ada",
            "password": "secret1",
            "intendedUse": "Research",
        },
    )

    assert response.status_code == 200
    assert "https://dev.catmapper.org/sociomap/register/verify" in sent["body"]


def test_newuser_verification_link_prefers_configured_frontend_url(client, monkeypatch):
    sent = {}

    monkeypatch.setenv("CATMAPPER_FRONTEND_URL", "https://preview.catmapper.org/")
    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "password_hash", lambda password: f"hashed::{password}")
    monkeypatch.setattr(user_routes, "sendEmail", lambda **kwargs: sent.update(kwargs) or "Email sent successfully")

    def fake_get_query(query, driver=None, params=None, **kwargs):
        if "RETURN u.userid as userid, u.username as username" in query:
            return []
        if 'u.access = "email_unverified"' in query:
            return [{"userid": "101"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(user_routes, "getQuery", fake_get_query)

    response = client.post(
        "/newuser",
        headers={"Origin": "https://catmapper.org"},
        json={
            "database": "sociomap",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "email": "ada@example.org",
            "username": "ada",
            "password": "secret1",
            "intendedUse": "Research",
        },
    )

    assert response.status_code == 200
    assert "https://preview.catmapper.org/sociomap/register/verify" in sent["body"]


def test_newuser_resends_for_email_unverified_user(client, monkeypatch):
    captured = {}

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "password_hash", lambda password: f"hashed::{password}")
    monkeypatch.setattr(user_routes, "sendEmail", lambda **kwargs: "Email sent successfully")

    def fake_get_query(query, driver=None, params=None, **kwargs):
        if "RETURN u.userid as userid, u.username as username" in query:
            return [{
                "userid": "101",
                "username": "ada",
                "email": "ada@example.org",
                "access": "email_unverified",
            }]
        if 'resent registration email verification' in query:
            captured.update(params)
            return [{"userid": "101"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(user_routes, "getQuery", fake_get_query)

    response = client.post(
        "/newuser",
        json={
            "database": "sociomap",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "email": "ada@example.org",
            "username": "ada",
            "password": "secret1",
            "intendedUse": "Research",
        },
    )

    assert response.status_code == 200
    assert captured["userid"] == "101"
    assert captured["verificationRequests"]


def test_newuser_existing_enabled_user_still_fails(client, monkeypatch):
    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, params=None, **kwargs):
        if "RETURN u.userid as userid, u.username as username" in query:
            return [{
                "userid": "101",
                "username": "ada",
                "email": "ada@example.org",
                "access": "enabled",
            }]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(user_routes, "getQuery", fake_get_query)

    response = client.post(
        "/newuser",
        json={
            "database": "sociomap",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "email": "ada@example.org",
            "username": "ada",
            "password": "secret1",
            "intendedUse": "Research",
        },
    )

    assert response.status_code == 400
    assert "Username already exists" in response.get_json()["error"]


def test_newuser_existing_enabled_email_still_fails(client, monkeypatch):
    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, params=None, **kwargs):
        if "RETURN u.userid as userid, u.username as username" in query:
            return [{
                "userid": "101",
                "username": "other",
                "email": "ada@example.org",
                "access": "enabled",
            }]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(user_routes, "getQuery", fake_get_query)

    response = client.post(
        "/newuser",
        json={
            "database": "sociomap",
            "firstName": "Ada",
            "lastName": "Lovelace",
            "email": "ada@example.org",
            "username": "ada",
            "password": "secret1",
            "intendedUse": "Research",
        },
    )

    assert response.status_code == 400
    assert "Account with this email already exists" in response.get_json()["error"]


def test_confirm_newuser_email_moves_user_to_pending_and_notifies_admin(client, monkeypatch):
    sent = []
    expires = (user_routes._utc_now() + user_routes.timedelta(minutes=15)).isoformat() + "Z"
    pending_entry = json.dumps({
        "request_id": "register_abc",
        "verification_code": "123456",
        "expires_at": expires,
    })

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(
        user_routes,
        "_load_any_user_by_identifier",
        lambda identifier: {
            "userid": "101",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["sociomap"],
            "intendedUse": "Research",
            "access": "email_unverified",
            "pendingRegistrationVerificationRequests": [pending_entry],
        },
    )

    def fake_get_query(query, driver=None, params=None, **kwargs):
        if 'u.access = "pending"' in query:
            assert params["verificationRequests"] == []
            return [{
                "userid": "101",
                "first": "Ada",
                "last": "Lovelace",
                "email": "ada@example.org",
                "database": ["sociomap"],
                "intendedUse": "Research",
                "access": "pending",
            }]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(user_routes, "getQuery", fake_get_query)
    monkeypatch.setattr(user_routes, "get_alert_recipients", lambda: ["admin@example.org"])
    monkeypatch.setattr(user_routes, "sendEmail", lambda *args, **kwargs: sent.append(kwargs) or "Email sent successfully")

    response = client.post(
        "/newuser/confirm-email",
        json={"email": "ada@example.org", "requestId": "register_abc", "verificationCode": "123456"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "pending"
    assert sent
    assert sent[0]["subject"] == "New registered user"


def test_confirm_newuser_email_rejects_bad_code(client, monkeypatch):
    expires = (user_routes._utc_now() + user_routes.timedelta(minutes=15)).isoformat() + "Z"
    pending_entry = json.dumps({
        "request_id": "register_abc",
        "verification_code": "123456",
        "expires_at": expires,
    })

    monkeypatch.setattr(
        user_routes,
        "_load_any_user_by_identifier",
        lambda identifier: {
            "userid": "101",
            "access": "email_unverified",
            "pendingRegistrationVerificationRequests": [pending_entry],
        },
    )

    response = client.post(
        "/newuser/confirm-email",
        json={"email": "ada@example.org", "requestId": "register_abc", "verificationCode": "000000"},
    )

    assert response.status_code == 400
    assert "Invalid verification code" in response.get_json()["error"]
