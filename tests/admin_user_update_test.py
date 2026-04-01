import CMroutes.admin_routes as admin_routes


def test_admin_user_update_changes_password_with_hash_and_timestamp(client, monkeypatch):
    user_row = {
        "userid": "42",
        "first": "Ada",
        "last": "Lovelace",
        "username": "ada",
        "email": "ada@example.org",
        "database": ["sociomap"],
        "intendedUse": "Research",
        "access": "enabled",
        "role": "user",
        "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z",
        "logCount": 1,
    }

    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "900", "role": "admin"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "_build_activity_stats_for_userids", lambda userids: {"42": {"total": {"totalActions": 3}}})
    monkeypatch.setattr(admin_routes, "password_hash", lambda value: f"hashed::{value}")
    monkeypatch.setattr(admin_routes, "_now_iso", lambda: "2026-04-01T12:00:00Z")

    def fake_get_query(query, driver=None, params=None, type="dict", **kwargs):
        if "MATCH (u:USER {userid: toString($userid)})" in query and "SET" not in query:
            return [dict(user_row)]

        if "MATCH (u:USER {userid: toString($userid)})" in query and "SET" in query:
            assert params["userid"] == "42"
            assert params["passwordProvided"] is True
            assert params["password"] == "hashed::new-secret"
            assert params["passwordChangedAt"] == "2026-04-01T12:00:00Z"
            assert "password: '[hidden]' -> '[updated]'" in params["logEntries"][0]
            updated = dict(user_row)
            updated["updatedAt"] = params["updatedAt"]
            updated["logCount"] = 2
            return [updated]

        raise AssertionError(f"Unexpected query in test: {query}")

    monkeypatch.setattr(admin_routes, "getQuery", fake_get_query)

    response = client.post(
        "/admin/users/update",
        headers={"Authorization": "Bearer test-token"},
        json={
            "userid": "42",
            "updates": {
                "password": "new-secret",
            },
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["message"] == "User updated"
    assert payload["changedFields"] == ["password"]
    assert payload["user"]["userid"] == "42"


def test_admin_user_update_rejects_short_password(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "900", "role": "admin"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(
        admin_routes,
        "getQuery",
        lambda query, driver=None, params=None, type="dict", **kwargs: [{
            "userid": "42",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["sociomap"],
            "intendedUse": "Research",
            "access": "enabled",
            "role": "user",
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
            "logCount": 1,
        }],
    )

    response = client.post(
        "/admin/users/update",
        headers={"Authorization": "Bearer test-token"},
        json={
            "userid": "42",
            "updates": {
                "password": "123",
            },
        },
    )

    assert response.status_code == 400
    assert "at least 6 characters" in response.get_json()["error"].lower()
