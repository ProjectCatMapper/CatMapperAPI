import CMroutes.admin_routes as admin_routes


def test_admin_edit_allows_bearer_token_without_legacy_cred(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "admin"})
    monkeypatch.setattr(admin_routes, "replaceProperty", lambda cmid, prop, old, new, database: "ok")
    monkeypatch.setattr(admin_routes, "login", lambda user, pwd: (_ for _ in ()).throw(Exception("legacy login path should not run")))

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "replaceProperty",
            "cmid": "AM1",
            "property": "Name",
            "old": "Old",
            "new": "New",
        },
    )

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "ok"


def test_admin_edit_rejects_non_admin_bearer(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: (_ for _ in ()).throw(Exception("User is not authorized")))

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={"database": "ArchaMap", "fun": "replaceProperty"},
    )

    assert response.status_code == 500
    assert "not authorized" in response.get_data(as_text=True).lower()


def test_update_waiting_uses_rejects_user_mismatch(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "200", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "waitingUSES",
        lambda database: (_ for _ in ()).throw(AssertionError("waitingUSES should not execute on mismatch")),
    )

    response = client.post(
        "/updateWaitingUSES",
        json={"database": "ArchaMap", "user": "201"},
    )

    assert response.status_code == 500
    assert "does not match authenticated api key/token owner" in response.get_data(as_text=True).lower()
