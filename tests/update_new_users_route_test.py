import CMroutes.user_routes as user_routes


def test_update_new_users_normalizes_database_to_lowercase(client, monkeypatch):
    monkeypatch.setattr(
        user_routes,
        "verify_request_auth",
        lambda **kwargs: {"userid": "200", "role": "admin"},
    )
    captured = {}

    def fake_enable_user(database, process, userid, approver):
        captured["database"] = database
        captured["process"] = process
        captured["userid"] = userid
        captured["approver"] = approver
        return []

    monkeypatch.setattr(user_routes, "enableUser", fake_enable_user)

    response = client.post(
        "/updateNewUsers",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "process": "None",
            "userid": "none",
        },
    )

    assert response.status_code == 200
    assert response.get_json() == []
    assert captured == {
        "database": "archamap",
        "process": "None",
        "userid": "none",
        "approver": "200",
    }


def test_update_new_users_returns_500_on_neo4j_query_error(client, monkeypatch):
    monkeypatch.setattr(
        user_routes,
        "verify_request_auth",
        lambda **kwargs: {"userid": "200", "role": "admin"},
    )

    def fake_enable_user(database, process, userid, approver):
        raise Exception(
            "Query execution error: {code: Neo.ClientError.Statement.SyntaxError} "
            "{message: Invalid input}"
        )

    monkeypatch.setattr(user_routes, "enableUser", fake_enable_user)

    response = client.post(
        "/updateNewUsers",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "process": "None",
            "userid": "none",
        },
    )

    assert response.status_code == 500
    assert "Neo.ClientError.Statement.SyntaxError" in response.get_data(as_text=True)
