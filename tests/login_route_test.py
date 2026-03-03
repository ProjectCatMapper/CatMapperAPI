import CMroutes.user_routes as user_routes


def test_login_success_returns_token_payload(client, monkeypatch):
    monkeypatch.setattr(
        user_routes,
        "login",
        lambda user, password: {
            "userid": "42",
            "username": "ada",
            "role": "admin",
        },
    )
    monkeypatch.setattr(user_routes, "issue_auth_token", lambda userid, role: "token-123")

    response = client.post("/login", json={"user": "ada", "password": "secret"})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "userid": "42",
        "username": "ada",
        "role": "admin",
        "token": "token-123",
    }


def test_login_auth_error_tuple_returns_json_error(client, monkeypatch):
    monkeypatch.setattr(
        user_routes,
        "login",
        lambda user, password: ({"error": "invalid password"}, 401),
    )

    response = client.post("/login", json={"user": "ada", "password": "bad"})

    assert response.status_code == 401
    assert response.get_json() == {"error": "invalid password"}


def test_login_auth_error_string_tuple_returns_json_error(client, monkeypatch):
    monkeypatch.setattr(
        user_routes,
        "login",
        lambda user, password: ("verification failed", 401),
    )

    response = client.post("/login", json={"user": "ada", "password": "bad"})

    assert response.status_code == 401
    assert response.get_json() == {"error": "verification failed"}
