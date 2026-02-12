import json

import CMroutes.user_routes as user_routes


def _row_from_user(user):
    return {
        "userid": user["userid"],
        "first": user.get("first", ""),
        "last": user.get("last", ""),
        "username": user.get("username", ""),
        "email": user.get("email", ""),
        "database": user.get("database", []),
        "intendedUse": user.get("intendedUse", ""),
        "createdAt": user.get("createdAt", "2026-01-01T00:00:00Z"),
        "updatedAt": user.get("updatedAt", "2026-01-01T00:00:00Z"),
        "passwordLastChangedAt": user.get("passwordLastChangedAt", "2026-01-01T00:00:00Z"),
        "password": user.get("password", ""),
    }


def _fake_getquery_factory(users):
    def fake_getQuery(query, driver=None, params=None, type="dict", **kwargs):
        payload = dict(params or {})
        payload.update(kwargs)

        if "MATCH (u:USER {userid: toString($userid)})" in query and "u.password as password" in query:
            user = users.get(str(payload["userid"]))
            return [_row_from_user(user)] if user else []

        if "MATCH (u:USER {username: $username})" in query and "WHERE u.userid <>" in query:
            count = sum(
                1
                for user in users.values()
                if user.get("username") == payload["username"] and user.get("userid") != str(payload["userid"])
            )
            return [count] if type == "list" else [{"count": count}]

        if "MATCH (u:USER {email: $email})" in query and "WHERE u.userid <>" in query:
            count = sum(
                1
                for user in users.values()
                if user.get("email") == payload["email"] and user.get("userid") != str(payload["userid"])
            )
            return [count] if type == "list" else [{"count": count}]

        if "SET" in query and "u.first = $first" in query:
            user = users.get(str(payload["userid"]))
            if not user:
                return []
            user.update(
                {
                    "first": payload["first"],
                    "last": payload["last"],
                    "username": payload["username"],
                    "email": payload["email"],
                    "database": payload["database"].split("|") if payload["database"] else [],
                    "intendedUse": payload["intendedUse"],
                    "updatedAt": payload["updatedAt"],
                }
            )
            row = _row_from_user(user)
            row.pop("password", None)
            return [row]

        if "SET" in query and "u.password = $password" in query:
            user = users.get(str(payload["userid"]))
            if not user:
                return []
            user["password"] = payload["password"]
            user["passwordLastChangedAt"] = payload["changedAt"]
            user["updatedAt"] = payload["changedAt"]
            return [{"passwordLastChangedAt": payload["changedAt"]}]

        raise AssertionError(f"Unexpected query in test: {query}")

    return fake_getQuery


def _auth_cred(user_id="100"):
    return {"userid": user_id, "key": "token-key"}


def test_get_profile_returns_user(client, monkeypatch):
    users = {
        "100": {
            "userid": "100",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["SocioMap"],
            "intendedUse": "Research",
            "password": "old-pass",
        }
    }

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "verifyUser", lambda user, key, role=None: "verified")
    monkeypatch.setattr(user_routes, "getQuery", _fake_getquery_factory(users))

    response = client.get(
        "/profile/100",
        query_string={"credentials": json.dumps(_auth_cred("100"))},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["userId"] == "100"
    assert payload["firstName"] == "Ada"
    assert payload["database"] == "SocioMap"


def test_profile_update_request_and_confirm(client, monkeypatch):
    users = {
        "100": {
            "userid": "100",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["SocioMap"],
            "intendedUse": "Research",
            "password": "old-pass",
        }
    }

    user_routes.PROFILE_UPDATE_REQUESTS.clear()

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "verifyUser", lambda user, key, role=None: "verified")
    monkeypatch.setattr(user_routes, "getQuery", _fake_getquery_factory(users))

    request_response = client.post(
        "/profile/request-update",
        json={
            "userId": "100",
            "credentials": _auth_cred("100"),
            "updates": {
                "firstName": "Ada",
                "lastName": "Byron",
                "username": "ada-byron",
                "email": "ada.byron@example.org",
                "database": "ArchaMap",
                "intendedUse": "Profile API testing",
            },
        },
    )

    assert request_response.status_code == 200
    request_payload = request_response.get_json()
    assert request_payload["requestId"].startswith("profile_")
    assert len(request_payload["debugVerificationCode"]) == 6

    confirm_response = client.post(
        "/profile/confirm-update",
        json={
            "userId": "100",
            "credentials": _auth_cred("100"),
            "requestId": request_payload["requestId"],
            "verificationCode": request_payload["debugVerificationCode"],
        },
    )

    assert confirm_response.status_code == 200
    confirm_payload = confirm_response.get_json()
    assert confirm_payload["lastName"] == "Byron"
    assert confirm_payload["username"] == "ada-byron"
    assert confirm_payload["database"] == "ArchaMap"


def test_password_change_request_and_confirm(client, monkeypatch):
    users = {
        "100": {
            "userid": "100",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["SocioMap"],
            "intendedUse": "Research",
            "password": "old-pass",
        }
    }

    user_routes.PASSWORD_CHANGE_REQUESTS.clear()

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "verifyUser", lambda user, key, role=None: "verified")
    monkeypatch.setattr(user_routes, "verifyPassword", lambda stored_hash, candidate: stored_hash == candidate)
    monkeypatch.setattr(user_routes, "password_hash", lambda value: f"hashed::{value}")
    monkeypatch.setattr(user_routes, "getQuery", _fake_getquery_factory(users))

    request_response = client.post(
        "/profile/request-password-change",
        json={
            "userId": "100",
            "credentials": _auth_cred("100"),
            "currentPassword": "old-pass",
            "newPassword": "NewStrong",
        },
    )

    assert request_response.status_code == 200
    request_payload = request_response.get_json()
    assert request_payload["requestId"].startswith("password_")
    assert len(request_payload["debugVerificationCode"]) == 6

    confirm_response = client.post(
        "/profile/confirm-password-change",
        json={
            "userId": "100",
            "credentials": _auth_cred("100"),
            "requestId": request_payload["requestId"],
            "verificationCode": request_payload["debugVerificationCode"],
        },
    )

    assert confirm_response.status_code == 200
    payload = confirm_response.get_json()
    assert payload["passwordLastChangedAt"]
    assert users["100"]["password"] == "hashed::NewStrong"


def test_password_change_rejects_numbers_or_symbols(client, monkeypatch):
    users = {
        "100": {
            "userid": "100",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["SocioMap"],
            "intendedUse": "Research",
            "password": "old-pass",
        }
    }

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "verifyUser", lambda user, key, role=None: "verified")
    monkeypatch.setattr(user_routes, "verifyPassword", lambda stored_hash, candidate: stored_hash == candidate)
    monkeypatch.setattr(user_routes, "getQuery", _fake_getquery_factory(users))

    response = client.post(
        "/profile/request-password-change",
        json={
            "userId": "100",
            "credentials": _auth_cred("100"),
            "currentPassword": "old-pass",
            "newPassword": "abc123",
        },
    )

    assert response.status_code == 400
    assert "at least 6 letters" in response.get_json()["error"]


def test_profile_update_rejects_duplicate_username(client, monkeypatch):
    users = {
        "100": {
            "userid": "100",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["SocioMap"],
            "intendedUse": "Research",
            "password": "old-pass",
        },
        "200": {
            "userid": "200",
            "first": "Grace",
            "last": "Hopper",
            "username": "taken_name",
            "email": "grace@example.org",
            "database": ["ArchaMap"],
            "intendedUse": "Research",
            "password": "old-pass-2",
        },
    }

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "verifyUser", lambda user, key, role=None: "verified")
    monkeypatch.setattr(user_routes, "getQuery", _fake_getquery_factory(users))

    response = client.post(
        "/profile/request-update",
        json={
            "userId": "100",
            "credentials": _auth_cred("100"),
            "updates": {
                "firstName": "Ada",
                "lastName": "Lovelace",
                "username": "taken_name",
                "email": "ada@example.org",
                "database": "SocioMap",
                "intendedUse": "Research",
            },
        },
    )

    assert response.status_code == 400
    assert "Username already exists" in response.get_json()["error"]


def test_profile_update_rejects_duplicate_email(client, monkeypatch):
    users = {
        "100": {
            "userid": "100",
            "first": "Ada",
            "last": "Lovelace",
            "username": "ada",
            "email": "ada@example.org",
            "database": ["SocioMap"],
            "intendedUse": "Research",
            "password": "old-pass",
        },
        "200": {
            "userid": "200",
            "first": "Grace",
            "last": "Hopper",
            "username": "grace",
            "email": "taken@example.org",
            "database": ["ArchaMap"],
            "intendedUse": "Research",
            "password": "old-pass-2",
        },
    }

    monkeypatch.setattr(user_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(user_routes, "verifyUser", lambda user, key, role=None: "verified")
    monkeypatch.setattr(user_routes, "getQuery", _fake_getquery_factory(users))

    response = client.post(
        "/profile/request-update",
        json={
            "userId": "100",
            "credentials": _auth_cred("100"),
            "updates": {
                "firstName": "Ada",
                "lastName": "Lovelace",
                "username": "ada",
                "email": "taken@example.org",
                "database": "SocioMap",
                "intendedUse": "Research",
            },
        },
    )

    assert response.status_code == 400
    assert "Account with this email already exists" in response.get_json()["error"]
