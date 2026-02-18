import CMroutes.auth_utils as auth_utils


def test_verify_request_auth_accepts_api_key_credentials(monkeypatch):
    monkeypatch.setattr(auth_utils, "parse_bearer_token", lambda req=None: None)
    monkeypatch.setattr(auth_utils, "verifyUser", lambda user, key, role=None: "verification failed")
    monkeypatch.setattr(auth_utils, "getDriver", lambda database: object())
    monkeypatch.setattr(
        auth_utils,
        "verifyPassword",
        lambda stored_hash, candidate: stored_hash == f"hashed::{candidate}",
    )
    monkeypatch.setattr(
        auth_utils,
        "getQuery",
        lambda query, driver=None, params=None, **kwargs: [
            {"access": "enabled", "role": "admin", "apiKeyHash": "hashed::cmk_secret", "apiKeyHashes": []}
        ],
    )

    claims = auth_utils.verify_request_auth(
        required_userid="100",
        required_role="admin",
        credentials={"userid": "100", "key": "cmk_secret"},
    )

    assert claims["userid"] == "100"
    assert claims["role"] == "admin"


def test_verify_request_auth_rejects_invalid_api_key_credentials(monkeypatch):
    monkeypatch.setattr(auth_utils, "parse_bearer_token", lambda req=None: None)
    monkeypatch.setattr(auth_utils, "verifyUser", lambda user, key, role=None: "verification failed")
    monkeypatch.setattr(auth_utils, "getDriver", lambda database: object())
    monkeypatch.setattr(auth_utils, "verifyPassword", lambda stored_hash, candidate: False)
    monkeypatch.setattr(
        auth_utils,
        "getQuery",
        lambda query, driver=None, params=None, **kwargs: [
            {"access": "enabled", "role": "user", "apiKeyHash": "hashed::cmk_secret", "apiKeyHashes": []}
        ],
    )

    try:
        auth_utils.verify_request_auth(
            required_userid="100",
            credentials={"userid": "100", "key": "bad_key"},
        )
    except Exception as exc:
        assert "not verified" in str(exc).lower()
    else:
        raise AssertionError("Expected verify_request_auth to fail for invalid API key")
