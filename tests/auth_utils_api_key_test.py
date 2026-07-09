import CMroutes.auth_utils as auth_utils
from itsdangerous import BadSignature, SignatureExpired


class DummyRequest:
    def __init__(self, headers=None):
        self.headers = headers or {}


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


def test_verify_request_auth_accepts_x_api_key_header_without_userid(monkeypatch):
    monkeypatch.setattr(auth_utils, "parse_bearer_token", lambda req=None: None)
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
            {
                "userid": "123",
                "access": "enabled",
                "role": "user",
                "apiKeyHash": "hashed::cmk_header_key",
                "apiKeyHashes": [],
            }
        ],
    )

    claims = auth_utils.verify_request_auth(
        req=DummyRequest(headers={"X-API-Key": "cmk_header_key"}),
    )

    assert claims["userid"] == "123"
    assert claims["role"] == "user"


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


def test_verify_request_auth_rejects_required_userid_mismatch_from_api_key(monkeypatch):
    monkeypatch.setattr(auth_utils, "parse_bearer_token", lambda req=None: None)
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
            {
                "userid": "123",
                "access": "enabled",
                "role": "user",
                "apiKeyHash": "hashed::cmk_header_key",
                "apiKeyHashes": [],
            }
        ],
    )

    try:
        auth_utils.verify_request_auth(
            required_userid="999",
            req=DummyRequest(headers={"X-API-Key": "cmk_header_key"}),
        )
    except Exception as exc:
        assert "do not match" in str(exc).lower()
    else:
        raise AssertionError("Expected userid mismatch to fail")


def test_verify_request_auth_reports_expired_bearer_token(monkeypatch):
    class ExpiredSerializer:
        def loads(self, token, max_age=None):
            raise SignatureExpired("expired")

    monkeypatch.setattr(auth_utils, "_serializer", lambda: ExpiredSerializer())

    try:
        auth_utils.verify_request_auth(
            req=DummyRequest(headers={"Authorization": "Bearer stale-token"}),
        )
    except Exception as exc:
        assert "session expired" in str(exc).lower()
        assert auth_utils.classify_auth_error_status(exc) == 401
    else:
        raise AssertionError("Expected expired bearer token to fail")


def test_verify_request_auth_reports_invalid_bearer_token(monkeypatch):
    class InvalidSerializer:
        def loads(self, token, max_age=None):
            raise BadSignature("bad signature")

    monkeypatch.setattr(auth_utils, "_serializer", lambda: InvalidSerializer())

    try:
        auth_utils.verify_request_auth(
            req=DummyRequest(headers={"Authorization": "Bearer invalid-token"}),
        )
    except Exception as exc:
        assert "invalid token" in str(exc).lower()
        assert auth_utils.classify_auth_error_status(exc) == 401
    else:
        raise AssertionError("Expected invalid bearer token to fail")
