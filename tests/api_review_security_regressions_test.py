import CMroutes.admin_routes as admin_routes
import CMroutes.auth_utils as auth_utils


def test_merge_uses_ties_is_post_only(client):
    response = client.get("/mergeUSESties", query_string={"database": "ArchaMap", "CMID": "AM1"})
    assert response.status_code == 405


def test_bearer_auth_returns_current_role_after_demotion(monkeypatch):
    monkeypatch.setattr(auth_utils, "parse_bearer_token", lambda req=None: {"userid": "7", "role": "admin"})
    monkeypatch.setattr(auth_utils, "_get_active_user", lambda userid: {"access": "enabled", "role": "user"})
    claims = auth_utils.verify_request_auth(req=object())
    assert claims == {"userid": "7", "role": "user"}


def test_bearer_auth_demoted_user_cannot_satisfy_admin(monkeypatch):
    monkeypatch.setattr(auth_utils, "parse_bearer_token", lambda req=None: {"userid": "7", "role": "admin"})
    monkeypatch.setattr(auth_utils, "_get_active_user", lambda userid: {"access": "enabled", "role": "user"})
    try:
        auth_utils.verify_request_auth(required_role="admin", req=object())
    except Exception as exc:
        assert str(exc) == "User is not authorized"
    else:
        raise AssertionError("demoted bearer token was accepted as admin")


def test_add_foci_requires_post_and_admin(client, monkeypatch):
    response = client.get("/addFoci", query_string={"database": "ArchaMap", "datasetID": "AM1", "foci": "AM2"})
    assert response.status_code == 405
    monkeypatch.setattr("CMroutes.homepage_routes.verify_request_auth", lambda **kwargs: (_ for _ in ()).throw(Exception("Missing credentials")))
    response = client.post("/addFoci", json={"database": "ArchaMap", "datasetID": "AM1", "foci": "AM2"})
    assert response.status_code == 401


def test_send_test_email_requires_post_and_admin(client):
    assert client.get("/send_test_email/test@example.com").status_code == 405
    assert client.post("/send_test_email/test@example.com", json={}).status_code == 401
