import pandas as pd
import pytest

import CMroutes.upload_routes as upload_routes


@pytest.fixture(autouse=True)
def _stub_waiting_uses_task(monkeypatch):
    monkeypatch.setattr(
        upload_routes,
        "_start_waiting_uses_task",
        lambda **kwargs: "task-123",
    )


def _base_payload():
    return {
        "database": "ArchaMap",
        "so": "simple",
        "ao": "add_uses",
        "addoptions": {"district": False, "recordyear": False},
        "allContext": [],
        "user": "api-user",
        "df": [{"source_name": "Alpha", "source_key": "K1"}],
        "formData": {
            "domain": "LANGUAGE",
            "subdomain": "DIALECT",
            "datasetID": "AD1",
            "cmNameColumn": "source_name",
            "categoryNamesColumn": "",
            "alternateCategoryNamesColumns": [],
            "cmidColumn": "",
            "keyColumn": "source_key",
        },
    }


def test_upload_simple_uses_subdomain_label_when_present(client, monkeypatch):
    seen = {}

    def fake_input_nodes_uses(**kwargs):
        seen["dataset"] = kwargs["dataset"]
        seen["user"] = kwargs["user"]
        return pd.DataFrame([{"CMID": "AM1"}]), ["CMID"]

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "input_Nodes_Uses", fake_input_nodes_uses)

    response = client.post("/uploadInputNodes", json=_base_payload())

    assert response.status_code == 200
    body = response.get_json() or {}
    assert body.get("waitingUsesTask") == "task-123"
    assert seen["dataset"][0]["label"] == "DIALECT"
    assert seen["user"] == "api-user"


def test_upload_simple_falls_back_to_domain_when_subdomain_missing(client, monkeypatch):
    seen = {}

    def fake_input_nodes_uses(**kwargs):
        seen["dataset"] = kwargs["dataset"]
        seen["user"] = kwargs["user"]
        return pd.DataFrame([{"CMID": "AM1"}]), ["CMID"]

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "input_Nodes_Uses", fake_input_nodes_uses)

    payload = _base_payload()
    payload["formData"]["subdomain"] = ""
    payload["formData"]["domain"] = "ADM1"
    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 200
    assert seen["dataset"][0]["label"] == "ADM1"
    assert seen["user"] == "api-user"


def test_upload_simple_concatenates_multiple_altname_columns(client, monkeypatch):
    seen = {}

    def fake_input_nodes_uses(**kwargs):
        seen["dataset"] = kwargs["dataset"]
        seen["user"] = kwargs["user"]
        return pd.DataFrame([{"CMID": "AM1"}]), ["CMID"]

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "input_Nodes_Uses", fake_input_nodes_uses)

    payload = _base_payload()
    payload["df"] = [
        {
            "source_name": "Alpha",
            "source_key": "K1",
            "alt_one": "A1",
            "alt_two": "A2",
            "alt_three": "",
        }
    ]
    payload["formData"]["alternateCategoryNamesColumns"] = ["alt_one", "alt_two", "alt_three"]

    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 200
    assert seen["dataset"][0]["altNames"] == "A1;A2"
    assert seen["user"] == "api-user"


def test_upload_rejects_user_mismatch_with_authenticated_identity(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "input_Nodes_Uses",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("upload should not execute")),
    )

    payload = _base_payload()
    payload["user"] = "other-user"

    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 403
    body = response.get_data(as_text=True).lower()
    assert "does not match authenticated api key/token owner" in body


def test_upload_returns_401_for_missing_credentials(client, monkeypatch):
    monkeypatch.setattr(
        upload_routes,
        "verify_request_auth",
        lambda **kwargs: (_ for _ in ()).throw(Exception("Missing credentials")),
    )
    monkeypatch.setattr(
        upload_routes,
        "input_Nodes_Uses",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("upload should not execute without auth")),
    )

    response = client.post("/uploadInputNodes", json=_base_payload())

    assert response.status_code == 401
    body = response.get_json() or {}
    assert "missing credentials" in str(body.get("error", "")).lower()


def test_upload_waiting_uses_status_returns_task_for_authenticated_user(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "_get_waiting_uses_task",
        lambda task_id: {
            "taskId": task_id,
            "status": "completed",
            "user": "api-user",
            "database": "ArchaMap",
            "createdAt": "2026-02-26T00:00:00+00:00",
            "startedAt": "2026-02-26T00:00:01+00:00",
            "finishedAt": "2026-02-26T00:00:02+00:00",
            "message": "Successfully updated 3 CMIDs in batches of 1000.",
            "error": None,
        },
    )

    response = client.post(
        "/uploadWaitingUSESStatus",
        json={"taskId": "task-123", "user": "api-user"},
    )

    assert response.status_code == 200
    body = response.get_json() or {}
    assert body.get("taskId") == "task-123"
    assert body.get("status") == "completed"


def test_upload_waiting_uses_status_rejects_user_mismatch(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "_get_waiting_uses_task",
        lambda task_id: (_ for _ in ()).throw(AssertionError("status lookup should not execute on mismatch")),
    )

    response = client.post(
        "/uploadWaitingUSESStatus",
        json={"taskId": "task-123", "user": "other-user"},
    )

    assert response.status_code == 403
    body = response.get_data(as_text=True).lower()
    assert "does not match authenticated api key/token owner" in body
