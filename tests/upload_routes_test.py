import pytest

import CMroutes.upload_routes as upload_routes
from CMroutes.task_store import get_task_store


@pytest.fixture(autouse=True)
def _clear_task_state():
    store = get_task_store()
    if hasattr(store, "upload_tasks"):
        with store.lock:
            store.upload_tasks.clear()
            store.waiting_tasks.clear()
    yield
    if hasattr(store, "upload_tasks"):
        with store.lock:
            store.upload_tasks.clear()
            store.waiting_tasks.clear()


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

    def fake_start_upload_task(**kwargs):
        seen.update(kwargs)
        return "upload-task-123"

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "_start_upload_task", fake_start_upload_task)

    response = client.post("/uploadInputNodes", json=_base_payload())

    assert response.status_code == 202
    body = response.get_json() or {}
    assert body.get("taskId") == "upload-task-123"
    assert seen["job_args"]["dataset"][0]["label"] == "DIALECT"
    assert seen["job_args"]["batchSize"] == 500
    assert seen["job_args"]["optionalProperties"] == []
    assert seen["user"] == "api-user"
    assert seen["total_rows"] == 1


def test_upload_simple_falls_back_to_domain_when_subdomain_missing(client, monkeypatch):
    seen = {}

    def fake_start_upload_task(**kwargs):
        seen.update(kwargs)
        return "upload-task-123"

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "_start_upload_task", fake_start_upload_task)

    payload = _base_payload()
    payload["formData"]["subdomain"] = ""
    payload["formData"]["domain"] = "ADM1"
    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 202
    assert seen["job_args"]["dataset"][0]["label"] == "ADM1"
    assert seen["job_args"]["optionalProperties"] == []
    assert seen["user"] == "api-user"


def test_upload_simple_concatenates_multiple_altname_columns(client, monkeypatch):
    seen = {}

    def fake_start_upload_task(**kwargs):
        seen.update(kwargs)
        return "upload-task-123"

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "_start_upload_task", fake_start_upload_task)

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

    assert response.status_code == 202
    assert seen["job_args"]["dataset"][0]["altNames"] == "A1;A2"
    assert seen["user"] == "api-user"


def test_upload_rejects_simple_mode_for_non_add_uses(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "_start_upload_task",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("upload should not execute")),
    )

    payload = _base_payload()
    payload["ao"] = "update_add"

    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 500
    body = response.get_json() or {}
    assert "only supported with `ao = add_uses`" in str(body.get("error", "")).lower()


def test_upload_rejects_preformatted_keys_in_simple_mode(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "_start_upload_task",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("upload should not execute")),
    )

    payload = _base_payload()
    payload["df"] = [{"source_name": "Alpha", "source_key": "Type == Adamana Brown"}]

    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 500
    body = response.get_json() or {}
    assert "simple upload expects raw key values" in str(body.get("error", "")).lower()


def test_upload_prefers_optional_properties_over_all_context(client, monkeypatch):
    seen = {}

    def fake_start_upload_task(**kwargs):
        seen.update(kwargs)
        return "upload-task-123"

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "_start_upload_task", fake_start_upload_task)

    payload = _base_payload()
    payload["allContext"] = ["legacy_a", "legacy_b"]
    payload["optionalProperties"] = ["preferred_prop"]

    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 202
    assert seen["job_args"]["optionalProperties"] == ["preferred_prop"]
    body = response.get_json() or {}
    assert "warnings" not in body


def test_upload_returns_deprecation_warning_for_all_context_only(client, monkeypatch):
    seen = {}

    def fake_start_upload_task(**kwargs):
        seen.update(kwargs)
        return "upload-task-123"

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "_start_upload_task", fake_start_upload_task)

    payload = _base_payload()
    payload["allContext"] = ["legacy_prop"]

    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 202
    assert seen["job_args"]["optionalProperties"] == ["legacy_prop"]
    body = response.get_json() or {}
    warnings = body.get("warnings") or []
    assert any("deprecated" in str(w).lower() for w in warnings)


def test_upload_passes_ignore_if_same_to_job_args(client, monkeypatch):
    seen = {}

    def fake_start_upload_task(**kwargs):
        seen.update(kwargs)
        return "upload-task-123"

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(upload_routes, "_start_upload_task", fake_start_upload_task)

    payload = _base_payload()
    payload["so"] = "standard"
    payload["ignore_if_same"] = True
    payload["df"] = [
        {"CMID": "AM1", "Key": "Type == Adamana Brown", "datasetID": "AD1", "label": "DIALECT", "Name": "Alpha"}
    ]
    payload["formData"]["cmNameColumn"] = "Name"
    payload["formData"]["categoryNamesColumn"] = "Name"
    payload["formData"]["cmidColumn"] = "CMID"
    payload["formData"]["keyColumn"] = "Key"

    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 202
    assert seen["job_args"]["ignoreIfSame"] is True


def test_upload_rejects_user_mismatch_with_authenticated_identity(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "_start_upload_task",
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
        "_start_upload_task",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("upload should not execute without auth")),
    )

    response = client.post("/uploadInputNodes", json=_base_payload())

    assert response.status_code == 401
    body = response.get_json() or {}
    assert "missing credentials" in str(body.get("error", "")).lower()


def test_upload_status_returns_task_for_authenticated_user(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "_get_upload_task",
        lambda task_id, cursor=0: {
            "taskId": task_id,
            "status": "running",
            "user": "api-user",
            "database": "ArchaMap",
            "createdAt": "2026-03-03T00:00:00+00:00",
            "startedAt": "2026-03-03T00:00:01+00:00",
            "finishedAt": None,
            "message": None,
            "error": None,
            "progress": {
                "batchSize": 500,
                "totalRows": 1000,
                "totalBatches": 2,
                "completedBatches": 1,
                "percent": 50,
            },
            "events": ["End of batch"],
            "nextCursor": 1,
            "file": None,
            "order": None,
            "error_details": [{"row": 2, "field": "Key", "code": "invalid_format", "message": "bad"}],
        },
    )

    response = client.post(
        "/uploadInputNodesStatus",
        json={"taskId": "task-123", "user": "api-user", "cursor": 0},
    )

    assert response.status_code == 200
    body = response.get_json() or {}
    assert body.get("taskId") == "task-123"
    assert body.get("status") == "running"
    assert body.get("progress", {}).get("batchSize") == 500
    assert body.get("error_details")[0]["field"] == "Key"


def test_upload_status_rejects_user_mismatch(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "_get_upload_task",
        lambda task_id, cursor=0: (_ for _ in ()).throw(AssertionError("status lookup should not execute on mismatch")),
    )

    response = client.post(
        "/uploadInputNodesStatus",
        json={"taskId": "task-123", "user": "other-user"},
    )

    assert response.status_code == 403
    body = response.get_data(as_text=True).lower()
    assert "does not match authenticated api key/token owner" in body


def test_upload_cancel_sets_cancel_requested(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "api-user", "role": "user"})
    task_state = {
        "taskId": "task-123",
        "status": "running",
        "user": "api-user",
        "database": "ArchaMap",
        "createdAt": "2026-03-03T00:00:00+00:00",
        "startedAt": "2026-03-03T00:00:01+00:00",
        "finishedAt": None,
        "message": None,
        "error": None,
        "progress": {
            "batchSize": 500,
            "totalRows": 10,
            "totalBatches": 1,
            "completedBatches": 0,
            "percent": 0,
        },
        "events": [],
        "nextCursor": 0,
        "file": None,
        "order": None,
        "cancelRequested": False,
        "waitingUsesTask": None,
        "waitingUsesStatus": None,
    }

    def fake_get_upload_task(task_id, cursor=0):
        return dict(task_state)

    def fake_cancel_upload_task(task_id, task):
        task_state["cancelRequested"] = True
        task_state["events"] = task_state["events"] + ["Cancellation requested by user."]
        task_state["nextCursor"] += 1

    monkeypatch.setattr(upload_routes, "_get_upload_task", fake_get_upload_task)
    monkeypatch.setattr(upload_routes, "_cancel_upload_task", fake_cancel_upload_task)

    response = client.post(
        "/uploadInputNodesCancel",
        json={"taskId": "task-123", "user": "api-user", "cursor": 0},
    )

    assert response.status_code == 202
    body = response.get_json() or {}
    assert body.get("taskId") == "task-123"
    assert task_state["cancelRequested"] is True
    assert "Cancellation requested by user." in task_state["events"]


def test_cancel_upload_task_marks_queued_task_canceled_and_clears_payload(monkeypatch):
    store = get_task_store()
    task_id = store.create_upload_task(
        user="api-user",
        database="ArchaMap",
        total_rows=100,
        batch_size=500,
    )
    store.set_upload_job_payload(task_id, {"example": True})

    task = store.get_upload_task(task_id, cursor=0)
    assert task["status"] == "queued"

    monkeypatch.setattr(upload_routes, "_send_cancel_to_rq", lambda task_id, task: (True, "removed-queued-job"))
    upload_routes._cancel_upload_task(task_id, task)

    updated = store.get_upload_task(task_id, cursor=0)
    assert updated["status"] == "canceled"
    assert store.get_upload_job_payload(task_id) is None
    assert "Queued job removed from queue." in updated["events"]


def test_cancel_upload_task_running_adds_stop_signal_event(monkeypatch):
    store = get_task_store()
    task_id = store.create_upload_task(
        user="api-user",
        database="ArchaMap",
        total_rows=100,
        batch_size=500,
    )
    store.mark_upload_running(task_id)

    task = store.get_upload_task(task_id, cursor=0)
    assert task["status"] == "running"

    monkeypatch.setattr(upload_routes, "_send_cancel_to_rq", lambda task_id, task: (True, "sent-stop-signal"))
    upload_routes._cancel_upload_task(task_id, task)

    updated = store.get_upload_task(task_id, cursor=0)
    assert updated["status"] == "running"
    assert updated["cancelRequested"] is True
    assert "Stop signal sent to worker." in updated["events"]


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
