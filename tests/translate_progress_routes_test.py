import time

import pandas as pd

import CMroutes.search_routes as search_routes


def _translate_payload():
    return {
        "database": "ArchaMap",
        "property": "Name",
        "domain": "PERIOD",
        "key": "false",
        "term": "period",
        "country": "",
        "context": "",
        "dataset": "",
        "yearStart": None,
        "yearEnd": None,
        "query": "false",
        "table": [{"period": "Archaic"}, {"period": "Classic"}],
        "countsamename": False,
        "uniqueRows": "true",
    }


def test_translate_start_and_status_flow_returns_progress_and_result(client, monkeypatch):
    monkeypatch.setattr(search_routes, "get_redis_connection", lambda: None)

    def fake_translate(**kwargs):
        callback = kwargs.get("progress_callback")
        if callable(callback):
            callback(percent=20, message="Preprocessing complete.", processedRows=0, totalRows=2)
            callback(percent=55, message="Processing 1 out of 2 rows.", processedRows=1, totalRows=2)
            callback(percent=90, message="Processing 2 out of 2 rows.", processedRows=2, totalRows=2)
        return pd.DataFrame([{"period": "Archaic", "CMID": "AM1"}]), ["period", "CMID"], []

    monkeypatch.setattr(search_routes, "translate", fake_translate)

    start_response = client.post("/translate/start", json=_translate_payload())
    assert start_response.status_code == 200
    start_payload = start_response.get_json()
    assert start_payload["status"] == "processing"
    assert start_payload["percent"] == 10
    task_id = start_payload["taskId"]
    assert task_id

    final_payload = None
    last_payload = None
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        status_response = client.post("/translate/status", json={"taskId": task_id})
        assert status_response.status_code == 200
        payload = status_response.get_json()
        last_payload = payload
        if payload.get("status") == "completed":
            final_payload = payload
            break
        time.sleep(0.01)

    assert final_payload is not None, f"translate task did not complete in time; last payload={last_payload}"
    assert final_payload["percent"] == 100
    assert final_payload["file"] == [{"CMID": "AM1", "period": "Archaic"}]
    assert final_payload["order"] == ["period", "CMID"]
    assert final_payload["warnings"] == []


def test_translate_status_returns_404_for_unknown_task(client, monkeypatch):
    monkeypatch.setattr(search_routes, "get_redis_connection", lambda: None)

    response = client.post("/translate/status", json={"taskId": "missing-task"})

    assert response.status_code == 404
    assert response.get_json()["error"] == "Task not found"
