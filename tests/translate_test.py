import pandas as pd
import CMroutes.search_routes as search_routes


def test_translate_endpoint_returns_file_and_order(client, monkeypatch):
    def fake_translate(**kwargs):
        df = pd.DataFrame([{"period": "Archaic", "CMID": "AM1"}])
        return df, ["period", "CMID"]

    monkeypatch.setattr(search_routes, "translate", fake_translate)

    payload = {
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
        "table": [{"period": "Archaic"}],
        "uniqueRows": "true",
    }

    response = client.post("/translate", json=payload)

    assert response.status_code == 200
    body = response.get_json()
    assert body["file"] == [{"CMID": "AM1", "period": "Archaic"}]
    assert body["order"] == ["period", "CMID"]
    assert body["warnings"] == []


def test_translate_endpoint_includes_overwrite_warnings(client, monkeypatch):
    def fake_translate(**kwargs):
        df = pd.DataFrame([{"period": "Archaic", "CMID": "AM1"}])
        return df, ["period", "CMID"], ["Overwrote existing uploaded column: CMID_period"]

    monkeypatch.setattr(search_routes, "translate", fake_translate)

    payload = {
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
        "table": [{"period": "Archaic"}],
        "uniqueRows": "true",
    }

    response = client.post("/translate", json=payload)

    assert response.status_code == 200
    body = response.get_json()
    assert body["warnings"] == ["Overwrote existing uploaded column: CMID_period"]
