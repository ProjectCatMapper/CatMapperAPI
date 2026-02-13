import pandas as pd

import CMroutes.upload_routes as upload_routes


def _base_payload():
    return {
        "database": "ArchaMap",
        "so": "simple",
        "ao": "add_uses",
        "addoptions": {"district": False, "recordyear": False},
        "allContext": [],
        "user": "tester",
        "df": [{"source_name": "Alpha", "source_key": "K1"}],
        "formData": {
            "domain": "LANGUAGE",
            "subdomain": "DIALECT",
            "datasetID": "AD1",
            "cmNameColumn": "source_name",
            "categoryNamesColumn": "",
            "alternateCategoryNamesColumn": "",
            "cmidColumn": "",
            "keyColumn": "source_key",
        },
    }


def test_upload_simple_uses_subdomain_label_when_present(client, monkeypatch):
    seen = {}

    def fake_input_nodes_uses(**kwargs):
        seen["dataset"] = kwargs["dataset"]
        return pd.DataFrame([{"CMID": "AM1"}]), ["CMID"]

    monkeypatch.setattr(upload_routes, "input_Nodes_Uses", fake_input_nodes_uses)

    response = client.post("/uploadInputNodes", json=_base_payload())

    assert response.status_code == 200
    assert seen["dataset"][0]["label"] == "DIALECT"


def test_upload_simple_falls_back_to_domain_when_subdomain_missing(client, monkeypatch):
    seen = {}

    def fake_input_nodes_uses(**kwargs):
        seen["dataset"] = kwargs["dataset"]
        return pd.DataFrame([{"CMID": "AM1"}]), ["CMID"]

    monkeypatch.setattr(upload_routes, "input_Nodes_Uses", fake_input_nodes_uses)

    payload = _base_payload()
    payload["formData"]["subdomain"] = ""
    payload["formData"]["domain"] = "ADM1"
    response = client.post("/uploadInputNodes", json=payload)

    assert response.status_code == 200
    assert seen["dataset"][0]["label"] == "ADM1"
