import pandas as pd

import CMroutes.upload_routes as upload_routes


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

    assert response.status_code == 500
    body = response.get_data(as_text=True).lower()
    assert "does not match authenticated api key/token owner" in body
