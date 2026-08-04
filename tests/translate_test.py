import pandas as pd
import importlib
import CMroutes.search_routes as search_routes

search_module = importlib.import_module("CM.search")


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


def test_translate_key_dataset_filter_requires_matching_uses_key(monkeypatch):
    monkeypatch.setattr(search_module, "getDriver", lambda database: object())
    monkeypatch.setattr(search_module, "validate_domain_label", lambda domain, **kwargs: domain)
    monkeypatch.setattr(search_module, "getQuery", lambda query, driver, params=None: [])

    result = search_module.translate(
        database="SocioMap",
        property="Key",
        domain="CATEGORY",
        key="false",
        term="key_term",
        country="",
        context="",
        dataset="datasetID",
        yearStart=None,
        yearEnd=None,
        query="true",
        table=[{"key_term": "V024 == 6", "datasetID": "SD468368"}],
        countsamename=None,
    )

    query = " ".join(result[0]["query"].split())
    assert "match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) where r.Key = matching" in query


def test_translate_formats_country_lists_when_first_row_is_blank(monkeypatch):
    monkeypatch.setattr(search_module, "getDriver", lambda database: object())
    monkeypatch.setattr(search_module, "validate_domain_label", lambda domain, **kwargs: domain)

    def fake_get_query(query, driver, params=None):
        return [
            {
                "CMuniqueCategoryID": 0,
                "CMuniqueRowID": [0],
                "term": "Alpha",
                "country": None,
                "context": None,
                "CMID": "SM1",
                "CMName": "Alpha",
                "label": "CATEGORY",
                "matching": "Alpha",
                "matchingDistance": 0,
                "CMcountry": "",
                "Key": "",
            },
            {
                "CMuniqueCategoryID": 1,
                "CMuniqueRowID": [1],
                "term": "Beta",
                "country": None,
                "context": None,
                "CMID": "SM2",
                "CMName": "Beta",
                "label": "CATEGORY",
                "matching": "Beta",
                "matchingDistance": 0,
                "CMcountry": ["United States of America", "Mexico"],
                "Key": "",
            },
        ]

    monkeypatch.setattr(search_module, "getQuery", fake_get_query)

    data, order, warnings = search_module.translate(
        database="SocioMap",
        property="Name",
        domain="CATEGORY",
        key="false",
        term="source_name",
        country="",
        context="",
        dataset="",
        yearStart=None,
        yearEnd=None,
        query="false",
        table=[{"source_name": "Alpha"}, {"source_name": "Beta"}],
        countsamename=None,
    )

    assert warnings == []
    assert "CMcountry_source_name" in order
    assert data["CMcountry_source_name"].tolist() == ["", "United States of America; Mexico"]
