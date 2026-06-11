import json

import CM.reconciliation as reconciliation
import CMroutes.reconciliation_routes as reconciliation_routes


def test_reconciliation_manifest_uses_clean_api_host_urls(client):
    response = client.get("/reconcile/SocioMap", base_url="https://api.catmapper.org")

    assert response.status_code == 200
    body = response.get_json()
    assert body["versions"] == ["0.2"]
    assert body["preview"]["url"] == "https://api.catmapper.org/reconcile/SocioMap/preview/{{id}}"
    assert body["suggest"]["entity"]["service_url"] == "https://api.catmapper.org/reconcile/SocioMap"
    assert body["view"]["url"] == "https://catmapper.org/sociomap/{{id}}"


def test_reconciliation_manifest_alias_advertises_canonical_dev_url(client):
    response = client.get(
        "/api/databases/ArchaMap/reconcile",
        base_url="https://dev-api.catmapper.org",
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["preview"]["url"] == "https://dev-api.catmapper.org/reconcile/ArchaMap/preview/{{id}}"
    assert body["view"]["url"] == "https://dev.catmapper.org/archamap/{{id}}"


def test_reconciliation_get_queries_batch(client, monkeypatch):
    captured = {}

    def fake_reconcile_query_batch(database, queries):
        captured["database"] = database
        captured["queries"] = queries
        return {"q0": {"result": [{"id": "SM1", "name": "Aymara", "score": 100}]}}

    monkeypatch.setattr(reconciliation_routes, "reconcile_query_batch", fake_reconcile_query_batch)

    response = client.get(
        "/reconcile/SocioMap",
        query_string={"queries": json.dumps({"q0": {"query": "Aymara"}})},
    )

    assert response.status_code == 200
    assert captured == {"database": "SocioMap", "queries": {"q0": {"query": "Aymara"}}}
    assert response.get_json()["q0"]["result"][0]["id"] == "SM1"


def test_reconciliation_post_form_queries_batch(client, monkeypatch):
    captured = {}

    def fake_reconcile_query_batch(database, queries):
        captured["database"] = database
        captured["queries"] = queries
        return {"row-a": {"result": []}}

    monkeypatch.setattr(reconciliation_routes, "reconcile_query_batch", fake_reconcile_query_batch)

    response = client.post(
        "/api/reconcile/ArchaMap",
        data={"queries": json.dumps({"row-a": {"query": "ceramic"}})},
    )

    assert response.status_code == 200
    assert captured == {"database": "ArchaMap", "queries": {"row-a": {"query": "ceramic"}}}
    assert response.get_json() == {"row-a": {"result": []}}


def test_reconciliation_suggest_entity_route(client, monkeypatch):
    def fake_suggest_entities(database, prefix="", cursor=0, limit=20):
        assert database == "SocioMap"
        assert prefix == "ay"
        return {"result": [{"id": "SM1", "name": "Aymara"}]}

    monkeypatch.setattr(reconciliation_routes, "suggest_entities", fake_suggest_entities)

    response = client.get("/reconcile/SocioMap/suggest/entity?prefix=ay")

    assert response.status_code == 200
    assert response.get_json() == {"result": [{"id": "SM1", "name": "Aymara"}]}


def test_reconciliation_preview_route_returns_html(client, monkeypatch):
    def fake_build_preview_html(database, cmid, frontend_base_url):
        assert database == "SocioMap"
        assert cmid == "SM1"
        assert frontend_base_url
        return "<!doctype html><html><body>Aymara</body></html>"

    monkeypatch.setattr(reconciliation_routes, "build_preview_html", fake_build_preview_html)

    response = client.get("/reconcile/SocioMap/preview/SM1")

    assert response.status_code == 200
    assert response.mimetype == "text/html"
    assert b"Aymara" in response.data


def test_reconciliation_preview_route_404_for_missing_node(client, monkeypatch):
    def fake_build_preview_html(database, cmid, frontend_base_url):
        raise LookupError("Node not found")

    monkeypatch.setattr(reconciliation_routes, "build_preview_html", fake_build_preview_html)

    response = client.get("/reconcile/SocioMap/preview/SM404")

    assert response.status_code == 404
    assert response.get_json() == {"error": "Node not found"}


def test_reconciliation_properties_route(client, monkeypatch):
    def fake_propose_properties(database, type_id=None, limit=None):
        assert database == "ArchaMap"
        assert type_id == "CERAMICS"
        assert limit == "3"
        return {"type": "CERAMICS", "properties": [{"id": "CMID", "name": "CatMapper ID"}], "limit": 3}

    monkeypatch.setattr(reconciliation_routes, "propose_properties", fake_propose_properties)

    response = client.get("/reconcile/ArchaMap/properties?type=CERAMICS&limit=3")

    assert response.status_code == 200
    assert response.get_json()["properties"] == [{"id": "CMID", "name": "CatMapper ID"}]


def test_reconciliation_extend_on_root_and_alias(client, monkeypatch):
    captured = []

    def fake_build_data_extension_response(database, extension_query):
        captured.append((database, extension_query))
        return {
            "meta": [{"id": "Name", "name": "Name"}],
            "rows": {"SM1": {"Name": [{"str": "Aymara"}]}},
        }

    monkeypatch.setattr(reconciliation_routes, "build_data_extension_response", fake_build_data_extension_response)
    payload = {"ids": ["SM1"], "properties": [{"id": "Name"}]}

    root = client.post("/reconcile/SocioMap", data={"extend": json.dumps(payload)})
    alias = client.post("/reconcile/SocioMap/extend", json=payload)

    assert root.status_code == 200
    assert alias.status_code == 200
    assert captured == [("SocioMap", payload), ("SocioMap", payload)]
    assert root.get_json()["rows"]["SM1"]["Name"] == [{"str": "Aymara"}]


def test_reconciliation_cors_preflight_allows_openrefine_testbench(client):
    response = client.open(
        "/reconcile/SocioMap",
        method="OPTIONS",
        headers={
            "Origin": "https://reconciliation-api.github.io",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code in (200, 204)
    assert response.headers.get("Access-Control-Allow-Origin") == "https://reconciliation-api.github.io"


def test_reconcile_query_batch_maps_search_rows(monkeypatch):
    def fake_search(**kwargs):
        assert kwargs["database"] == "SocioMap"
        assert kwargs["term"] == "Aymara"
        assert kwargs["property"] == "Name"
        assert kwargs["domain"] == "ETHNICITY"
        assert kwargs["limit"] == 3
        return {
            "data": [{
                "CMID": "SM1",
                "CMName": "Aymara",
                "domain": ["ETHNICITY"],
                "country": ["Bolivia"],
                "matching": "Aymara",
                "matchingDistance": 0,
            }]
        }

    monkeypatch.setattr(reconciliation, "search", fake_search)

    result = reconciliation.reconcile_query_batch(
        "SocioMap",
        {"q0": {"query": "Aymara", "type": "ETHNICITY", "limit": 3}},
    )

    candidate = result["q0"]["result"][0]
    assert candidate["id"] == "SM1"
    assert candidate["score"] == 100
    assert candidate["match"] is True
    assert candidate["type"] == [{"id": "ETHNICITY", "name": "Ethnicity"}]


def test_data_extension_response_encodes_values(monkeypatch):
    monkeypatch.setattr(reconciliation, "getDriver", lambda database: object())
    monkeypatch.setattr(
        reconciliation,
        "get_reconciliation_properties",
        lambda database: [
            {"id": "Name", "name": "Name"},
            {"id": "domain", "name": "Domain"},
            {"id": "dataset", "name": "Dataset"},
        ],
    )

    def fake_get_query(query, driver=None, params=None, type="dict", **kwargs):
        assert params == {"ids": ["SM1"]}
        return [{
            "requestedId": "SM1",
            "CMID": "SM1",
            "CMName": "Aymara",
            "labels": ["CATEGORY", "ETHNICITY"],
            "properties": {"population": 12},
            "country": ["Bolivia"],
            "Key": [],
            "dataset": [{"id": "SD1", "name": "Example Dataset"}],
        }]

    monkeypatch.setattr(reconciliation, "getQuery", fake_get_query)

    result = reconciliation.build_data_extension_response(
        "SocioMap",
        {"ids": ["SM1"], "properties": [{"id": "Name"}, {"id": "domain"}, {"id": "dataset"}]},
    )

    assert result["meta"] == [
        {"id": "Name", "name": "Name"},
        {"id": "domain", "name": "Domain"},
        {"id": "dataset", "name": "Dataset"},
    ]
    assert result["rows"]["SM1"]["Name"] == [{"str": "Aymara"}]
    assert result["rows"]["SM1"]["domain"] == [{"str": "ETHNICITY"}]
    assert result["rows"]["SM1"]["dataset"] == [{"id": "SD1", "name": "Example Dataset"}]


def test_reconciliation_rejects_large_batches(client):
    queries = {f"q{i}": {"query": str(i)} for i in range(reconciliation.RECONCILIATION_BATCH_SIZE + 1)}

    response = client.get("/reconcile/SocioMap", query_string={"queries": json.dumps(queries)})

    assert response.status_code == 413
    assert "batch exceeds" in response.get_json()["error"]
