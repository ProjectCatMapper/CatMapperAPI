import pandas as pd


def test_cors_preflight_allows_documented_api_key_header(client):
    response = client.open(
        "/profile/bookmarks/100", method="OPTIONS",
        headers={"Origin": "https://test.catmapper.org",
                 "Access-Control-Request-Method": "GET",
                 "Access-Control-Request-Headers": "x-api-key"},
    )
    assert response.status_code in (200, 204)
    assert "x-api-key" in response.headers.get("Access-Control-Allow-Headers", "").lower()


def test_merge_code_download_is_explicitly_unsupported(client):
    response = client.post("/api/merge/code-downloads", json={})
    assert response.status_code == 501
    assert response.get_json()["code"] == "merge_code_download_unavailable"


def test_metadata_node_returns_only_databases_containing_node(monkeypatch):
    from CMroutes import metadata_routes

    monkeypatch.setattr(metadata_routes, "getDriver", lambda name: name)
    monkeypatch.setattr(metadata_routes, "getQuery", lambda **kwargs:
                        [{"n": {"CMID": "SM1"}}] if kwargs["driver"] == "sociomap" else [])
    monkeypatch.setattr(metadata_routes, "serialize_node", lambda node: node)
    result = metadata_routes.getMetdataProperties("SM1")
    assert result == [{"SocioMap": {"CMID": "SM1"}}]


def test_metadata_node_returns_404_when_absent_from_both_databases(client, monkeypatch):
    from CMroutes import metadata_routes

    monkeypatch.setattr(metadata_routes, "getDriver", lambda name: name)
    monkeypatch.setattr(metadata_routes, "getQuery", lambda **kwargs: [])
    response = client.get("/api/metadata/nodes/SM404")
    assert response.status_code == 404
    assert response.get_json()["CMID"] == "SM404"


def test_advanced_download_category_query_has_no_trailing_comma(monkeypatch):
    from CM import download as module
    captured = []
    monkeypatch.setattr(module, "getDriver", lambda _: object())

    def fake_query(query, **kwargs):
        captured.append(query)
        if kwargs.get("type") == "df":
            return pd.DataFrame([{"CMID": "C1", "CMName": "x"}])
        if "not p.type = \"relationship\"" in query:
            return [{"property": "DatasetCitation", "type": "node"}]
        return []

    monkeypatch.setattr(module, "getQuery", fake_query)
    module.getAdvancedDownload("ArchaMap", "SITE", ["DatasetCitation"], ["C1"])
    category_query = next(query for query in captured if "match (c:CATEGORY)" in query)
    assert "return c.CMID as CMID, c.CMName as CMName, labels(c) as domains, apoc.text.join" in category_query
    assert "as datasets," not in category_query
