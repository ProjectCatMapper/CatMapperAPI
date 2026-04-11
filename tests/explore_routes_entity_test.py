import CMroutes.explore_routes as explore_routes


def test_entity_json_aggregates_node_page_payload(client, monkeypatch):
    info_payload = {
        "CMID": "AM1",
        "CMName": "Root Node",
        "Domains": ["CATEGORY", "MERGING"],
    }
    category_payload = {
        "samples": [{"Name": "Example"}],
        "categories": [{"Domain": "SITE", "Count": 3}],
        "childcategories": [{"CMID": "AM2"}],
        "relnames": ["USES", "MERGING"],
    }
    geometry_payload = {
        "polygons": {"features": []},
        "points": [{"lat": 1, "lng": 2}],
        "datasetpoints": [{"lat": 3, "lng": 4}],
    }
    network_payloads = {
        "USES": {"node": [{"id": "root"}], "relations": [], "relNodes": [], "params": [{"relation": "USES"}]},
        "MERGING": {"node": [{"id": "root"}], "relations": [{"type": "MERGING"}], "relNodes": [], "params": [{"relation": "MERGING"}]},
    }
    merge_summary_payload = {
        "nodeType": "MERGING",
        "stackSummary": [{"stackID": "S1"}],
        "stackSummaryTotals": {"datasetCount": 1},
        "datasetSummary": [],
        "mergingTemplateCount": 0,
        "mergingTies": [],
        "equivalenceTies": [],
    }

    monkeypatch.setattr(explore_routes, "getCategoryInfo", lambda database, cmid: info_payload)
    monkeypatch.setattr(explore_routes, "getCategoryPage", lambda database, cmid: category_payload)
    monkeypatch.setattr(explore_routes, "exploreGeometry", lambda database, cmid: geometry_payload)
    monkeypatch.setattr(
        explore_routes,
        "_get_networkjs_payload",
        lambda **kwargs: network_payloads[kwargs["relation"]],
    )

    import CMroutes.merge_routes as merge_routes

    monkeypatch.setattr(
        merge_routes,
        "build_merge_template_summary_payload",
        lambda database, cmid: merge_summary_payload,
    )

    response = client.get("/entity/ArchaMap/AM1.json")

    assert response.status_code == 200
    assert response.headers["Content-Disposition"] == 'inline; filename="ArchaMap_AM1.json"'

    payload = response.get_json()
    assert payload["version"] == "1.0"
    assert payload["resourceType"] == "nodeExplorePage"
    assert payload["database"] == "ArchaMap"
    assert payload["cmid"] == "AM1"
    assert payload["canonicalUrl"].endswith("/archamap/AM1")
    assert payload["info"] == info_payload
    assert payload["categoryPage"] == category_payload
    assert payload["geometry"] == geometry_payload
    assert payload["mergeTemplateSummary"] == merge_summary_payload
    assert payload["networks"] == network_payloads


def test_entity_json_returns_404_when_node_missing(client, monkeypatch):
    monkeypatch.setattr(explore_routes, "getCategoryInfo", lambda database, cmid: None)

    response = client.get("/entity/ArchaMap/AM404.json")

    assert response.status_code == 404
    assert response.get_json() == {"error": "Node not found"}
