import CMroutes.explore_routes as explore_routes


def test_map_layer_options_exposes_descendant_contains_when_geometry_exists(client, monkeypatch):
    monkeypatch.setattr(explore_routes, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, type=None, **kwargs):
        assert kwargs["cmid"] == "SM227020"
        return [{
            "nodeCount": 2,
            "availableDescendantDepth": 2,
            "depthRows": [{"depth": 1, "count": 1}, {"depth": 2, "count": 1}],
        }]

    monkeypatch.setattr(explore_routes, "getQuery", fake_get_query)

    response = client.get("/databases/sociomap/nodes/SM227020/map-layer-options")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["limits"]["availableDescendantDepth"] == 2
    assert payload["layers"][0]["id"] == "descendants:CONTAINS"
    assert payload["layers"][0]["available"] is True
    assert payload["layers"][0]["depthCounts"] == [
        {"depth": 1, "count": 1},
        {"depth": 2, "count": 1},
    ]


def test_inherited_explore_geometry_returns_descendant_point_identity(client, monkeypatch):
    monkeypatch.setattr(explore_routes, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, type=None, **kwargs):
        if "RETURN descendant.CMID AS CMID" in query:
            return [{"CMID": "SM1", "CMName": "Child", "depth": 1}]
        if "RETURN" in query and "nodeCount" in query:
            return [{
                "nodeCount": 1,
                "availableDescendantDepth": 1,
                "depthRows": [{"depth": 1, "count": 1}],
            }]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(explore_routes, "getQuery", fake_get_query)
    monkeypatch.setattr(
        explore_routes,
        "exploreGeometry",
        lambda database, cmid: {
            "points": [{"cood": [1, 2], "source": "Dataset A"}],
            "polygons": [],
        },
    )

    response = client.get(
        "/databases/sociomap/nodes/SM227020/explore-geometry",
        query_string={"layers": "descendants", "maxDepth": "1"},
    )

    assert response.status_code == 200
    layer = response.get_json()["maplayers"][0]
    assert layer["id"] == "descendants:CONTAINS"
    assert layer["points"][0]["sourceNodeName"] == "Child"
    assert layer["points"][0]["sourceNodeCMID"] == "SM1"
    assert layer["points"][0]["inheritanceRelationship"] == "CONTAINS"
