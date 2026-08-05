import CMroutes.explore_routes as explore_routes


def test_map_layer_options_supports_prefixed_and_unprefixed_routes(client, monkeypatch):
    calls = []

    def fake_options(database, cmid, max_depth=None, node_limit=None):
        calls.append((database, cmid, max_depth, node_limit))
        return {
            "database": database,
            "cmid": cmid,
            "layers": [{"id": "descendants:CONTAINS", "available": True}],
        }

    monkeypatch.setattr(explore_routes, "getMapLayerOptions", fake_options)

    for path in (
        "/databases/sociomap/nodes/SM227020/map-layer-options",
        "/api/databases/sociomap/nodes/SM227020/map-layer-options",
    ):
        response = client.get(path, query_string={"maxDepth": "3", "nodeLimit": "25"})
        assert response.status_code == 200
        assert response.get_json()["layers"][0]["id"] == "descendants:CONTAINS"

    assert calls == [
        ("sociomap", "SM227020", "3", "25"),
        ("sociomap", "SM227020", "3", "25"),
    ]


def test_explore_geometry_supports_prefixed_and_unprefixed_routes(client, monkeypatch):
    calls = []

    def fake_geometry(database, cmid, **kwargs):
        calls.append((database, cmid, kwargs))
        return {
            "maplayers": [{"id": "descendants:CONTAINS", "points": []}],
            "limits": {"maxDepth": 1},
        }

    monkeypatch.setattr(explore_routes, "exploreGeometry", fake_geometry)

    for path in (
        "/databases/sociomap/nodes/SM227020/explore-geometry",
        "/api/databases/sociomap/nodes/SM227020/explore-geometry",
    ):
        response = client.get(
            path,
            query_string={"layers": "descendants", "maxDepth": "1"},
        )
        assert response.status_code == 200
        assert response.get_json()["maplayers"][0]["id"] == "descendants:CONTAINS"

    assert len(calls) == 2
    assert all(call[0:2] == ("sociomap", "SM227020") for call in calls)
    assert all(call[2]["layers"] == "descendants" for call in calls)
    assert all(call[2]["max_depth"] == "1" for call in calls)
