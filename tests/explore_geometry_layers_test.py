import json

import CM.explore as explore
import CMroutes.explore_routes as explore_routes


def test_explore_geometry_defaults_to_direct_layer(monkeypatch):
    driver = object()

    monkeypatch.setattr(explore, "getDriver", lambda database: driver)
    monkeypatch.setattr(explore, "getPolygon", lambda cmid, neo4j_driver: [])
    monkeypatch.setattr(
        explore,
        "getPoints",
        lambda cmid, neo4j_driver: [
            {
                "geometry": json.dumps({"type": "Point", "coordinates": [10, 20]}),
                "source": "Direct Dataset",
                "Key": "direct-key",
            }
        ],
    )
    monkeypatch.setattr(explore, "getDatasetPoints", lambda cmid, neo4j_driver: [])

    payload = explore.exploreGeometry("ArchaMap", "AM1")

    assert payload["points"] == [{"cood": [10, 20], "source": "Direct Dataset"}]
    assert payload["polygons"] == []
    assert payload["datasetpoints"] == []
    assert payload["maplayers"][0]["id"] == "direct"
    assert payload["maplayers"][0]["mode"] == "direct"


def test_explore_geometry_related_layer_adds_provenance_without_direct(monkeypatch):
    driver = object()

    monkeypatch.setattr(explore, "getDriver", lambda database: driver)
    monkeypatch.setattr(
        explore,
        "_get_related_map_nodes",
        lambda neo4j_driver, cmid, relationships, node_limit: [
            {
                "CMID": "AM2",
                "CMName": "District A",
                "relationship": "AREA_OF",
                "path": ["AM1", "AM2"],
            }
        ],
    )
    monkeypatch.setattr(
        explore,
        "_get_points_for_cmids",
        lambda neo4j_driver, cmids: [
            {
                "geometry": json.dumps({"type": "Point", "coordinates": [30, 40]}),
                "source": "District Dataset",
                "sourceNodeCMID": "AM2",
                "sourceNodeName": "District A",
            }
        ],
    )
    monkeypatch.setattr(explore, "_get_polygons_for_cmids", lambda neo4j_driver, cmids: [])

    payload = explore.exploreGeometry(
        "ArchaMap",
        "AM1",
        layers="related",
        relations="AREA_OF",
    )

    assert payload["points"] == []
    assert payload["polygons"] == []
    related_layer = payload["maplayers"][0]
    assert related_layer["id"] == "related:AREA_OF"
    assert related_layer["pointCount"] == 1
    assert related_layer["points"][0]["cood"] == [30, 40]
    assert related_layer["points"][0]["inherited"] is True
    assert related_layer["points"][0]["inheritedFromCMID"] == "AM2"
    assert related_layer["points"][0]["inheritedFromName"] == "District A"
    assert related_layer["points"][0]["inheritanceRelationship"] == "AREA_OF"


def test_map_layer_options_summarize_direct_related_and_descendant(monkeypatch):
    driver = object()

    monkeypatch.setattr(explore, "getDriver", lambda database: driver)
    monkeypatch.setattr(
        explore,
        "_get_geometry_counts_for_cmids",
        lambda neo4j_driver, cmids: {
            "AM1": {"pointCount": 1, "polygonCount": 0},
            "AM2": {"pointCount": 0, "polygonCount": 2},
            "AM3": {"pointCount": 3, "polygonCount": 0},
        },
    )
    monkeypatch.setattr(
        explore,
        "_get_related_map_nodes",
        lambda neo4j_driver, cmid, relationships, node_limit: [
            {"CMID": "AM2", "CMName": "District A", "relationship": "AREA_OF"}
        ],
    )
    monkeypatch.setattr(
        explore,
        "_get_descendant_map_nodes",
        lambda neo4j_driver, cmid, max_depth, node_limit: [
            {"CMID": "AM3", "CMName": "Language A", "relationship": "CONTAINS", "depth": 1}
        ],
    )

    payload = explore.getMapLayerOptions("ArchaMap", "AM1")
    layers = {layer["id"]: layer for layer in payload["layers"]}

    assert layers["direct"]["available"] is True
    assert layers["related:AREA_OF"]["available"] is True
    assert layers["related:AREA_OF"]["polygonCount"] == 2
    assert layers["descendants:CONTAINS"]["available"] is True
    assert layers["descendants:CONTAINS"]["pointCount"] == 3


def test_geometry_counts_only_include_polygons_present_in_gisdb(monkeypatch):
    graph_driver = object()
    gis_driver = object()

    def fake_get_query(query, driver, params=None, **kwargs):
        if "pointRel.geoCoords" in query:
            return [
                {"CMID": "AM1", "pointCount": 1},
                {"CMID": "AM2", "pointCount": 0},
            ]
        if "polyRel.geoPolygon AS geomID" in query:
            return [
                {"CMID": "AM1", "geomID": "missing-geom"},
                {"CMID": "AM2", "geomID": ["present-geom", "other-missing-geom"]},
            ]
        if "MATCH (g:GEOMETRY)" in query:
            assert driver is gis_driver
            return [{"geomID": "present-geom"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(explore, "getQuery", fake_get_query)
    monkeypatch.setattr(explore, "getDriver", lambda database: gis_driver)

    counts = explore._get_geometry_counts_for_cmids(graph_driver, ["AM1", "AM2"])

    assert counts["AM1"] == {"pointCount": 1, "polygonCount": 0}
    assert counts["AM2"] == {"pointCount": 0, "polygonCount": 1}


def test_map_layer_options_route(client, monkeypatch):
    monkeypatch.setattr(
        explore_routes,
        "getMapLayerOptions",
        lambda database, cmid, max_depth=None, node_limit=None: {
            "database": database,
            "cmid": cmid,
            "layers": [{"id": "direct", "available": False}],
        },
    )

    response = client.get("/api/databases/ArchaMap/nodes/AM1/map-layer-options")

    assert response.status_code == 200
    assert response.get_json()["layers"] == [{"id": "direct", "available": False}]
