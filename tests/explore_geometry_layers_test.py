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
    assert payload["limits"]["pointLimit"] == 5000
    assert payload["limits"]["polygonLimit"] == 2500
    assert payload["limits"]["featureLimit"] is None


def test_map_feature_limits_default_points_and_polygons_independently():
    points = [{"id": index} for index in range(explore.DEFAULT_MAP_POINT_LIMIT + 1)]
    polygons = [{"id": index} for index in range(explore.DEFAULT_MAP_POLYGON_LIMIT + 1)]

    limited_points, limited_polygons, truncated_points, truncated_polygons = explore._limit_map_features(
        points,
        polygons,
        explore.DEFAULT_MAP_POINT_LIMIT,
        explore.DEFAULT_MAP_POLYGON_LIMIT,
    )

    assert len(limited_points) == 5000
    assert len(limited_polygons) == 2500
    assert truncated_points == 1
    assert truncated_polygons == 1


def test_map_feature_limit_param_preserves_legacy_combined_cap():
    points = [{"id": index} for index in range(4)]
    polygons = [{"id": index} for index in range(4)]

    limited_points, limited_polygons, truncated_points, truncated_polygons = explore._limit_map_features(
        points,
        polygons,
        explore.DEFAULT_MAP_POINT_LIMIT,
        explore.DEFAULT_MAP_POLYGON_LIMIT,
        feature_limit=5,
    )

    assert len(limited_points) == 4
    assert len(limited_polygons) == 1
    assert truncated_points == 0
    assert truncated_polygons == 3


def test_descendant_candidate_limit_is_applied_after_depth_order(monkeypatch):
    captured = {}

    def fake_get_query(query, driver, params=None, **kwargs):
        captured["query"] = " ".join(query.split())
        captured["params"] = params
        return []

    monkeypatch.setattr(explore, "getQuery", fake_get_query)

    assert explore._get_descendant_map_nodes(object(), "SM1", 7, 5000) == []

    query = captured["query"]
    selection_order = query.index("ORDER BY depth, CMName, descendant.CMID")
    selection_limit = query.index("LIMIT $node_limit")
    result_projection = query.index("RETURN descendant.CMID AS CMID")

    assert selection_order < selection_limit < result_projection
    assert "WHERE length(candidatePath) = depth" in query
    assert captured["params"] == {"cmid": "SM1", "node_limit": 5000}


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
        "_get_related_map_node_counts",
        lambda neo4j_driver, cmid, relationships: {"AREA_OF": 5},
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
    assert related_layer["nodeLimited"] is True
    assert related_layer["displayedNodeCount"] == 1
    assert related_layer["totalNodeCount"] == 5


def test_explore_geometry_dataset_uses_category_layer_adds_provenance(monkeypatch):
    driver = object()

    monkeypatch.setattr(explore, "getDriver", lambda database: driver)
    monkeypatch.setattr(
        explore,
        "_get_dataset_used_category_nodes",
        lambda neo4j_driver, cmid, node_limit: [
            {
                "CMID": "AM2",
                "CMName": "District A",
                "relationship": "USES",
                "path": ["AD1", "AM2"],
            }
        ],
    )
    monkeypatch.setattr(
        explore,
        "_get_dataset_used_category_count",
        lambda neo4j_driver, cmid: 4,
    )
    requested = {}

    def exact_uses_points(neo4j_driver, dataset_cmid, cmids):
        requested["dataset_cmid"] = dataset_cmid
        requested["cmids"] = cmids
        return [
            {
                "geometry": json.dumps({"type": "Point", "coordinates": [30, 40]}),
                "source": "Requested Dataset",
                "sourceNodeCMID": "AM2",
                "sourceNodeName": "District A",
            }
        ]

    monkeypatch.setattr(explore, "_get_dataset_uses_points", exact_uses_points)
    monkeypatch.setattr(
        explore,
        "_get_dataset_uses_polygons",
        lambda neo4j_driver, dataset_cmid, cmids: [],
    )

    payload = explore.exploreGeometry("ArchaMap", "AD1", layers="uses")

    assert payload["points"] == []
    assert payload["polygons"] == []
    uses_layer = payload["maplayers"][0]
    assert uses_layer["id"] == "uses:CATEGORY"
    assert uses_layer["mode"] == "uses"
    assert uses_layer["relationship"] == "USES"
    assert uses_layer["pointCount"] == 1
    assert uses_layer["points"][0]["cood"] == [30, 40]
    assert uses_layer["points"][0]["CMID"] == "AM2"
    assert uses_layer["points"][0]["CMName"] == "District A"
    assert uses_layer["points"][0]["source"] == "Requested Dataset"
    assert uses_layer["points"][0]["inherited"] is True
    assert uses_layer["points"][0]["inheritedFromCMID"] == "AM2"
    assert uses_layer["points"][0]["inheritedFromName"] == "District A"
    assert uses_layer["points"][0]["inheritanceRelationship"] == "USES"
    assert uses_layer["nodeLimited"] is True
    assert uses_layer["displayedNodeCount"] == 1
    assert uses_layer["totalNodeCount"] == 4
    assert requested == {"dataset_cmid": "AD1", "cmids": ["AM2"]}


def test_dataset_uses_geometry_queries_are_scoped_to_the_root_dataset(monkeypatch):
    graph_driver = object()
    gis_driver = object()
    graph_queries = []

    def fake_get_query(query, driver, params=None, **kwargs):
        if driver is graph_driver:
            graph_queries.append((query, params))
            assert "(d:DATASET {CMID: $dataset_cmid})-[r:USES]->(c:CATEGORY)" in query
            assert params == {"dataset_cmid": "AD1", "category_cmids": ["AM2"]}
            if "r.geoCoords" in query:
                return [{
                    "geometry": json.dumps({"type": "Point", "coordinates": [30, 40]}),
                    "source": "Requested Dataset",
                    "sourceNodeCMID": "AM2",
                    "sourceNodeName": "District A",
                }]
            return [{
                "geomID": "geom-1",
                "source": "Requested Dataset",
                "sourceNodeCMID": "AM2",
                "sourceNodeName": "District A",
                "sourceNodeLabels": ["CATEGORY", "DISTRICT"],
            }]
        assert driver is gis_driver
        return [{
            "geometry": json.dumps({"type": "Polygon", "coordinates": []}),
            "source": "Requested Dataset",
            "sourceNodeCMID": "AM2",
            "sourceNodeName": "District A",
        }]

    monkeypatch.setattr(explore, "getQuery", fake_get_query)
    monkeypatch.setattr(explore, "getDriver", lambda database: gis_driver)

    points = explore._get_dataset_uses_points(graph_driver, "AD1", ["AM2"])
    polygons = explore._get_dataset_uses_polygons(graph_driver, "AD1", ["AM2"])

    assert points[0]["sourceNodeCMID"] == "AM2"
    assert polygons[0]["sourceNodeCMID"] == "AM2"
    assert len(graph_queries) == 2


def test_map_layer_options_summarize_direct_related_and_descendant(monkeypatch):
    driver = object()
    summary_depths = []

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
        "_get_dataset_used_category_nodes",
        lambda neo4j_driver, cmid, node_limit: [],
    )
    monkeypatch.setattr(
        explore,
        "_get_dataset_used_category_count",
        lambda neo4j_driver, cmid: 0,
    )
    monkeypatch.setattr(
        explore,
        "_get_related_map_node_counts",
        lambda neo4j_driver, cmid, relationships: {"AREA_OF": 3},
    )
    monkeypatch.setattr(
        explore,
        "_get_descendant_map_nodes",
        lambda neo4j_driver, cmid, max_depth, node_limit: [
            {"CMID": "AM3", "CMName": "Language A", "relationship": "CONTAINS", "depth": 1}
        ],
    )
    def fake_descendant_summary(neo4j_driver, cmid, max_depth):
        summary_depths.append(max_depth)
        return {
            "totalNodeCount": 4,
            "depthCounts": [{"depth": 1, "nodeCount": 2}, {"depth": 2, "nodeCount": 2}],
        }

    monkeypatch.setattr(explore, "_get_descendant_map_node_summary", fake_descendant_summary)

    payload = explore.getMapLayerOptions("ArchaMap", "AM1")
    layers = {layer["id"]: layer for layer in payload["layers"]}

    assert layers["direct"]["available"] is True
    assert layers["related:AREA_OF"]["available"] is True
    assert layers["related:AREA_OF"]["polygonCount"] == 2
    assert layers["related:AREA_OF"]["displayedNodeCount"] == 1
    assert layers["related:AREA_OF"]["totalNodeCount"] == 3
    assert layers["related:AREA_OF"]["nodeLimited"] is True
    assert layers["descendants:CONTAINS"]["available"] is True
    assert layers["descendants:CONTAINS"]["pointCount"] == 3
    assert layers["descendants:CONTAINS"]["displayedNodeCount"] == 1
    assert layers["descendants:CONTAINS"]["totalNodeCount"] == 4
    assert layers["descendants:CONTAINS"]["depthCounts"] == [
        {"depth": 1, "nodeCount": 2},
        {"depth": 2, "nodeCount": 2},
    ]
    assert layers["descendants:CONTAINS"]["availableDepth"] == 2
    assert payload["limits"]["maxDepth"] == 30
    assert payload["limits"]["availableDescendantDepth"] == 2
    assert payload["limits"]["defaultDepth"] == 2
    assert payload["limits"]["maxNodes"] == 5000
    assert payload["limits"]["defaultNodeLimit"] == 5000
    assert payload["limits"]["defaultPointLimit"] == 5000
    assert payload["limits"]["defaultPolygonLimit"] == 2500
    assert summary_depths == [explore.MAX_MAP_DESCENDANT_DEPTH]


def test_map_layer_options_summarize_dataset_used_categories(monkeypatch):
    driver = object()

    monkeypatch.setattr(explore, "getDriver", lambda database: driver)
    monkeypatch.setattr(
        explore,
        "_get_geometry_counts_for_cmids",
        lambda neo4j_driver, cmids: {
            "AD1": {"pointCount": 0, "polygonCount": 0},
            "AM2": {"pointCount": 1, "polygonCount": 1},
            "AM3": {"pointCount": 2, "polygonCount": 0},
        },
    )
    monkeypatch.setattr(
        explore,
        "_get_dataset_used_category_nodes",
        lambda neo4j_driver, cmid, node_limit: [
            {"CMID": "AM2", "CMName": "District A", "relationship": "USES"},
            {"CMID": "AM3", "CMName": "District B", "relationship": "USES"},
        ],
    )
    monkeypatch.setattr(
        explore,
        "_get_dataset_used_category_count",
        lambda neo4j_driver, cmid: 5,
    )
    monkeypatch.setattr(
        explore,
        "_get_dataset_uses_geometry_counts",
        lambda neo4j_driver, dataset_cmid, cmids: {
            "AM2": {"pointCount": 1, "polygonCount": 1},
            "AM3": {"pointCount": 2, "polygonCount": 0},
        },
    )
    monkeypatch.setattr(
        explore,
        "_get_related_map_nodes",
        lambda neo4j_driver, cmid, relationships, node_limit: [],
    )
    monkeypatch.setattr(
        explore,
        "_get_related_map_node_counts",
        lambda neo4j_driver, cmid, relationships: {},
    )
    monkeypatch.setattr(
        explore,
        "_get_descendant_map_nodes",
        lambda neo4j_driver, cmid, max_depth, node_limit: [],
    )
    monkeypatch.setattr(
        explore,
        "_get_descendant_map_node_summary",
        lambda neo4j_driver, cmid, max_depth: {"totalNodeCount": 0, "depthCounts": []},
    )

    payload = explore.getMapLayerOptions("ArchaMap", "AD1")
    layers = {layer["id"]: layer for layer in payload["layers"]}

    assert layers["uses:CATEGORY"]["available"] is True
    assert layers["uses:CATEGORY"]["label"] == "USES category locations"
    assert layers["uses:CATEGORY"]["pointCount"] == 3
    assert layers["uses:CATEGORY"]["polygonCount"] == 1
    assert layers["uses:CATEGORY"]["displayedNodeCount"] == 2
    assert layers["uses:CATEGORY"]["totalNodeCount"] == 5
    assert layers["uses:CATEGORY"]["nodeLimited"] is True


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


def test_process_polygons_propagates_source_to_nested_feature_collection():
    nested_geometry = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                },
            }
        ],
    }

    polygons, sources = explore._process_polygons(
        [
            {
                "geometry": json.dumps(nested_geometry),
                "source": "GADM3.6",
                "inherited": True,
                "inheritedFromCMID": "SM2142",
                "inheritedFromName": "Manitoba",
            }
        ],
        preserve_metadata=True,
    )

    nested_feature = polygons[0]["features"][0]
    assert sources == ["GADM3.6"]
    assert nested_feature["source"] == "GADM3.6"
    assert nested_feature["properties"]["source"] == "GADM3.6"
    assert nested_feature["properties"]["inheritedFromCMID"] == "SM2142"


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
