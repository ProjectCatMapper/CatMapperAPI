import CMroutes.explore_routes as explore_routes


def test_networksjs_uses_only_returns_nodes_connected_to_kept_edges(client, monkeypatch):
    monkeypatch.setattr(explore_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(explore_routes, "serialize_node", lambda node: node)
    monkeypatch.setattr(explore_routes, "serialize_relationship", lambda relationship: relationship)
    monkeypatch.setattr(
        explore_routes,
        "flatten_json",
        lambda entry: next(iter(entry.values())) if isinstance(entry, dict) and len(entry) == 1 else entry,
    )
    monkeypatch.setattr(explore_routes, "_get_label_metadata_map", lambda driver: {})
    monkeypatch.setattr(explore_routes, "_apply_node_colors", lambda rows, label_metadata_map: None)

    root_node = {"id": "root-1", "labels": ["CATEGORY"], "CMID": "AM1", "CMName": "Root"}
    kept_dataset = {"id": "ds-1", "labels": ["DATASET"], "CMID": "AD1", "CMName": "Dataset One"}
    orphan_dataset = {"id": "ds-2", "labels": ["DATASET"], "CMID": "AD2", "CMName": "Dataset Two"}
    rel_one = {"start_node_id": "ds-1", "end_node_id": "root-1", "type": "USES"}
    rel_two = {"start_node_id": "ds-2", "end_node_id": "root-1", "type": "USES"}

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "r:USES" in query and "collect(distinct a)" in query:
            return [{"a": [root_node], "r": [rel_one, rel_two], "e": [kept_dataset, orphan_dataset]}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(explore_routes, "getQuery", fake_get_query)

    response = client.get(
        "/networksjs",
        query_string={"cmid": "AM1", "database": "archamap", "relation": "USES", "limit": 1},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["node"] == [root_node]
    assert payload["relations"] == [rel_one]
    assert payload["relNodes"] == [kept_dataset]


def test_networksjs_returns_empty_payload_when_no_matching_root(client, monkeypatch):
    monkeypatch.setattr(explore_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(explore_routes, "getQuery", lambda *args, **kwargs: [])

    response = client.get(
        "/networksjs",
        query_string={"cmid": "AM19082", "database": "archamap", "relation": "USES", "limit": 10},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["node"] == []
    assert payload["relations"] == []
    assert payload["relNodes"] == []
    assert payload["params"] == [{
        "cmid": ["AM19082"],
        "database": "archamap",
        "domain": [],
        "relation": "USES",
    }]


def test_networksjs_merging_includes_stack_to_dataset_edges(client, monkeypatch):
    monkeypatch.setattr(explore_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(explore_routes, "serialize_node", lambda node: node)
    monkeypatch.setattr(explore_routes, "serialize_relationship", lambda relationship: relationship)
    monkeypatch.setattr(
        explore_routes,
        "flatten_json",
        lambda entry: next(iter(entry.values())) if isinstance(entry, dict) and len(entry) == 1 else entry,
    )
    monkeypatch.setattr(explore_routes, "_get_label_metadata_map", lambda driver: {})
    monkeypatch.setattr(explore_routes, "_apply_node_colors", lambda rows, label_metadata_map: None)

    root_stack = {"id": "stack-1", "labels": ["DATASET", "STACK"], "CMID": "AD1002", "CMName": "Stack"}
    linked_dataset = {"id": "ds-1", "labels": ["DATASET"], "CMID": "AD1000", "CMName": "Dataset One"}
    stack_to_dataset = {"start_node_id": "stack-1", "end_node_id": "ds-1", "type": "MERGING"}

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        assert "OPTIONAL MATCH (a:STACK)-[r7:MERGING]->(d2:DATASET)" in query
        return [{"a": [root_stack], "r": [stack_to_dataset], "e": [linked_dataset]}]

    monkeypatch.setattr(explore_routes, "getQuery", fake_get_query)

    response = client.get(
        "/networksjs",
        query_string={"cmid": "AD1002", "database": "archamap", "relation": "MERGING", "limit": 25},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["node"] == [root_stack]
    assert payload["relations"] == [stack_to_dataset]
    assert payload["relNodes"] == [linked_dataset]
