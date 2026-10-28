from contextlib import nullcontext
import pytest
from CM import merge_references, event_types

@pytest.fixture(autouse=True)
def mock_transaction_for_query_stubs(monkeypatch):
    # These tests exercise merge validation using existing query stubs.
    # Transaction commit/rollback is covered separately with a transactional driver.
    monkeypatch.setattr(admin_module, "query_transaction", lambda *args: nullcontext())
    monkeypatch.setattr(merge_references, "getQuery", lambda *args, **kwargs: admin_module.getQuery(*args, **kwargs))
    monkeypatch.setattr(event_types, "getQuery", lambda *args, **kwargs: [{"allowedValues": ["HIERARCHY", "SPLIT", "FOLLOWS"]}])

import CM.admin as admin_module
import CMroutes.admin_routes as admin_routes


def test_merge_nodes_rejects_mismatched_primary_domains(monkeypatch):
    monkeypatch.setattr(admin_module, "getQuery", lambda *args, **kwargs: [])
    monkeypatch.setattr(admin_module, "getDriver", lambda _database: object())
    monkeypatch.setattr(admin_module, "isValidCMID", lambda _cmid, _driver: ["ok"])

    summaries = {
        "AM1": {"CMID": "AM1", "CMName": "District Node", "primaryDomain": "AREA"},
        "AM2": {"CMID": "AM2", "CMName": "Ethnicity Node", "primaryDomain": "ETHNICITY"},
    }
    monkeypatch.setattr(admin_module, "getNodeMergeSummary", lambda cmid, _driver: summaries[cmid])

    result = admin_module.mergeNodes("AM1", "AM2", "admin_user", "ArchaMap")

    assert isinstance(result, tuple)
    assert result[1] == 500
    assert "Primary domain mismatch" in result[0]


def test_admin_node_summary_returns_cmid_name_and_primary_domain(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(
        admin_routes,
        "getNodeMergeSummary",
        lambda cmid, _driver: {
            "CMID": cmid,
            "CMName": "Example Node",
            "labels": ["CATEGORY", "ADM1"],
            "primaryDomain": "AREA",
        },
    )

    response = client.get(
        "/admin/nodeSummary",
        query_string={"database": "ArchaMap", "CMID": "AM123"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["CMID"] == "AM123"
    assert payload["CMName"] == "Example Node"
    assert payload["primaryDomain"] == "AREA"


def test_resolve_primary_domain_accepts_variable_label(monkeypatch):
    monkeypatch.setattr(
        admin_module,
        "getQuery",
        lambda query, driver, params=None, type=None: [{"label": "VARIABLE", "groupLabel": None}],
    )

    result = admin_module._resolve_primary_domain_from_labels(
        ["VARIABLE"],
        driver=object(),
    )

    assert result == "VARIABLE"


def test_merge_nodes_uses_variable_label_for_variable_domain(monkeypatch):
    monkeypatch.setattr(admin_module, "getDriver", lambda _database: object())
    monkeypatch.setattr(admin_module, "isValidCMID", lambda _cmid, _driver: ["ok"])
    monkeypatch.setattr(
        admin_module,
        "getNodeMergeSummary",
        lambda cmid, _driver: {
            "CMID": cmid,
            "CMName": f"Node {cmid}",
            "primaryDomain": "VARIABLE",
        },
    )
    monkeypatch.setattr(admin_module, "addCMNameRel", lambda database, cmid: f"ok:{cmid}")
    monkeypatch.setattr(admin_module, "replaceProperty", lambda **kwargs: None)
    monkeypatch.setattr(admin_module, "createLog", lambda **kwargs: None)

    monkeypatch.setattr(admin_module, "processUSES", lambda **kwargs: "processed")

    queries = []

    def fake_get_query(query, driver, params=None, type=None, **kwargs):
        queries.append(query)
        if "return elementId(r) as relID" in query:
            return []
        if "return c.CMID as cmid" in query:
            return []
        if "RETURN m.CMName AS property" in query:
            return []
        if "CALL apoc.refactor.mergeNodes" in query:
            assert "match (a:VARIABLE {CMID: $keepcmid})" in query
            assert "match (b:VARIABLE {CMID: $deletecmid})" in query
            return ["AM1"]
        if "create (del:DELETED" in query:
            return ["deleted-node-id"]
        return []

    monkeypatch.setattr(admin_module, "getQuery", fake_get_query)

    result = admin_module.mergeNodes("AM1", "AM2", "admin_user", "ArchaMap")

    assert isinstance(result, list)
    assert any("Started Combining AM2 into AM1" in str(item) for item in result)


def test_merge_nodes_allows_variable_domain_when_isvalidcmid_would_reject(monkeypatch):
    monkeypatch.setattr(admin_module, "getDriver", lambda _database: object())
    monkeypatch.setattr(admin_module, "isValidCMID", lambda _cmid, _driver: [])
    monkeypatch.setattr(
        admin_module,
        "getNodeMergeSummary",
        lambda cmid, _driver: {
            "CMID": cmid,
            "CMName": f"Node {cmid}",
            "primaryDomain": "VARIABLE",
        },
    )
    monkeypatch.setattr(admin_module, "addCMNameRel", lambda database, cmid: f"ok:{cmid}")
    monkeypatch.setattr(admin_module, "replaceProperty", lambda **kwargs: None)
    monkeypatch.setattr(admin_module, "createLog", lambda **kwargs: None)
    monkeypatch.setattr(admin_module, "processUSES", lambda **kwargs: None)

    def fake_get_query(query, driver, params=None, type=None, **kwargs):
        if "return elementId(r) as relID" in query:
            return []
        if "return c.CMID as cmid" in query:
            return []
        if "RETURN m.CMName AS property" in query:
            return []
        if "CALL apoc.refactor.mergeNodes" in query:
            assert "match (a:VARIABLE {CMID: $keepcmid})" in query
            assert "match (b:VARIABLE {CMID: $deletecmid})" in query
            return ["AM1"]
        if "create (del:DELETED" in query:
            return ["deleted-node-id"]
        if "return elementId(n) as id" in query:
            return ["node-id"]
        return []

    monkeypatch.setattr(admin_module, "getQuery", fake_get_query)

    result = admin_module.mergeNodes("AM1", "AM2", "admin_user", "ArchaMap")

    assert isinstance(result, list)
    assert any("Completed combining AM2 into AM1" in str(item) for item in result)


def test_replace_parent_context_updates_only_exact_parent_ids():
    import json

    original = [
        '{"parent": "AM2", "eventType": "SPLIT", "eventDate": "2008", "note": "AM2"}',
        '{"parent":"AM20","eventType":"HIERARCHY"}',
    ]
    result = admin_module._replace_parent_context_cmid(original, "AM2", "AM1")
    assert json.loads(result[0]) == {
        "parent": "AM1", "eventType": "SPLIT", "eventDate": "2008", "note": "AM2",
    }
    assert result[1] == original[1]
    assert json.loads(original[0])["parent"] == "AM2"
    assert admin_module._replace_parent_context_cmid(original[0], "AM2", "AM1") == result[0]
    assert admin_module._replace_parent_context_cmid(
        {"parent": "AM2", "eventType": "FOLLOWS"}, "AM2", "AM1",
    ) == {"parent": "AM1", "eventType": "FOLLOWS"}


def test_merge_rewrites_context_only_references_before_dropping_node(monkeypatch):
    import json

    monkeypatch.setattr(admin_module, "getDriver", lambda _: object())
    monkeypatch.setattr(admin_module, "getNodeMergeSummary", lambda cmid, _: {
        "CMID": cmid, "CMName": cmid, "primaryDomain": "AREA",
    })
    monkeypatch.setattr(admin_module, "addCMNameRel", lambda *args: None)
    monkeypatch.setattr(admin_module, "createLog", lambda **kwargs: None)
    monkeypatch.setattr(admin_module, "processUSES", lambda **kwargs: None)
    context = ['{"parent":"AM2","eventType":"HIERARCHY"}']
    rewritten = []

    def fake_get_query(query, driver, params=None, type=None, **kwargs):
        if "RETURN m.CMName AS property" in query:
            return ["parent"]
        if "properties(r) AS properties" in query:
            assert params["discard"] == "AM2"
            assert "parentContext" in params["contextProps"]
            return [{"relID": "context-only-ref", "CMID": "AM99", "datasetID": "AD1",
                     "Key": "Key == 1", "properties": {"parentContext": context}}]
        if "UNWIND $updates AS update" in query:
            rewritten.extend(params["updates"])
        if "CALL apoc.refactor.mergeNodes" in query:
            assert json.loads(rewritten[0]["properties"]["parentContext"][0])["parent"] == "AM1"
            return ["AM1"]
        return []

    monkeypatch.setattr(admin_module, "getQuery", fake_get_query)
    result = admin_module.mergeNodes("AM1", "AM2", "admin", "ArchaMap")
    assert isinstance(result, list), result
    assert rewritten[0]["relID"] == "context-only-ref"
    assert json.loads(rewritten[0]["properties"]["parentContext"][0])["eventType"] == "HIERARCHY"


def test_merge_stops_if_context_replacement_fails(monkeypatch):
    monkeypatch.setattr(admin_module, "getDriver", lambda _: object())
    monkeypatch.setattr(admin_module, "getNodeMergeSummary", lambda cmid, _: {
        "CMID": cmid, "CMName": cmid, "primaryDomain": "AREA",
    })
    monkeypatch.setattr(admin_module, "addCMNameRel", lambda *args: None)


    def fake_get_query(query, driver, **kwargs):
        assert "CALL apoc.refactor.mergeNodes" not in query
        if "properties(r) AS properties" in query:
            return [{"relID": "bad", "CMID": "AM99", "datasetID": "AD1", "Key": "Key == 1",
                     "properties": {"parentContext": ["{bad JSON}"]}}]
        return []

    monkeypatch.setattr(admin_module, "getQuery", fake_get_query)
    result = admin_module.mergeNodes("AM1", "AM2", "admin", "ArchaMap")
    assert result[1] == 500
    assert "Invalid parentContext JSON" in result[0]


def test_parent_context_decode_failure_prevents_partial_replacement(monkeypatch):
    monkeypatch.setattr(admin_module, "getDriver", lambda _: object())

    def fake_get_query(query, driver, **kwargs):
        assert "SET r.parentContext" not in query
        return [
            {"relID": "valid", "CMID": "AM99", "value": ['{"parent":"AM2"}']},
            {"relID": "invalid", "CMID": "AM99", "value": ["{bad JSON}"]},
        ]

    monkeypatch.setattr(admin_module, "getQuery", fake_get_query)
    result = admin_module.replaceProperty(["AM99"], "parentContext", "AM2", "AM1", "ArchaMap")
    assert result[1] == 500
