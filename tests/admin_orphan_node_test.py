"""CATMA-326: typed legacy nodes need not have CATEGORY or USES to edit CMName."""
import pytest

from CM import admin


@pytest.mark.parametrize("cmid", ["AM181112", "AM181174", "AM181343", "SM181112"])
def test_rename_typed_orphan_preserves_labels_and_uses(monkeypatch, cmid):
    queries = []
    logs = []
    names = []
    monkeypatch.setattr(admin, "getDriver", lambda _: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda _: [])
    monkeypatch.setattr(admin, "createLog", lambda **kwargs: logs.append(kwargs))
    monkeypatch.setattr(admin, "addCMNameRel", lambda database, CMID: names.append(CMID))

    def get_query(query, driver=None, params=None, type=None):
        queries.append(query)
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["PERIOD"]}]
        if "MATCH (l:LABEL)" in query:
            assert params == {"labels": ["PERIOD"]}
            return [{"label": "PERIOD"}]
        if "RETURN a.CMName AS val" in query:
            return ["Original name"]
        if "SET a.CMName = $id" in query:
            assert params == {"changeNodeID": cmid, "id": "Corrected name"}
            return [{"nodeID": "orphan-node"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", get_query)
    result = admin.add_edit_delete_Node("archamap", "admin-user", {
        "s1_1": "edit", "s1_2": cmid, "s1_3": "Corrected name", "s1_7": "CMName",
    })
    assert result == "updated successfully"
    assert names == [cmid]
    assert logs[0]["id"] == ["orphan-node"]
    assert logs[0]["isDataset"] is False
    assert not any("SET a:" in query or "CREATE" in query for query in queries)


@pytest.mark.parametrize("labels", [["DELETED"], ["PERIOD", "DELETED"], ["CATEGORY", "DELETED"]])
def test_deleted_category_is_rejected_before_metadata_lookup(monkeypatch, labels):
    def get_query(query, **kwargs):
        assert "RETURN labels(n) AS labels" in query
        return [{"labels": labels}]
    monkeypatch.setattr(admin, "getQuery", get_query)
    with pytest.raises(ValueError, match="deleted node"):
        admin._editable_node_label("AM181112", object())


@pytest.mark.parametrize("cmid,labels", [
    ("AM181112", []), ("AM181112", ["UNKNOWN"]),
    ("AM181112", ["REPOSITORY"]), ("AM181112", ["ALL NODES"]),
    ("XX181112", ["PERIOD"]), ("AM181112x", ["PERIOD"]),
])
def test_unknown_or_noncategory_nodes_remain_uneditable(monkeypatch, cmid, labels):
    def get_query(query, **kwargs):
        return [{"labels": labels}] if "RETURN labels(n) AS labels" in query else []
    monkeypatch.setattr(admin, "getQuery", get_query)
    with pytest.raises(ValueError, match="no editable node label"):
        admin._editable_node_label(cmid, object())


def test_missing_node_remains_invalid(monkeypatch):
    monkeypatch.setattr(admin, "getQuery", lambda **kwargs: [])
    with pytest.raises(ValueError, match="is invalid"):
        admin._editable_node_label("AM181112", object())


def test_owner_guard_still_blocks_orphan_mutation(monkeypatch):
    monkeypatch.setattr(admin, "getDriver", lambda _: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda _: [])
    monkeypatch.setattr(admin, "addCMNameRel", lambda *args, **kwargs: None)
    def get_query(query, driver=None, params=None, **kwargs):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["PERIOD"]}]
        if "MATCH (l:LABEL)" in query:
            return [{"label": "PERIOD"}]
        if "RETURN a.CMName AS val" in query:
            return ["Original"]
        if "SET a.CMName = $id" in query:
            assert params["actorUserId"] == "other-user"
            assert "ownerUserId" in query
            return []
        raise AssertionError(query)
    monkeypatch.setattr(admin, "getQuery", get_query)
    with pytest.raises(PermissionError, match="not authorized"):
        admin.add_edit_delete_Node("archamap", "other-user", {
            "s1_1": "edit", "s1_2": "AM181112", "s1_3": "Changed", "s1_7": "CMName",
            "_actorClaims": {"userid": "other-user", "role": "user"},
        })
