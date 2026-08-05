import CM.ownership as ownership
import CM.log as log_module


def test_ownership_metadata_uses_only_authoritative_owner_and_lock():
    assert ownership.ownership_metadata({"userid": 7, "role": "user"}) == {
        "ownerUserId": "7",
        "modifiedByOtherUser": False,
    }


def test_uses_authorization_is_owner_only_and_rejects_lock(monkeypatch):
    captured = {}
    monkeypatch.setattr(ownership, "getDriver", lambda database: object())

    def fake_get_query(**kwargs):
        captured.update(kwargs)
        return [{"relID": "rel-1", "targetCount": 1, "ownedCount": 1}]

    monkeypatch.setattr(ownership, "getQuery", fake_get_query)

    assert ownership.assert_owned_uses_by_relids(
        "ArchaMap",
        ["rel-1"],
        {"userid": "7", "role": "user"},
    )
    assert "createdByUserId" not in captured["query"]
    assert "ownerUserId" in captured["query"]
    assert "modifiedByOtherUser" in captured["query"]


def test_reconcile_owner_edit_metadata_is_fail_closed_and_removes_legacy(monkeypatch):
    seen = []
    monkeypatch.setattr(ownership, "getDriver", lambda database: object())

    def fake_get_query(query, driver, params=None, type="dict"):
        seen.append({"query": query, "params": params or {}})
        if "OWNER_METADATA_CONFLICT_CHECK" in query:
            return []
        if "OWNER_METADATA_DEFINITION_UPSERT" in query:
            return [{"count": 4}]
        if "OWNER_METADATA_VERIFY" in query:
            return [{
                "ownedNodes": 10,
                "nodesMissingLock": 0,
                "nodesWithLegacy": 0,
                "ownedUses": 20,
                "usesMissingLock": 0,
                "usesWithLegacy": 0,
            }]
        return [{"count": 2}]

    monkeypatch.setattr(ownership, "getQuery", fake_get_query)

    result = ownership.reconcile_owner_edit_metadata(
        "ArchaMap",
        return_type="data",
    )

    assert result["propertyDefinitions"] == 4
    assert result["verification"]["ownedNodes"] == 10
    assert result["verification"]["ownedUses"] == 20

    node_lock = next(
        call for call in seen
        if "OWNER_METADATA_NODE_LOCK_RECONCILE" in call["query"]
    )
    uses_lock = next(
        call for call in seen
        if "OWNER_METADATA_USES_LOCK_RECONCILE" in call["query"]
    )
    assert node_lock["params"]["systemUsers"] == ["0"]
    assert uses_lock["params"]["systemUsers"] == ["0"]
    assert "WHEN size(users) = 0 THEN true" in node_lock["query"]
    assert "WHEN size(users) = 0 THEN true" in uses_lock["query"]
    assert "toBooleanOrNull(toString(n.modifiedByOtherUser))" in node_lock["query"]
    assert "WHEN currentLock = true THEN true" in node_lock["query"]
    assert "toBooleanOrNull(toString(r.modifiedByOtherUser))" in uses_lock["query"]
    assert "WHEN currentLock = true THEN true" in uses_lock["query"]
    assert any(
        "REMOVE n.createdByUserId, n.createdAt, n.contributionId" in call["query"]
        for call in seen
    )
    assert any(
        "REMOVE r.createdByUserId, r.createdAt, r.contributionId" in call["query"]
        for call in seen
    )


def test_internal_property_metadata_uses_synchronized_cmids():
    definitions = ownership.INTERNAL_OWNER_PROPERTY_METADATA

    assert [row["CMID"] for row in definitions] == [
        "CP188",
        "CP189",
        "CP190",
        "CP191",
    ]
    assert {
        (row["CMName"], row["type"], row["metaType"])
        for row in definitions
    } == {
        ("ownerUserId", "node", "string"),
        ("ownerUserId", "relationship", "string"),
        ("modifiedByOtherUser", "node", "boolean"),
        ("modifiedByOtherUser", "relationship", "boolean"),
    }
    assert all(row["internal"] is True for row in definitions)
    assert all(row["editable"] is False for row in definitions)


def test_create_log_updates_monotonic_human_modification_lock(monkeypatch):
    captured = {}

    def fake_get_query(query, driver, params=None, **kwargs):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(log_module, "getQuery", fake_get_query)

    assert log_module.createLog(
        id="rel-1",
        type="relation",
        log="changed Name",
        user="8",
        driver=object(),
    ) == "Completed"

    query = captured["query"]
    assert "l.modifiedByOtherUser" in query
    assert "toString(row.user) = '0'" in query
    assert "toString(l.ownerUserId) <> toString(row.user)" in query
    assert "THEN true" in query
