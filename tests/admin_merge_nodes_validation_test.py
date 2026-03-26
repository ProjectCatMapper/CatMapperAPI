import CM.admin as admin_module
import CMroutes.admin_routes as admin_routes


def test_merge_nodes_rejects_mismatched_primary_domains(monkeypatch):
    monkeypatch.setattr(admin_module, "getDriver", lambda _database: object())
    monkeypatch.setattr(admin_module, "isValidCMID", lambda _cmid, _driver: ["ok"])

    summaries = {
        "AM1": {"CMID": "AM1", "CMName": "District Node", "primaryDomain": "DISTRICT"},
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
            "primaryDomain": "DISTRICT",
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
    assert payload["primaryDomain"] == "DISTRICT"


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

    queries = []

    def fake_get_query(query, driver, params=None, type=None, **kwargs):
        queries.append(query)
        if "return elementId(r) as relID" in query:
            return []
        if "return c.CMID as cmid" in query:
            return []
        if "return m.CMName as property" in query:
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
        if "return m.CMName as property" in query:
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
