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
