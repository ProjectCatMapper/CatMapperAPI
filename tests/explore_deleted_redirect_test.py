from CM import explore as explore_module


def test_get_category_info_supports_deleted_nodes(monkeypatch):
    monkeypatch.setattr(explore_module, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, cmid=None, type=None, params=None, **kwargs):
        if "RETURN labels(n) AS labels" in query:
            return [["DELETED"]]
        if "MATCH (a:DELETED)" in query and "Merged_into_CMID" in query:
            return [{
                "CMName": "Legacy Node",
                "CMID": "SMOLD",
                "Domains": ["DELETED"],
                "Merged_into_CMID": "SMNEW",
            }]
        return []

    monkeypatch.setattr(explore_module, "getQuery", fake_get_query)

    result = explore_module.getCategoryInfo("SocioMap", "SMOLD")

    assert result["CMID"] == "SMOLD"
    assert "DELETED" in result["Domains"]
    assert result["Merged_into_CMID"] == "SMNEW"


def test_get_category_page_returns_empty_payload_when_node_not_found(monkeypatch):
    monkeypatch.setattr(explore_module, "getDriver", lambda database: object())
    monkeypatch.setattr(explore_module, "getQuery", lambda *args, **kwargs: [])

    result = explore_module.getCategoryPage("SocioMap", "SM404")

    assert result == {
        "samples": [],
        "categories": [],
        "childcategories": [],
        "relnames": [],
    }
