import pandas as pd

import CM.merge as merge_mod


def _normalize_result(result):
    if isinstance(result, tuple):
        return result[0], result[1]
    return result, 200


def _base_crossdomain_kwargs():
    return dict(
        dataset_choices=["SD1", "AD2", "SD3"],
        category_label="CATEGORY",
        criteria="crossdomain",
        database="ArchaMap",
        intersection=False,
        selectedKeyvariables={},
        ncontains=2,
        resultFormat="key-to-key",
        source_domain="LANGUAGE",
        target_domain="ETHNICITY",
        return_domain="ETHNICITY",
        primary_dataset="SD1",
        max_hops=3,
    )


def test_crossdomain_errors_when_no_of_relationship_exists(monkeypatch):
    monkeypatch.setattr(merge_mod, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_mod, "validate_domain_label", lambda label, driver=None, aliases=None, extra_allowed=None: str(label).upper())
    monkeypatch.setattr(merge_mod, "getQuery", lambda query, driver=None, params=None, type=None: [])

    payload, status = _normalize_result(merge_mod.proposeMerge(**_base_crossdomain_kwargs()))

    assert status == 400
    assert "No *_OF relationship exists" in str(payload.get("error", ""))


def test_crossdomain_uses_contains_and_of_only(monkeypatch):
    monkeypatch.setattr(merge_mod, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_mod, "validate_domain_label", lambda label, driver=None, aliases=None, extra_allowed=None: str(label).upper())

    captured = {"queries": []}

    def fake_get_query(query, driver=None, params=None, type=None):
        captured["queries"].append(query)
        if "ENDS WITH '_OF'" in query:
            return [{"relType": "LANGUAGE_OF"}]
        if "RETURN d.CMID AS datasetID, d.CMName AS datasetName" in query:
            return [
                {"datasetID": "SD1", "datasetName": "Dataset One"},
                {"datasetID": "AD2", "datasetName": "Dataset Two"},
                {"datasetID": "SD3", "datasetName": "Dataset Three"},
            ]
        if type == "df":
            return pd.DataFrame(
                [
                    {"datasetID": "SD1", "sourceCMID": "L1", "sourceCMName": "Lang A", "sourceExpandedCMID": "L1", "sourceExpandedCMName": "Lang A", "targetCMID": "E1", "targetCMName": "Eth A", "CMID": "E1", "CMName": "Eth A", "Key": "lang == a", "Name": "Name A", "sourceTie": 0, "targetTie": 0, "tie": 1},
                    {"datasetID": "AD2", "sourceCMID": "L2", "sourceCMName": "Lang B", "sourceExpandedCMID": "L2", "sourceExpandedCMName": "Lang B", "targetCMID": "E1", "targetCMName": "Eth A", "CMID": "E1", "CMName": "Eth A", "Key": "eth == b", "Name": "Name B", "sourceTie": 0, "targetTie": 0, "tie": 1},
                    {"datasetID": "SD3", "sourceCMID": "L3", "sourceCMName": "Lang C", "sourceExpandedCMID": "L3", "sourceExpandedCMName": "Lang C", "targetCMID": "E1", "targetCMName": "Eth A", "CMID": "E1", "CMName": "Eth A", "Key": "eth == c", "Name": "Name C", "sourceTie": 0, "targetTie": 0, "tie": 1},
                ]
            )
        return []

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    payload, status = _normalize_result(merge_mod.proposeMerge(**_base_crossdomain_kwargs()))

    assert status == 200
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert payload[0]["relationshipType"] == "LANGUAGE_OF"
    assert payload[0]["CMID"] == "E1"
    all_queries = "\n".join(captured["queries"])
    assert "CONTAINS" in all_queries
    assert "LANGUAGE_OF" in all_queries
    assert "EQUIVALENT" not in all_queries
    assert " IS " not in all_queries


def test_crossdomain_key_to_category_includes_dataset_name_and_relationship(monkeypatch):
    monkeypatch.setattr(merge_mod, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_mod, "validate_domain_label", lambda label, driver=None, aliases=None, extra_allowed=None: str(label).upper())

    def fake_get_query(query, driver=None, params=None, type=None):
        if "ENDS WITH '_OF'" in query:
            return [{"relType": "LANGUAGE_OF"}]
        if "RETURN d.CMID AS datasetID, d.CMName AS datasetName" in query:
            return [
                {"datasetID": "SD1", "datasetName": "Dataset One"},
                {"datasetID": "AD2", "datasetName": "Dataset Two"},
                {"datasetID": "SD3", "datasetName": "Dataset Three"},
            ]
        if type == "df":
            return pd.DataFrame(
                [
                    {"datasetID": "SD1", "sourceCMID": "L1", "sourceCMName": "Lang A", "sourceExpandedCMID": "L1", "sourceExpandedCMName": "Lang A", "targetCMID": "E1", "targetCMName": "Eth A", "CMID": "E1", "CMName": "Eth A", "Key": "lang == a", "Name": "Name A", "sourceTie": 0, "targetTie": 0, "tie": 1},
                    {"datasetID": "AD2", "sourceCMID": "L2", "sourceCMName": "Lang B", "sourceExpandedCMID": "L2", "sourceExpandedCMName": "Lang B", "targetCMID": "E1", "targetCMName": "Eth A", "CMID": "E1", "CMName": "Eth A", "Key": "eth == b", "Name": "Name B", "sourceTie": 0, "targetTie": 0, "tie": 1},
                    {"datasetID": "SD3", "sourceCMID": "L3", "sourceCMName": "Lang C", "sourceExpandedCMID": "L3", "sourceExpandedCMName": "Lang C", "targetCMID": "E1", "targetCMName": "Eth A", "CMID": "E1", "CMName": "Eth A", "Key": "eth == c", "Name": "Name C", "sourceTie": 0, "targetTie": 0, "tie": 1},
                ]
            )
        return []

    monkeypatch.setattr(merge_mod, "getQuery", fake_get_query)

    kwargs = _base_crossdomain_kwargs()
    kwargs["intersection"] = True
    kwargs["resultFormat"] = "key-to-category"
    payload, status = _normalize_result(merge_mod.proposeMerge(**kwargs))

    assert status == 200
    assert isinstance(payload, list)
    assert len(payload) == 3
    assert payload[0]["datasetCMName"] in {"Dataset One", "Dataset Two", "Dataset Three"}
    assert payload[0]["relationshipType"] == "LANGUAGE_OF"
