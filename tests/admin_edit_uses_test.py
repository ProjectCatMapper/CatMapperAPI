import CM.admin as admin
import pytest


def _base_input():
    return {
        "s1_1": "edit",
        "s1_2": "SM254496",
        "s1_3": "792",
        "s1_7": 1,
        "s1_8": "populationEstimate",
        "s1_4": [
            [
                {"CMName": "Example Category", "CMID": "SM254496"},
                {"Key": None, "id": "rel-123"},
                {"CMName": "Example Dataset", "CMID": "SD1"},
            ]
        ],
    }


def test_add_edit_delete_uses_uses_relid_for_selected_relation(monkeypatch):
    captured = {}

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    def fake_update_property(df, optionalProperties, isDataset, database, user, updateType, propertyType="USES", sep="||||"):
        captured["df"] = df.copy()
        captured["optionalProperties"] = list(optionalProperties)
        return {"result": [{"relID": "rel-123"}], "df": df.to_dict(orient="records")}

    monkeypatch.setattr(admin, "updateProperty", fake_update_property)

    result = admin.add_edit_delete_USES("sociomap", "tester", _base_input())

    assert result == "done"
    row = captured["df"].to_dict(orient="records")[0]
    assert captured["optionalProperties"] == ["populationEstimate"]
    assert row["relID"] == "rel-123"
    assert row["CMID"] == "SM254496"
    assert row["datasetID"] == "SD1"
    assert row["populationEstimate"] == "792"


def test_add_edit_delete_uses_raises_when_no_rows_are_updated(monkeypatch):
    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)
    monkeypatch.setattr(
        admin,
        "updateProperty",
        lambda *args, **kwargs: {"result": [], "df": []},
    )

    with pytest.raises(Exception, match="No USES ties were updated"):
        admin.add_edit_delete_USES("sociomap", "tester", _base_input())


def test_add_edit_delete_uses_handles_list_population_meta_type(monkeypatch):
    captured = {}

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(
        admin,
        "getPropertiesMetadata",
        lambda driver: [
            {"type": "relationship", "property": "populationEstimate", "metaType": "listFloat"},
        ],
    )
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    def fake_update_property(df, optionalProperties, isDataset, database, user, updateType, propertyType="USES", sep="||||"):
        captured["df"] = df.copy()
        return {"result": [{"relID": "rel-123"}], "df": df.to_dict(orient="records")}

    monkeypatch.setattr(admin, "updateProperty", fake_update_property)

    result = admin.add_edit_delete_USES("sociomap", "tester", _base_input())

    assert result == "done"
    row = captured["df"].to_dict(orient="records")[0]
    assert row["populationEstimate"] == ["792"]


def test_add_edit_delete_uses_delete_logs_multiple_rel_ids(monkeypatch):
    captured = {}

    payload = _base_input()
    payload["s1_1"] = "delete"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "REMOVE r[$USES_property]" in query:
            return [{"relID": "rel-1"}, {"relID": "rel-2"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    def fake_create_log(id, type, log, user, driver, isDataset=False):
        captured["id"] = id
        captured["log"] = log
        captured["type"] = type
        return "Completed"

    monkeypatch.setattr(admin, "createLog", fake_create_log)

    result = admin.add_edit_delete_USES("sociomap", "tester", payload)

    assert result == "done"
    assert captured["type"] == "relation"
    assert captured["id"] == ["rel-1", "rel-2"]
    assert captured["log"] == [
        "deleted USES property populationEstimate",
        "deleted USES property populationEstimate",
    ]


def test_add_edit_delete_uses_normalizes_variable_category_type(monkeypatch):
    captured = {}
    payload = _base_input()
    payload["s1_3"] = "categorical"
    payload["s1_8"] = "categoryType"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(
        admin,
        "getNodeMergeSummary",
        lambda cmid, driver: {"primaryDomain": "VARIABLE"},
    )
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    def fake_update_property(df, optionalProperties, isDataset, database, user, updateType, propertyType="USES", sep="||||"):
        captured["df"] = df.copy()
        return {"result": [{"relID": "rel-123"}], "df": df.to_dict(orient="records")}

    monkeypatch.setattr(admin, "updateProperty", fake_update_property)

    result = admin.add_edit_delete_USES("sociomap", "tester", payload)

    assert result == "done"
    row = captured["df"].to_dict(orient="records")[0]
    assert row["categoryType"] == "CATEGORICAL"


def test_add_edit_delete_uses_rejects_invalid_variable_category_type(monkeypatch):
    payload = _base_input()
    payload["s1_3"] = "numeric"
    payload["s1_8"] = "categoryType"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(
        admin,
        "getNodeMergeSummary",
        lambda cmid, driver: {"primaryDomain": "VARIABLE"},
    )

    with pytest.raises(ValueError, match="Invalid categoryType"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)
