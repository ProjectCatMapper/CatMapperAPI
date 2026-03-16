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
    assert row["populationEstimate"] == 792.0


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
