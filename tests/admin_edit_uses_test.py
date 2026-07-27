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


def test_uses_self_context_exception_uses_district_property_name():
    assert admin._is_uses_self_context_exception("district", None)
    assert admin._is_uses_self_context_exception("District", None)
    assert admin._is_uses_self_context_exception("parent", "CONTAINS")
    assert admin._is_uses_self_context_exception("language", "AREA_OF")
    assert not admin._is_uses_self_context_exception("language", "LANGUOID_OF")


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


def test_validate_parent_context_list_accepts_string_event_date(monkeypatch):
    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        assert params == {"CMID": "AM27636"}
        return [{"cmidExists": True}]

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    errors = admin.validate_parent_context_list(
        object(),
        ['{"parent":"AM27636","eventDate":"420","eventType":"FOLLOWS"}'],
    )

    assert errors == []


def test_validate_parent_context_list_accepts_parent_only(monkeypatch):
    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        assert params == {"CMID": "SM257723"}
        return [{"cmidExists": True}]

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    errors = admin.validate_parent_context_list(
        object(),
        ['{"parent":"SM257723"}'],
    )

    assert errors == []


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


def test_add_edit_delete_uses_rejects_invalid_key_format(monkeypatch):
    payload = _base_input()
    payload["s1_3"] = "EC = 16981"
    payload["s1_8"] = "Key"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)
    monkeypatch.setattr(
        admin,
        "updateProperty",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("updateProperty should not be called")),
    )

    with pytest.raises(ValueError, match="Each Key pair must include"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)


def test_add_edit_delete_uses_accepts_valid_key_format(monkeypatch):
    captured = {}
    payload = _base_input()
    payload["s1_3"] = "EC == 16981"
    payload["s1_8"] = "Key"

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def run(self, query, **kwargs):
            captured["duplicate_rows"] = kwargs.get("rows")
            class FakeResult:
                @staticmethod
                def data():
                    return []

            return FakeResult()

    class FakeDriver:
        def session(self):
            return FakeSession()

    monkeypatch.setattr(admin, "getDriver", lambda database: FakeDriver())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)

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

    result = admin.add_edit_delete_USES("sociomap", "tester", payload)

    assert result == "done"
    assert captured["optionalProperties"] == ["NewKey"]
    row = captured["df"].to_dict(orient="records")[0]
    assert row["NewKey"] == "EC == 16981"
    assert captured["duplicate_rows"]["NewKey"] == "EC == 16981"


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


def test_add_edit_delete_uses_rejects_stale_selected_relation_index(monkeypatch):
    payload = _base_input()
    payload["s1_7"] = 2

    with pytest.raises(ValueError, match="Selected USES tie is invalid"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)


def test_add_edit_delete_uses_rejects_selected_relation_without_dataset(monkeypatch):
    payload = _base_input()
    payload["s1_4"] = [
        [
            {"CMName": "Example Category", "CMID": "SM254496"},
            {"Key": None, "id": "rel-123"},
        ]
    ]

    with pytest.raises(ValueError, match="Selected USES tie payload is invalid"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)


def test_add_edit_delete_uses_parent_validation_reports_invalid_current_cmid(monkeypatch):
    payload = _base_input()
    payload["s1_8"] = "parent"
    payload["s1_3"] = "SM999999"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "RETURN labels(n)" in query:
            return []
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    with pytest.raises(ValueError, match="SM254496 is invalid"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)


def test_add_edit_delete_uses_parent_context_accepts_single_json_string_and_syncs_parent(monkeypatch):
    captured = {}
    payload = _base_input()
    payload["s1_8"] = "parentContext"
    payload["s1_3"] = '{"parent":"SM257723"}'

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(
        admin,
        "getPropertiesMetadata",
        lambda driver: [
            {"type": "relationship", "property": "parentContext", "metaType": "list"},
            {"type": "relationship", "property": "parent", "metaType": "list"},
        ],
    )
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None}]
        if "RETURN EXISTS" in query:
            return [{"cmidExists": True}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    def fake_update_property(df, optionalProperties, isDataset, database, user, updateType, propertyType="USES", sep="||||"):
        captured["df"] = df.copy()
        captured["optionalProperties"] = list(optionalProperties)
        return {"result": [{"relID": "rel-123"}], "df": df.to_dict(orient="records")}

    monkeypatch.setattr(admin, "updateProperty", fake_update_property)

    result = admin.add_edit_delete_USES("sociomap", "tester", payload)

    assert result == "done"
    assert captured["optionalProperties"] == ["parentContext", "parent"]
    row = captured["df"].to_dict(orient="records")[0]
    assert row["parentContext"] == ['{"parent":"SM257723"}']
    assert row["parent"] == ["SM257723"]


def test_add_edit_delete_uses_parent_sends_multi_parent_list(monkeypatch):
    captured = {}
    payload = _base_input()
    payload["s1_8"] = "parent"
    payload["s1_3"] = "SD2182 || SD2181"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(
        admin,
        "getPropertiesMetadata",
        lambda driver: [
            {"type": "relationship", "property": "parent", "metaType": "list"},
        ],
    )
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(admin, "getGroupLabels", lambda cmid, driver: "DATASET")
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_update_property(df, optionalProperties, isDataset, database, user, updateType, propertyType="USES", sep="||||"):
        captured["df"] = df.copy()
        return {"result": [{"relID": "rel-123"}], "df": df.to_dict(orient="records")}

    monkeypatch.setattr(admin, "updateProperty", fake_update_property)

    result = admin.add_edit_delete_USES("sociomap", "tester", payload)

    assert result == "done"
    row = captured["df"].to_dict(orient="records")[0]
    assert row["parent"] == ["SD2182", "SD2181"]


def test_add_edit_delete_uses_rejects_same_domain_contextual_property(monkeypatch):
    payload = _base_input()
    payload["s1_2"] = "SM-LANG"
    payload["s1_3"] = "SM-OTHER-LANG"
    payload["s1_8"] = "language"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "getGroupLabels", lambda cmid, driver: "LANGUOID")
    monkeypatch.setattr(
        admin,
        "validatePropertyCMID",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("CMID validation should not run after same-domain rejection")),
    )

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None, "relationship": "LANGUOID_OF"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)
    monkeypatch.setattr(
        admin,
        "updateProperty",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("updateProperty should not be called")),
    )

    with pytest.raises(ValueError, match="Cannot add language"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)


def test_add_edit_delete_uses_allows_district_same_domain_exception(monkeypatch):
    captured = {}
    payload = _base_input()
    payload["s1_2"] = "SM-AREA"
    payload["s1_3"] = "SM-OTHER-AREA"
    payload["s1_8"] = "district"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "getGroupLabels", lambda cmid, driver: "AREA")
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: None)
    monkeypatch.setattr(
        admin,
        "validate_contextual_tie_primary_domains",
        lambda *args, **kwargs: None,
    )

    def fake_validate_property_cmid(value, proptoChange, validgroupLabel, driver):
        captured["validated"] = {
            "value": value,
            "property": proptoChange,
            "groupLabel": validgroupLabel,
        }

    monkeypatch.setattr(admin, "validatePropertyCMID", fake_validate_property_cmid)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            return [{"groupLabel": None, "relationship": "AREA_OF"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    def fake_update_property(df, optionalProperties, isDataset, database, user, updateType, propertyType="USES", sep="||||"):
        captured["df"] = df.copy()
        captured["optionalProperties"] = list(optionalProperties)
        return {"result": [{"relID": "rel-123"}], "df": df.to_dict(orient="records")}

    monkeypatch.setattr(admin, "updateProperty", fake_update_property)

    result = admin.add_edit_delete_USES("sociomap", "tester", payload)

    assert result == "done"
    assert captured["validated"] == {
        "value": "SM-OTHER-AREA",
        "property": "district",
        "groupLabel": "AREA",
    }
    assert captured["optionalProperties"] == ["district"]
    row = captured["df"].to_dict(orient="records")[0]
    assert row["district"] == "SM-OTHER-AREA"


def test_add_edit_delete_uses_checks_actual_labels_for_language_of(monkeypatch):
    payload = _base_input()
    payload["s1_2"] = "SM-DIALECT"
    payload["s1_3"] = "SM-FAMILY"
    payload["s1_8"] = "language"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "getGroupLabels", lambda cmid, driver: "LANGUOID")
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (n:PROPERTY)" in query and "RETURN n.groupLabel as groupLabel" in query:
            # LANGUAGE_OF cannot reliably reveal the LANGUOID primary domain by
            # parsing its name, so the endpoint-label validator must decide.
            return [{"groupLabel": None, "relationship": "LANGUAGE_OF"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    def reject_shared_labels(driver, source_cmid, target_cmids, relationship):
        assert source_cmid == "SM-DIALECT"
        assert target_cmids == ["SM-FAMILY"]
        assert relationship == "LANGUAGE_OF"
        raise ValueError("same primary domain: LANGUOID")

    monkeypatch.setattr(
        admin,
        "validate_contextual_tie_primary_domains",
        reject_shared_labels,
    )
    monkeypatch.setattr(
        admin,
        "updateProperty",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("updateProperty should not run after endpoint rejection")
        ),
    )

    with pytest.raises(ValueError, match="LANGUOID"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)


def test_add_edit_delete_uses_reports_process_uses_failure(monkeypatch):
    payload = _base_input()
    payload["s1_3"] = "SM-PARENT"
    payload["s1_8"] = "parent"

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])
    monkeypatch.setattr(admin, "getGroupLabels", lambda cmid, driver: "ETHNICITY")
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        admin,
        "updateProperty",
        lambda *args, **kwargs: {"result": [{"relID": "rel-123"}]},
    )
    monkeypatch.setattr(
        admin,
        "processUSES",
        lambda **kwargs: ("updateContains failed", 500),
    )

    with pytest.raises(RuntimeError, match="updateContains failed"):
        admin.add_edit_delete_USES("sociomap", "tester", payload)


def test_add_edit_delete_node_dataset_parent_normalizes_multi_parent_values(monkeypatch):
    captured = {}
    payload = {
        "s1_1": "add",
        "s1_2": "SD2183",
        "s1_3": "SD2182, SD2181",
        "s1_7": "parent",
    }

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [
        {"type": "node", "property": "parent", "metaType": "list"},
    ])
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)
    monkeypatch.setattr(admin, "processDATASETs", lambda database, CMID, user: captured.setdefault("processed", CMID))

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["DATASET", "STACK"]}]
        if "RETURN a.parent AS val" in query:
            return [[]]
        if "SET a.parent = $id" in query:
            captured["params"] = params
            return []
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    result = admin.add_edit_delete_Node("sociomap", "tester", payload)

    assert result == "updated successfully"
    assert captured["params"] == {"id": ["SD2182", "SD2181"]}
    assert captured["processed"] == "SD2183"


def test_add_edit_delete_node_list_metadata_splits_pipe_delimited_values(monkeypatch):
    captured = {}
    payload = {
        "s1_1": "edit",
        "s1_2": "SD767",
        "s1_3": "SM461549 || SM461550 || SM461551",
        "s1_7": "foci",
    }

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [
        {"type": "node", "property": "foci", "metaType": "list"},
    ])

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["DATASET"]}]
        if "RETURN a.foci AS val" in query:
            return [["SM461549", "SM461550"]]
        if "SET a.foci = $id" in query:
            captured["params"] = params
            return []
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)
    monkeypatch.setattr(admin, "processDATASETs", lambda database, CMID, user: captured.setdefault("processed", CMID))

    result = admin.add_edit_delete_Node("sociomap", "tester", payload)

    assert result == "updated successfully"
    assert captured["params"] == {"id": ["SM461549", "SM461550", "SM461551"]}


def test_add_edit_delete_node_rejects_deleted_node(monkeypatch):
    payload = {
        "s1_1": "add",
        "s1_2": "SD2183",
        "s1_3": "SD2182 || SD2181",
        "s1_7": "parent",
    }

    monkeypatch.setattr(admin, "getDriver", lambda database: object())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [
        {"type": "node", "property": "parent", "metaType": "list"},
    ])
    monkeypatch.setattr(admin, "validatePropertyCMID", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["DELETED"]}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    with pytest.raises(ValueError, match="deleted node"):
        admin.add_edit_delete_Node("sociomap", "tester", payload)


def test_add_edit_delete_node_rejects_restricted_identifier_wrong_domain(monkeypatch):
    payload = {
        "s1_1": "add",
        "s1_2": "SM-LANG",
        "s1_3": "US",
        "s1_7": "FIPS",
    }

    class FakeDomainSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def run(self, query, params=None):
            return [{"label": "LANGUOID", "groupLabel": "LANGUOID"}]

    class FakeDomainDriver:
        def session(self):
            return FakeDomainSession()

    monkeypatch.setattr(admin, "getDriver", lambda database: FakeDomainDriver())
    monkeypatch.setattr(admin, "getPropertiesMetadata", lambda driver: [])

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "RETURN n.CMID AS CMID" in query:
            return [{"CMID": "SM-LANG", "CMName": "Language", "labels": ["CATEGORY", "LANGUOID"]}]
        if "OPTIONAL MATCH (m:LABEL {CMName: label})" in query:
            return [{"label": "LANGUOID", "groupLabel": "LANGUOID"}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(admin, "getQuery", fake_get_query)

    with pytest.raises(ValueError, match="FIPS.*AREA"):
        admin.add_edit_delete_Node("sociomap", "tester", payload)
