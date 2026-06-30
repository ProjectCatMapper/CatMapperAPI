import pandas as pd
import pytest

import CM.upload as upload
from CM.keys import (
    invalid_key_format_error,
    invalid_key_row_details,
    invalid_key_row_numbers,
    is_valid_key_format,
    key_format_warning_messages,
    key_format_warnings,
)


@pytest.mark.parametrize(
    "key",
    [
        "Type == Adamana Brown",
        "Type == Adamana Brown && Period == Archaic",
    ],
)
def test_key_format_validator_accepts_standard_keys(key):
    assert is_valid_key_format(key) is True


@pytest.mark.parametrize(
    "key",
    [
        "",
        None,
        "Type==Adamana Brown",
        "Type == ",
        " == Adamana Brown",
        "Type == Alpha &&",
        "Type == Alpha && Period",
    ],
)
def test_key_format_validator_rejects_malformed_keys(key):
    assert is_valid_key_format(key) is False


@pytest.mark.parametrize(
    "key",
    [
        "Type == Alpha == Beta",
        "Type == Alpha&&Period == Early",
        "Type&&Subtype == Alpha",
        "Type == 1==2",
        "Type == Alpha&&",
        "&&Type == Alpha",
    ],
)
def test_key_format_validator_allows_reserved_tokens_inside_key_parts(key):
    assert is_valid_key_format(key) is True


def test_invalid_key_row_numbers_are_one_based():
    rows = ["Type == Alpha", "Type == Alpha &&", "Type==Beta"]

    assert invalid_key_row_numbers(rows) == [2, 3]


@pytest.mark.parametrize(
    "key, expected_message",
    [
        ("Type == Alpha&&Period == Early", 'Does the original variable value contain "&&"?'),
        ("Type == Alpha == Beta", 'Does the original variable value contain "=="?'),
        ("Type&&Subtype == Alpha", 'Does the original variable name contain "&&"?'),
        ("Type == 1==2", 'Does the original variable value contain "=="?'),
        ("Type == Alpha&&", 'Does the original variable value contain "&&"?'),
        ("&&Type == Alpha", 'Does the original variable name contain "&&"?'),
    ],
)
def test_key_format_warnings_preserve_reserved_token_prompt(key, expected_message):
    assert any(expected_message in warning for warning in key_format_warnings(key))


def test_invalid_key_row_details_include_row_specific_messages():
    rows = ["Type == Alpha", "Type==Alpha", "Type == "]

    details = invalid_key_row_details(rows)

    assert [detail["row"] for detail in details] == [2, 3]
    assert 'Found "==" without spaces around it' in details[0]["message"]
    assert 'Key value must not be empty after " == "' in details[1]["message"]


def test_invalid_key_format_error_includes_guidance_and_row_messages():
    message = invalid_key_format_error(["Type==Alpha"], "Key")

    assert "Invalid 'Key' format in rows:\n[1]." in message
    assert 'Use spaces around delimiters: variable == value.' in message
    assert 'Row 1: Found "==" without spaces around it' in message


def test_key_format_warning_messages_include_rows_and_columns():
    messages = key_format_warning_messages(["Type == 1==2"], "Key")

    assert len(messages) == 1
    assert messages[0].startswith("Key row 1:")
    assert 'Does the original variable value contain "=="?' in messages[0]


def test_collect_unique_column_values_for_multi_value_column():
    dataset = pd.DataFrame({"language": ["AM1; AM2", "AM2;AM3", "", None, " AM1 ", "['AM4']"]})

    values = upload._collect_unique_column_values(dataset, "language", {"language"})

    assert set(values) == {"AM1", "AM2", "AM3", "AM4"}


def test_normalize_semicolon_value_list_handles_stringified_lists():
    assert upload._normalize_semicolon_value_list("['AM22269']") == ["AM22269"]
    assert upload._normalize_semicolon_value_list('["AM22269", "AM22270"]') == [
        "AM22269",
        "AM22270",
    ]
    assert upload._normalize_semicolon_value_list("AM22269; AM22270") == [
        "AM22269",
        "AM22270",
    ]


@pytest.mark.parametrize("column_name", ["country", "district", "District"])
def test_required_label_for_area_reference_columns(column_name):
    assert upload._required_label_for_column(column_name) == "AREA"


def test_create_uses_normalizes_stringified_district_lists(monkeypatch):
    captured = {}

    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "getPropertiesMetadata", lambda driver: [
        {"property": "Name", "metaType": "string"},
        {"property": "district", "metaType": "CMID"},
        {"property": "label", "metaType": "string"},
    ])
    monkeypatch.setattr(upload, "updateAltNames", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "createLog", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (p:PROPERTY)" in query:
            return [
                {"property": "datasetID"},
                {"property": "CMID"},
                {"property": "Key"},
                {"property": "Name"},
                {"property": "district"},
                {"property": "label"},
            ]
        if "RETURN count(*) AS count" in query:
            return [0]
        if "WITH DISTINCT row.datasetID AS datasetID, row.Key AS keyValue" in query:
            return [
                {
                    "datasetID": "AD1",
                    "Key": "Type == Alpha",
                    "existingCMIDs": [],
                    "rel_count": 0,
                }
            ]
        if params and "rows" in params:
            captured["rows"] = params["rows"]
            return [
                {
                    "nodeID": "node-1",
                    "relID": "rel-1",
                    "Key": "Type == Alpha",
                    "datasetID": "AD1",
                    "CMID": "AM1",
                    "CMName": "Alpha",
                    "Name": "Alpha",
                    "district": ["AM22269"],
                    "label": "DIALECT",
                }
            ]
        return [1]

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    result = upload.createUSES(
        pd.DataFrame(
            [
                {
                    "datasetID": "AD1",
                    "CMID": "AM1",
                    "Key": "Type == Alpha",
                    "Name": "Alpha",
                    "district": "['AM22269']",
                    "label": "DIALECT",
                }
            ]
        ),
        database="ArchaMap",
        user="tester",
    )

    assert captured["rows"][0]["district"] == "AM22269"
    assert result["links"][0]["district"] == "AM22269"


def test_input_nodes_uses_rejects_existing_dataset_key_duplicate(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='node'" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='relationship'" in query:
            return []
        if "MATCH (l:LABEL) return l.CMName as label" in query:
            return ["DIALECT"]
        if "WITH DISTINCT row.datasetID AS datasetID, row.Key AS keyValue" in query:
            return [
                {
                    "datasetID": "AD1",
                    "Key": "Type == Alpha",
                    "existingCMIDs": ["AM999"],
                    "rel_count": 1,
                }
            ]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    with pytest.raises(ValueError, match="same datasetID and Key already exists"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "CMID": "AM1",
                    "datasetID": "AD1",
                    "Key": "Type == Alpha",
                    "label": "DIALECT",
                    "Name": "Alpha",
                }
            ],
            database="ArchaMap",
            uploadOption="add_uses",
            formatKey=False,
            optionalProperties=[],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_input_nodes_uses_rejects_upload_dataset_key_duplicate_to_different_cmids(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='node'" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='relationship'" in query:
            return []
        if "MATCH (l:LABEL) return l.CMName as label" in query:
            return ["DIALECT"]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    with pytest.raises(ValueError, match="Duplicate datasetID \\+ Key values"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "CMID": "AM1",
                    "datasetID": "AD1",
                    "Key": "Type == Alpha",
                    "label": "DIALECT",
                    "Name": "Alpha",
                },
                {
                    "CMID": "AM2",
                    "datasetID": "AD1",
                    "Key": "Type == Alpha",
                    "label": "DIALECT",
                    "Name": "Beta",
                },
            ],
            database="ArchaMap",
            uploadOption="add_uses",
            formatKey=False,
            optionalProperties=[],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_input_nodes_uses_formats_key_before_key_validation(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: [])

    def _raise_on_create_key(_dataset, _cols):
        raise RuntimeError("createKey called")

    monkeypatch.setattr(upload, "createKey", _raise_on_create_key)

    with pytest.raises(RuntimeError, match="createKey called"):
        upload.input_Nodes_Uses(
            dataset=[{"CMID": "", "datasetID": "AD1", "Key": "raw-key", "label": "DIALECT"}],
            database="ArchaMap",
            uploadOption="add_uses",
            formatKey=True,
            optionalProperties=[],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_add_uses_rejects_blank_name_when_creating_new_node(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: [])

    with pytest.raises(ValueError, match="non-empty Name or CMName"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "CMID": "   ",
                    "datasetID": "AD1",
                    "Key": "Type == Adamana Brown",
                    "label": "DIALECT",
                    "Name": "   ",
                }
            ],
            database="ArchaMap",
            uploadOption="add_uses",
            formatKey=False,
            optionalProperties=[],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_input_nodes_uses_rejects_malformed_key_before_upload(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: [])

    with pytest.raises(ValueError, match="Invalid 'Key' format"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "CMID": "AM1",
                    "datasetID": "AD1",
                    "Key": "Type == Alpha &&",
                    "label": "DIALECT",
                    "Name": "Alpha",
                }
            ],
            database="ArchaMap",
            uploadOption="add_uses",
            formatKey=False,
            optionalProperties=[],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_add_node_rejects_blank_cmname(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: [])

    with pytest.raises(ValueError, match="non-empty Name or CMName"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "CMName": "   ",
                    "Name": "Visible Name",
                    "datasetID": "AD1",
                    "Key": "Type == Adamana Brown",
                    "label": "DIALECT",
                }
            ],
            database="ArchaMap",
            uploadOption="add_node",
            formatKey=False,
            optionalProperties=[],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_validate_variable_category_type_values_normalizes_variable_rows_by_label():
    dataset = pd.DataFrame(
        {
            "label": ["VARIABLE", "AREA"],
            "categoryType": ["categorical", "numeric"],
        }
    )

    upload._validate_variable_category_type_values(
        dataset,
        optionalProperties=["categoryType"],
        driver=object(),
    )

    assert dataset.loc[0, "categoryType"] == "CATEGORICAL"
    assert dataset.loc[1, "categoryType"] == "numeric"


def test_validate_variable_category_type_values_uses_cmid_metadata_when_label_missing(monkeypatch):
    dataset = pd.DataFrame(
        {
            "CMID": ["AM1", "AM2"],
            "categoryType": ["continuous", "numeric"],
        }
    )

    monkeypatch.setattr(
        upload,
        "_fetch_cmid_metadata",
        lambda driver, cmids, chunk_size=1500: {
            "AM1": {"labels": {"CATEGORY", "VARIABLE"}, "groupLabels": set()},
            "AM2": {"labels": {"CATEGORY", "AREA"}, "groupLabels": set()},
        },
    )

    upload._validate_variable_category_type_values(
        dataset,
        optionalProperties=["categoryType"],
        driver=object(),
    )

    assert dataset.loc[0, "categoryType"] == "CONTINUOUS"
    assert dataset.loc[1, "categoryType"] == "numeric"


def test_input_nodes_uses_rejects_invalid_variable_category_type(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='node'" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='relationship'" in query:
            return ["categoryType"]
        if "MATCH (l:LABEL) return l.CMName as label" in query:
            return ["VARIABLE"]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    with pytest.raises(ValueError, match="Invalid categoryType"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "CMID": "AM1",
                    "datasetID": "AD1",
                    "Key": "Type == Adamana Brown",
                    "label": "VARIABLE",
                    "Name": "Adamana Brown",
                    "categoryType": "numeric",
                }
            ],
            database="ArchaMap",
            uploadOption="add_uses",
            formatKey=False,
            optionalProperties=["categoryType"],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_input_nodes_uses_rejects_invalid_variable_category_type_without_label_column(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(
        upload,
        "_fetch_cmid_metadata",
        lambda driver, cmids, chunk_size=1500: {
            "AM1": {"labels": {"CATEGORY", "VARIABLE"}, "groupLabels": set()},
        },
    )

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='node'" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='relationship'" in query:
            return ["categoryType"]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    with pytest.raises(ValueError, match="Invalid categoryType"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "CMID": "AM1",
                    "datasetID": "AD1",
                    "Key": "Type == Adamana Brown",
                    "categoryType": "numeric",
                }
            ],
            database="ArchaMap",
            uploadOption="update_replace",
            formatKey=False,
            optionalProperties=["categoryType"],
            user="tester",
            addDistrict=False,
            addRecordYear=False,
            geocode=False,
        )


def test_is_same_update_add_value_matches_stringified_values():
    assert upload._is_same_update_add_value("alpha", "alpha") is True
    assert upload._is_same_update_add_value(10, "10") is True
    assert upload._is_same_update_add_value(None, "10") is False
    assert upload._is_same_update_add_value("alpha", "beta") is False


def test_validate_non_parent_multi_value_columns_raises_for_wrong_label():
    dataset = pd.DataFrame(
        {
            "CMID": ["SM251419", "SM251420"],
            "language": ["AM1", "AM2"],
        }
    )
    column_map = {"language": ["AM1", "AM2"]}
    cmid_metadata = {
        "AM1": {"labels": {"CATEGORY", "LANGUOID"}, "groupLabels": set()},
        "AM2": {"labels": {"CATEGORY", "AREA"}, "groupLabels": set()},
    }

    with pytest.raises(ValueError) as err:
        upload._validate_non_parent_multi_value_columns(dataset, column_map, cmid_metadata)

    message = str(err.value)
    assert "Wrong labels in database for column 'language'" in message
    assert "row 2" in message
    assert "CMID SM251420" in message


def test_validate_non_parent_multi_value_columns_accepts_area_for_district():
    dataset = pd.DataFrame(
        {
            "CMID": [""],
            "District": ["SM64"],
        }
    )
    column_map = {"District": ["SM64"]}
    cmid_metadata = {
        "SM64": {"labels": {"CATEGORY", "AREA", "ADM0"}, "groupLabels": {"AREA"}},
    }

    result = upload._validate_non_parent_multi_value_columns(
        dataset,
        column_map,
        cmid_metadata,
    )

    assert result is None


def test_validate_restricted_node_property_domains_rejects_wrong_domain(monkeypatch):
    dataset = pd.DataFrame(
        {
            "CMID": ["SM-LANG"],
            "FIPS": ["US"],
        }
    )
    cmid_metadata = {
        "SM-LANG": {"labels": {"CATEGORY", "LANGUOID"}, "groupLabels": {"LANGUOID"}},
    }

    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: [])

    with pytest.raises(ValueError) as err:
        upload._validate_restricted_node_property_domains(
            dataset,
            ["FIPS"],
            cmid_metadata,
            object(),
        )

    assert "FIPS is only valid for AREA nodes" in str(err.value)


def test_validate_restricted_node_property_domains_accepts_iso3_for_languoid(monkeypatch):
    dataset = pd.DataFrame(
        {
            "CMID": ["SM-LANG"],
            "ISO3": ["eng"],
        }
    )
    cmid_metadata = {
        "SM-LANG": {"labels": {"CATEGORY", "LANGUOID"}, "groupLabels": {"LANGUOID"}},
    }

    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: [])

    assert (
        upload._validate_restricted_node_property_domains(
            dataset,
            ["ISO3"],
            cmid_metadata,
            object(),
        )
        is None
    )


def test_validate_parent_label_compatibility_raises_on_mismatch(monkeypatch):
    dataset = pd.DataFrame(
        {
            "CMID": ["AM100"],
            "parent": ["AM200"],
        }
    )
    cmid_metadata = {
        "AM100": {"labels": {"CATEGORY", "LANGUOID"}, "groupLabels": {"LANGUAGE"}},
        "AM200": {"labels": {"CATEGORY", "AREA"}, "groupLabels": {"AREA"}},
    }

    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(
        upload,
        "getQuery",
        lambda query, driver, type="dict", **kwargs: [
            {"groupLabel": "LANGUAGE"},
            {"groupLabel": "AREA"},
            {"groupLabel": "GENERIC"},
        ],
    )

    with pytest.raises(ValueError) as err:
        upload._validate_parent_label_compatibility(
            dataset=dataset,
            cmid_metadata=cmid_metadata,
            driver=object(),
            user="tester",
        )
    message = str(err.value)
    assert "Mismatch at row 1" in message
    assert "Child CMID: AM100" in message
    assert "Parent CMID: AM200" in message


def test_validate_parent_label_compatibility_accepts_generic_parent(monkeypatch):
    dataset = pd.DataFrame(
        {
            "CMID": ["AM100"],
            "parent": ["AM200"],
        }
    )
    cmid_metadata = {
        "AM100": {"labels": {"CATEGORY", "LANGUOID"}, "groupLabels": {"LANGUAGE"}},
        "AM200": {"labels": {"CATEGORY", "GENERIC"}, "groupLabels": {"GENERIC"}},
    }

    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(
        upload,
        "getQuery",
        lambda query, driver, type="dict", **kwargs: [
            {"groupLabel": "LANGUAGE"},
            {"groupLabel": "GENERIC"},
        ],
    )

    result = upload._validate_parent_label_compatibility(
        dataset=dataset,
        cmid_metadata=cmid_metadata,
        driver=object(),
        user="tester",
    )
    assert result is None


def test_collect_cmid_metadata_targets_includes_child_cmids_for_parent_validation():
    dataset = pd.DataFrame(
        {
            "CMID": ["SM251419"],
            "parent": ["SM251572"],
        }
    )
    column_map = {"parent": ["SM251572"]}

    targets = upload._collect_cmid_metadata_targets(dataset, column_map)

    assert set(targets) == {"SM251419", "SM251572"}


def test_fetch_cmid_metadata_accepts_set_inputs(monkeypatch):
    captured_chunks = []

    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)

    def fake_get_query(query, driver, params=None, **kwargs):
        captured_chunks.append(params["cmids"])
        return [
            {
                "cmid": "SM64",
                "labels": ["CATEGORY", "AREA"],
                "groupLabels": ["AREA"],
            }
        ]

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    metadata = upload._fetch_cmid_metadata(object(), {"SM64"})

    assert captured_chunks == [["SM64"]]
    assert metadata["SM64"] == {
        "labels": {"CATEGORY", "AREA"},
        "groupLabels": {"AREA"},
    }


def test_resolve_group_labels_falls_back_to_node_labels_when_mapping_missing():
    metadata_entry = {
        "labels": {"CATEGORY", "ETHNICITY"},
        "groupLabels": set(),
    }

    resolved = upload._resolve_group_labels(metadata_entry)

    assert resolved == {"ETHNICITY"}


def test_update_log_stream_includes_step_and_total_elapsed_seconds(monkeypatch, tmp_path):
    streamed = []
    timestamps = iter([100.0, 100.5, 101.25])

    monkeypatch.setattr(upload.time, "monotonic", lambda: next(timestamps))

    upload.set_upload_log_listener(lambda message: streamed.append(message))
    try:
        log_file = tmp_path / "upload_progress.txt"
        upload.updateLog(str(log_file), "step one", write="w")
        upload.updateLog(str(log_file), "step two", write="a")
    finally:
        upload.clear_upload_log_listener()

    assert streamed[0].startswith("[+0.50s | 0.50s] step one")
    assert streamed[1].startswith("[+0.75s | 1.25s] step two")


def test_summarize_upload_log_payload_avoids_large_json():
    payload = [{"a": 1}, {"b": 2}]
    summary = upload._summarize_upload_log_payload(payload)
    assert summary == "<list len=2>"
