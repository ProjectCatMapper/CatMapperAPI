import pandas as pd
import pytest

import CM.upload as upload


def test_collect_unique_column_values_for_multi_value_column():
    dataset = pd.DataFrame({"language": ["AM1; AM2", "AM2;AM3", "", None, " AM1 "]})

    values = upload._collect_unique_column_values(dataset, "language", {"language"})

    assert set(values) == {"AM1", "AM2", "AM3"}


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
        "AM2": {"labels": {"CATEGORY", "DISTRICT"}, "groupLabels": set()},
    }

    with pytest.raises(ValueError) as err:
        upload._validate_non_parent_multi_value_columns(dataset, column_map, cmid_metadata)

    message = str(err.value)
    assert "Wrong labels in database for column 'language'" in message
    assert "row 2" in message
    assert "CMID SM251420" in message


def test_validate_parent_label_compatibility_raises_on_mismatch(monkeypatch):
    dataset = pd.DataFrame(
        {
            "CMID": ["AM100"],
            "parent": ["AM200"],
        }
    )
    cmid_metadata = {
        "AM100": {"labels": {"CATEGORY", "LANGUOID"}, "groupLabels": {"LANGUAGE"}},
        "AM200": {"labels": {"CATEGORY", "DISTRICT"}, "groupLabels": {"DISTRICT"}},
    }

    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(
        upload,
        "getQuery",
        lambda query, driver, type="dict", **kwargs: [
            {"groupLabel": "LANGUAGE"},
            {"groupLabel": "DISTRICT"},
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

    upload._validate_parent_label_compatibility(
        dataset=dataset,
        cmid_metadata=cmid_metadata,
        driver=object(),
        user="tester",
    )


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
