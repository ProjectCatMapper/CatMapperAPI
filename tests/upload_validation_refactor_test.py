import pandas as pd
import pytest

import CM.upload as upload


def test_collect_unique_column_values_for_multi_value_column():
    dataset = pd.DataFrame({"language": ["AM1; AM2", "AM2;AM3", "", None, " AM1 "]})

    values = upload._collect_unique_column_values(dataset, "language", {"language"})

    assert set(values) == {"AM1", "AM2", "AM3"}


def test_validate_non_parent_multi_value_columns_raises_for_wrong_label():
    column_map = {"language": ["AM1", "AM2"]}
    cmid_metadata = {
        "AM1": {"labels": {"CATEGORY", "LANGUOID"}, "groupLabels": set()},
        "AM2": {"labels": {"CATEGORY", "DISTRICT"}, "groupLabels": set()},
    }

    with pytest.raises(ValueError, match="Wrong labels in database for column 'language'"):
        upload._validate_non_parent_multi_value_columns(column_map, cmid_metadata)


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

    with pytest.raises(ValueError, match="Mismatch at row 1"):
        upload._validate_parent_label_compatibility(
            dataset=dataset,
            cmid_metadata=cmid_metadata,
            driver=object(),
            user="tester",
        )


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
