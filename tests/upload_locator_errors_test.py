import pandas as pd
import pytest

import CM.upload as upload


@pytest.mark.parametrize("matched", [[], [{"CMID": "SM1", "datasetID": "SD1", "Key": "Key == 1", "relID": "rel-1"}]])
def test_unmatched_batch_reports_locators_before_any_write(monkeypatch, matched):
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "validate_event_rows", lambda *args: None)
    queries = []

    def query(query, **kwargs):
        queries.append(query)
        assert "RETURN elementId(r) AS relID" in query
        return pd.DataFrame(matched)

    monkeypatch.setattr(upload, "getQuery", query)
    result = upload.updateProperty(
        pd.DataFrame([
            {"CMID": f"SM{i}", "datasetID": "SD1", "Key": f"Key == {i}", "label": "LANGUAGE"}
            for i in (1, 2)
        ], index=[500, 501]),
        ["label"], False, "SocioMap", "tester", "overwrite",
    )
    assert result.startswith("Error: No matching USES ties found")
    assert "no rows in this batch were changed" in result.lower()
    assert len(queries) == 1


def test_duplicate_locator_matches_fail_before_writes(monkeypatch):
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "validate_event_rows", lambda *args: None)
    row = {"CMID": "SM1", "datasetID": "SD1", "Key": "Key == 1", "label": "LANGUAGE"}
    monkeypatch.setattr(upload, "getQuery", lambda **kwargs: pd.DataFrame([
        {**row, "relID": "rel-1"}, {**row, "relID": "rel-2"},
    ]).drop(columns="label"))
    result = upload.updateProperty(pd.DataFrame([row]), ["label"], False, "SocioMap", "tester", "overwrite")
    assert result.startswith("Error:")
    assert "not unique" in result
