"""Admin failures identify stored USES ties; upload failures retain row numbers."""
import json

import pandas as pd
import pytest

from CM import admin, context_policy, event_types as events, merge_references, upload
from CMroutes import admin_routes


pytestmark = pytest.mark.usefixtures("metadata_event_vocabulary")


def invalid_source():
    return {"CMID": "SM501203", "datasetID": "SD468410", "Key": "V130 == 6",
            "relID": "rel-1", "parentContext": ['{"eventType":"HIERARCHY"}']}


def assert_source_message(message):
    assert "USES tie (CMID=SM501203, datasetID=SD468410, Key='V130 == 6', relID=rel-1)" in message
    assert "parentContext parent must be a nonempty CMID string" in message
    assert "Row " not in message


@pytest.mark.parametrize("preceding_records", [0, 352])
def test_internal_list_position_does_not_become_an_admin_row_number(preceding_records):
    rows = [{"parentContext": None}] * preceding_records + [invalid_source()]
    with pytest.raises(ValueError) as error:
        events.validate_event_rows(rows, object())
    assert_source_message(str(error.value))


@pytest.mark.parametrize("explicit_row", [None, 42])
def test_spreadsheet_validation_keeps_its_row_reference(explicit_row):
    row = invalid_source()
    if explicit_row is not None:
        row["_spreadsheet_row"] = explicit_row
    with pytest.raises(ValueError, match=f"^Row {explicit_row or 1}:"):
        events.validate_event_rows([row], object(), spreadsheet=True)


def test_upload_preflight_reports_row_before_any_database_write(monkeypatch):
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "validate_upload_ownership_scope", lambda *args: None)
    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: pytest.fail("Graph write reached"))
    with pytest.raises(ValueError, match="^Row 1:"):
        upload.input_Nodes_Uses([invalid_source()], "SocioMap", "add_node", user="tester")


def test_property_edit_reports_tie_before_any_database_write(monkeypatch):
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "getQuery", lambda *args, **kwargs: pytest.fail("Graph write reached"))
    result = upload.updateProperty(pd.DataFrame([invalid_source()]), ["parentContext"],
                                   False, "SocioMap", "tester", "overwrite")
    assert_source_message(result)


def test_move_reports_stored_tie_before_mutation(monkeypatch):
    monkeypatch.setattr(admin, "getDriver", lambda _: object())

    def query(text, *args, **kwargs):
        assert "RETURN r.parentContext AS parentContext" in text
        for identity in ["c.CMID AS CMID", "d.CMID AS datasetID", "r.Key AS Key", "elementId(r) AS relID"]:
            assert identity in text
        return [invalid_source()]

    monkeypatch.setattr(admin, "getQuery", query)
    with pytest.raises(ValueError) as error:
        admin.moveUSESties("SocioMap", "tester", {
            "s1_2": "SM501203", "s1_3": "SM453864",
            "s1_7": json.dumps([{}, {"id": "rel-1"}]),
        }, "SD468410", [])
    assert_source_message(str(error.value))


def test_merge_reports_original_category_and_source_key_before_mutation(monkeypatch):
    row = invalid_source()

    def query(text, *args, **kwargs):
        if "RETURN m.CMName AS property" in text:
            return ["parent"]
        assert "properties(r) AS properties" in text
        return [{**row, "properties": {"parentContext": row["parentContext"]}}]

    monkeypatch.setattr(merge_references, "getQuery", query)
    plan = merge_references.plan_merge_references(object(), "SM453864", row["CMID"])
    assert plan["validationRows"][0]["CMID"] == "SM453864"
    with pytest.raises(ValueError) as error:
        merge_references.validate_merge_plan(object(), plan)
    assert_source_message(str(error.value))


@pytest.mark.parametrize("operation", ["merge", "replace"])
def test_malformed_stored_json_identifies_tie_without_writing(monkeypatch, operation):
    row = {**invalid_source(), "value": ["{bad JSON}"]}

    def query(text, *args, **kwargs):
        if "RETURN m.CMName AS property" in text:
            return ["parent"]
        assert "SET " not in text
        return [{**row, "properties": {"parentContext": row["value"]}}]

    if operation == "merge":
        monkeypatch.setattr(merge_references, "getQuery", query)
        with pytest.raises(ValueError) as error:
            merge_references.plan_merge_references(object(), "SM453864", row["CMID"])
        message = str(error.value)
    else:
        monkeypatch.setattr(admin, "getDriver", lambda _: object())
        monkeypatch.setattr(admin, "getQuery", query)
        message, status = admin.replaceProperty([row["CMID"]], "parentContext", "SM2", "SM3", "SocioMap")
        assert status == 500
    assert "CMID=SM501203" in message and "datasetID=SD468410" in message
    assert "Key='V130 == 6'" in message and "Invalid parentContext JSON" in message
    assert "Row " not in message


def test_reconciliation_error_includes_stored_source_identifiers(monkeypatch):
    def query(text, *args, **kwargs):
        if "allowedValues" in text:
            return [{"allowedValues": ["HIERARCHY"]}]
        assert "d.CMID AS datasetID" in text and "r.Key AS Key" in text
        return [{**invalid_source(), "parentContext": '{"parent":"SM2","eventType":"WRONG"}'}]

    monkeypatch.setattr(events, "getQuery", query)
    with pytest.raises(ValueError) as error:
        events.validate_stored_event_contexts(object(), ["SM501203"])
    message = str(error.value)
    assert "CMID=SM501203" in message and "datasetID=SD468410" in message
    assert "Key='V130 == 6'" in message and "Invalid eventType" in message
    assert "Row " not in message


def test_admin_contextual_policy_error_identifies_selected_tie(monkeypatch):
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    policy = context_policy.parse_policy([
        {"label": "AREA", "groupLabel": "AREA", "contextualTargetMode": "NONE"},
        {"label": "LANGUOID", "groupLabel": "LANGUOID", "relationship": "LANGUOID_OF",
         "contextualTargetMode": "ONLY", "contextualTargetDomains": []},
    ], [{"property": "language", "groupLabel": "LANGUOID", "relationship": "LANGUOID_OF"}])
    monkeypatch.setattr(context_policy, "load_policy", lambda _: policy)
    monkeypatch.setattr(context_policy, "getQuery", lambda *args, **kwargs: [
        {"cmid": "SM501203", "labels": ["CATEGORY", "AREA"]},
        {"cmid": "SM2", "labels": ["CATEGORY", "LANGUOID"]},
    ])
    monkeypatch.setattr(admin_routes, "getDriver", lambda _: object())

    def query(*args, **kwargs):
        assert "RETURN properties(r) AS properties" in kwargs["query"]
        return [{"properties": {"Key": "V130 == 6"}}]

    monkeypatch.setattr(admin_routes, "getQuery", query)
    with pytest.raises(admin_routes.InvalidUsesEdit) as error:
        admin_routes._validate_uses_edit_request("SocioMap", "add/edit/delete USES property", {
            "s1_1": "add", "s1_2": "SM501203", "s1_3": "SM2", "s1_7": 1,
            "s1_8": "language", "s1_4": [[{"CMID": "SM501203"},
                {"id": "rel-1", "Key": "V130 == 6"}, {"CMID": "SD468410"}]],
        })
    message = str(error.value)
    assert "USES tie (CMID=SM501203, datasetID=SD468410, Key='V130 == 6', relID=rel-1)" in message
    assert "LANGUOID_OF" in message and "Row " not in message and "row " not in message
