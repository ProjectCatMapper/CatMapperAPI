import json

import pandas as pd
import pytest

import CM.event_types as events
import CM.USES as uses
import CM.upload as upload
import CM.admin as admin


@pytest.fixture
def vocabulary(monkeypatch):
    values = ["SPLIT", "HIERARCHY", "CUSTOM"]
    monkeypatch.setattr(events, "getQuery", lambda query, driver: [{"allowedValues": values.copy()}])
    return values


def test_vocabulary_is_loaded_fresh_from_metadata(vocabulary):
    assert events.get_allowed_event_types(object()) == tuple(vocabulary)
    vocabulary.append("NEW_EVENT")
    assert "NEW_EVENT" in events.get_allowed_event_types(object())
    events.validate_event_rows([{"parentContext": '{"parent":"SM1","eventType":"NEW_EVENT"}'}], object())


@pytest.mark.parametrize("values", [None, [], "SPLIT", ["SPLIT", "SPLIT"], [" SPLIT"], [2]])
def test_missing_or_malformed_vocabulary_fails_closed(monkeypatch, values):
    monkeypatch.setattr(events, "getQuery", lambda *args: [{"allowedValues": values}])
    with pytest.raises(ValueError, match="allowedValues"):
        events.get_allowed_event_types(object())


@pytest.mark.parametrize("rows", [[], [{"allowedValues": ["SPLIT"]}] * 2])
def test_missing_or_duplicate_metadata_node_fails_closed(monkeypatch, rows):
    monkeypatch.setattr(events, "getQuery", lambda *args: rows)
    with pytest.raises(ValueError, match="one eventType"):
        events.get_allowed_event_types(object())


@pytest.mark.parametrize("value", [
    '{"parent":"SM1","eventType":"WRONG"}',
    ['{"parent":"SM1","eventType":"WRONG"}'],
    {"parent": "SM1", "eventType": "WRONG"},
    '[{"parent":"SM1","eventType":"WRONG"}]',
    '{"parent":"SM1","eventDate":"1999"}',
])
def test_invalid_embedded_event_is_rejected(vocabulary, value):
    with pytest.raises(ValueError, match="eventType"):
        events.validate_event_rows([{"parentContext": value}], object())


def test_combined_json_and_optional_hierarchy_context(vocabulary):
    value = '{"parent":"SM1","eventType":"CUSTOM","note":"inside;value"};{"parent":"SM2"}'
    assert len(events.validate_parent_context_events(value, vocabulary)) == 2
    events.validate_event_rows([{"parentContext": None}, {"eventType": ""}], object())


def test_admin_validator_uses_metadata_values(vocabulary, monkeypatch):
    monkeypatch.setattr(admin, "getQuery", lambda *args, **kwargs: [{"cmidExists": True}])
    assert admin.validate_parent_context_list(object(), ['{"parent":"SM1","eventType":"CUSTOM"}']) == []
    assert "Invalid eventType" in admin.validate_parent_context_list(
        object(), ['{"parent":"SM1","eventType":"MERGED"}'])[0][1]


@pytest.mark.parametrize("operation", ["create", "update", "pipeline"])
def test_all_upload_entrypoints_reject_invalid_json_before_writes(vocabulary, monkeypatch, operation):
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "validate_upload_ownership_scope", lambda *args: None)
    def forbidden(*args, **kwargs):
        pytest.fail("An invalid event reached a graph write")
    monkeypatch.setattr(upload, "getQuery", forbidden)
    df = pd.DataFrame([{"datasetID": "SD1", "CMID": "SM1", "Key": "ID == 1",
        "parentContext": '{"parent":"SM2","eventType":"WRONG"}'}])
    if operation == "create":
        with pytest.raises(ValueError, match="Invalid eventType"):
            upload.createUSES(df, "SocioMap", "tester")
    elif operation == "update":
        result = upload.updateProperty(df, ["parentContext"], False, "SocioMap", "tester", "overwrite")
        assert result.startswith("Error:") and "Invalid eventType" in result
    else:
        with pytest.raises(ValueError, match="Invalid eventType"):
            upload.input_Nodes_Uses(df, "SocioMap", "update_replace", user="tester")


def test_context_write_processes_returned_category_and_reports_failure(monkeypatch):
    calls = []
    monkeypatch.setattr(upload, "processUSES", lambda **kwargs: calls.append(kwargs))
    upload._sync_updated_uses_context("SocioMap", [{"CMID": "SM2"}, {"CMID": "SM2"}], ["parentContext"], "tester")
    assert calls == [{"database": "SocioMap", "CMID": ["SM2"], "user": "tester"}]
    monkeypatch.setattr(upload, "processUSES", lambda **kwargs: ("failed", 500))
    with pytest.raises(RuntimeError, match="processUSES failed"):
        upload._sync_updated_uses_context("SocioMap", [{"CMID": "SM2"}], ["parent"], "tester")


def test_reconciliation_clears_old_parent_events_and_preserves_paired_dates(vocabulary, monkeypatch):
    monkeypatch.setattr(uses, "getDriver", lambda _: object())
    changes = []
    def query(query, driver, params):
        if "collect(u.parentContext) AS contexts" in query:
            assert params["cmids"] == ["SMchild"]
            return [{"CMID": "SMchild", "ties": [
                {"relID": "old", "parent": "SMold", "eventType": ["SPLIT"], "eventDate": ["1999"]},
                {"relID": "new", "parent": "SMnew", "eventType": None, "eventDate": None},
            ], "contexts": [[
                '{"parent":"SMnew","eventType":"CUSTOM","eventDate":"2018"}',
                '{"parent":"SMnew","eventType":"HIERARCHY"}',
                '{"parent":"SMnew","eventType":"CUSTOM","eventDate":"2018"}',
            ]]}]
        changes.extend(params["updates"])
        return [{"updatedRelationships": len(changes)}]
    monkeypatch.setattr(uses, "getQuery", query)
    result = uses.updateContains("SocioMap", "SMchild")
    assert isinstance(result, dict)
    assert changes == [
        {"relID": "old", "eventType": None, "eventDate": None},
        {"relID": "new", "eventType": ["CUSTOM", "HIERARCHY"], "eventDate": ["2018", "NULL"]},
    ]


def test_reconciliation_validates_all_sources_before_changing_any_edges(vocabulary, monkeypatch):
    monkeypatch.setattr(uses, "getDriver", lambda _: object())
    def query(query, driver, params):
        assert "SET r.eventType" not in query
        return [{"CMID": "SM1", "ties": [], "contexts": ['{"parent":"SM2","eventType":"WRONG"}']}]
    monkeypatch.setattr(uses, "getQuery", query)
    result = uses.updateContains("SocioMap", ["SM1"])
    assert result[1] == 500 and "Invalid eventType" in result[0]


def test_nightly_checks_source_and_derived_events_against_metadata(vocabulary, monkeypatch):
    import CM.routines as routines
    monkeypatch.setattr(routines, "getDriver", lambda _: object())
    monkeypatch.setattr(routines, "validateJSON", lambda **kwargs: [])
    derived_vocabulary = []
    def query(query, driver, type=None, **kwargs):
        if "invalidEventType" in query:
            derived_vocabulary.extend(kwargs['allowedEventTypes'])
            return pd.DataFrame()
        if type == "dict" and "r.parentContext AS parentContext" in query:
            return [{"datasetID": "SD11", "CMID": "SM1", "Key": "ID == 1",
                "parentContext": ['{"parent":"SM2","eventType":"MERGED"}']}]
        return pd.DataFrame() if type == "df" else []
    monkeypatch.setattr(routines, "getQuery", query)
    result = routines.getBadComplexProperties("SocioMap", return_type="data")
    assert derived_vocabulary == vocabulary
    assert result['invalid_source_eventType_count'] == 1
    assert result['invalid_source_eventType'][0]['CMID'] == 'SM1'
    assert "Invalid eventType" in result['invalid_source_eventType'][0]['error']


def test_process_uses_validates_before_any_reconciliation_write(vocabulary, monkeypatch):
    monkeypatch.setattr(uses, "getDriver", lambda _: object())
    monkeypatch.setattr(uses, "getPropertiesMetadata", lambda **_: [])
    def query(query, driver, params=None):
        assert "RETURN r.parentContext AS parentContext" in query
        return [{"parentContext": '{"parent":"SM2","eventType":"WRONG"}'}]
    original = events.getQuery
    monkeypatch.setattr(events, "getQuery", lambda query_text, driver, params=None:
        original(query_text, driver) if "allowedValues" in query_text else query(query_text, driver, params))
    monkeypatch.setattr(uses, "updateAltNames", lambda **kwargs: pytest.fail("Reconciliation wrote before validation"))
    result = uses.processUSES("SocioMap", CMID="SM1")
    assert result[1] == 500 and "Invalid eventType" in result[0]


def test_existing_whitespace_is_canonicalized_only_for_reconciliation(vocabulary):
    original = {"parent": "SM1", "eventType": "SPLIT ", "eventDate": "1999"}
    with pytest.raises(ValueError, match="Invalid eventType"):
        events.validate_parent_context_events(original, vocabulary)
    rebuilt = events.validate_parent_context_events(original, vocabulary, normalize_legacy=True)
    assert rebuilt[0]["eventType"] == "SPLIT"
    assert original["eventType"] == "SPLIT "
    with pytest.raises(ValueError, match="Invalid eventType"):
        events.validate_parent_context_events({"eventType": "UNKNOWN "}, vocabulary, normalize_legacy=True)


def test_legacy_case_and_date_only_contexts_do_not_introduce_unapproved_events(vocabulary):
    assert events.validate_parent_context_events(
        {"parent": "SM1", "eventType": "Hierarchy"}, vocabulary,
        normalize_legacy=True)[0]['eventType'] == 'HIERARCHY'
    context = {"parent": "SM1", "eventDate": "2005"}
    with pytest.raises(ValueError, match="eventType is required"):
        events.validate_parent_context_events(context, vocabulary)
    assert 'eventType' not in events.validate_parent_context_events(
        context, vocabulary, normalize_legacy=True)[0]
