"""CATMA-324: blank contexts are optional, while populated ones remain strict."""
import copy
import json

import pandas as pd
import pytest

from CM import admin, event_types as events, merge_references, upload


pytestmark = pytest.mark.usefixtures("metadata_event_vocabulary")


@pytest.mark.parametrize("value", [
    {}, "{}", ["{}"], "[{}]",
    {"parent": "", "eventType": "", "eventDate": ""},
    {"parent": None, "eventType": None, "eventDate": float("nan")},
    '{"parent":"  ","eventType":" ","eventDate":null}',
])
def test_empty_objects_do_not_assert_a_parent_or_event(value):
    assert events.validate_parent_context_events(value, ["HIERARCHY"]) == []
    events.validate_event_rows([{"parentContext": value}], object())


@pytest.mark.parametrize("value", [
    {"eventType": "HIERARCHY"}, {"parent": "", "eventDate": "1999"},
    {"note": "Preserve this context"}, {"parent": []}, {"parent": 12},
])
def test_nonempty_parentless_contexts_still_fail(value):
    with pytest.raises(ValueError, match="parent must be a nonempty CMID string"):
        events.validate_event_rows([{"parentContext": value}], object())


def test_mixed_contexts_keep_the_populated_event_without_mutating_input():
    value = ["{}", '{"parent":"SM2","eventType":"SPLIT","eventDate":"1999"}', "{}"]
    original = copy.deepcopy(value)
    assert events.validate_parent_context_events(value, ["SPLIT"]) == [
        {"parent": "SM2", "eventType": "SPLIT", "eventDate": "1999"},
    ]
    assert value == original


@pytest.mark.parametrize("invalid,error", [
    ("{bad JSON}", "Invalid parentContext JSON"),
    ('{"eventType":"HIERARCHY"}', "parent must be a nonempty CMID string"),
    ('{"parent":"SM2","eventType":"UNKNOWN"}', "Invalid eventType"),
    ('{"parent":"SM2","eventDate":"1999"}', "eventType is required"),
])
def test_blank_object_does_not_hide_an_invalid_sibling(invalid, error):
    with pytest.raises(ValueError, match=error):
        events.validate_event_rows([{"parentContext": ["{}", invalid]}], object())


class UploadCaptured(Exception):
    """Stop at the relationship writer, with every database call stubbed."""


@pytest.fixture
def upload_graph(monkeypatch):
    captured = {"nodes": [], "links": None}
    monkeypatch.setattr(upload, "getDriver", lambda _: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "legacy")
    monkeypatch.setattr(upload, "getPropertiesMetadata", lambda _: [
        {"property": prop, "group": "parentContext"}
        for prop in ["parent", "eventType", "eventDate"]
    ])

    def query(text, driver=None, params=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL" in text:
            return []
        if "MATCH (p:PROPERTY)" in text:
            return ["parent", "eventType", "eventDate"] if "type='relationship'" in text else []
        if "MATCH (n:PROPERTY)" in text:
            return []
        if "return l.CMName as label" in text:
            return ["RELIGION"]
        if "RETURN labelValue, n.groupLabel AS groupLabel" in text:
            return [{"labelValue": "RELIGION", "groupLabel": "RELIGION"}]
        if "RETURN DISTINCT n.groupLabel AS groupLabel" in text:
            return [{"groupLabel": "RELIGION"}]
        if "COUNT(n) AS count" in text:
            return [{"value": row["value"], "count": 1} for row in params["rows"]]
        if "nodeLabels AS labels" in text:
            return [{"cmid": cmid, "labels": ["CATEGORY", "RELIGION"],
                     "groupLabels": ["RELIGION"]} for cmid in params["cmids"]]
        if "WITH DISTINCT row.datasetID AS datasetID, row.Key AS keyValue" in text:
            return []
        raise AssertionError(f"Unexpected query: {text}")

    def create_nodes(frame, *args, **kwargs):
        captured["nodes"].extend(frame.to_dict(orient="records"))
        return pd.DataFrame([{"importID": row["importID"], "CMID": f"SM{100 + i}",
                              "nodeID": f"node-{i}"} for i, row in frame.iterrows()])

    def create_links(links, **kwargs):
        captured["links"] = links.to_dict(orient="records")
        raise UploadCaptured

    monkeypatch.setattr(upload, "getQuery", query)
    monkeypatch.setattr(upload, "createNodes", create_nodes)
    monkeypatch.setattr(upload, "createUSES", create_links)
    return captured


def spreadsheet_rows():
    return [{"CMName": f"Religion {i}", "Name": f"Religion {i}", "label": "RELIGION",
             "datasetID": "SD1", "Key": f"ID == {i}", "parent": parent,
             "eventType": event, "eventDate": date}
            for i, (parent, event, date) in enumerate([
                (None, None, None), ("SM2", None, None), ("SM2", "SPLIT", 1999),
            ])]


def test_upload_with_blank_parent_and_event_cells_reaches_writer(upload_graph):
    with pytest.raises(UploadCaptured):
        upload.input_Nodes_Uses(spreadsheet_rows(), "SocioMap", "add_node", user="tester",
                                optionalProperties=["parent", "eventType", "eventDate"])
    rows = upload_graph["links"]
    assert len(rows) == 3
    assert rows[0]["parentContext"] == ""
    assert rows[1]["parent"] == "SM2" and rows[1]["parentContext"] == ""
    assert json.loads(rows[2]["parentContext"]) == {
        "parent": "SM2", "eventType": "SPLIT", "eventDate": "1999",
    }


def test_upload_still_rejects_new_parentless_event_before_node_creation(upload_graph):
    rows = spreadsheet_rows()
    rows[0]["eventType"] = "HIERARCHY"
    with pytest.raises(ValueError, match="parent must be a nonempty CMID string"):
        upload.input_Nodes_Uses(rows, "SocioMap", "add_node", user="tester",
                                optionalProperties=["parent", "eventType", "eventDate"])
    assert upload_graph["nodes"] == []
    assert upload_graph["links"] is None


def test_merge_preserves_stored_empty_objects_and_rewrites_real_parent(monkeypatch):
    contexts = ["{}", '{"parent":"SM2","eventType":"HIERARCHY"}']
    source = {"relID": "rel-1", "CMID": "SM453864", "datasetID": "SD468410",
              "Key": "V130 == 6", "properties": {"parentContext": contexts}}
    monkeypatch.setattr(merge_references, "getQuery", lambda text, *args, **kwargs:
                        ["parent"] if "RETURN m.CMName AS property" in text else [source])
    plan = merge_references.plan_merge_references(object(), "SM453864", "SM2")
    merge_references.validate_merge_plan(object(), plan)
    updated = plan["updates"][0]["properties"]["parentContext"]
    assert updated[0] == "{}"
    assert json.loads(updated[1]) == {"parent": "SM453864", "eventType": "HIERARCHY"}
    assert contexts == ["{}", '{"parent":"SM2","eventType":"HIERARCHY"}']


def test_move_with_all_from_options_preserves_empty_stored_context(monkeypatch):
    stored = ["{}"]
    writes = []
    monkeypatch.setattr(admin, "getDriver", lambda _: object())
    monkeypatch.setattr(admin, "USESLogText", lambda *args: "example tie")
    monkeypatch.setattr(admin, "getID", lambda *args: "node-1")
    monkeypatch.setattr(admin, "createLog", lambda **kwargs: None)
    monkeypatch.setattr(admin, "processUSES", lambda **kwargs: "processed")

    def query(text, *args, **kwargs):
        if "RETURN r.parentContext AS parentContext" in text:
            return [{"parentContext": stored}]
        assert "apoc.refactor.to(r, to)" in text
        writes.append(kwargs["params"])
        return [{"relID": "rel-2"}]

    monkeypatch.setattr(admin, "getQuery", query)
    result = admin.moveUSESties("SocioMap", "tester", {
        "s1_2": "SM452473", "s1_3": "SM24787",
        "s1_7": json.dumps([{}, {"id": "rel-1"}]),
    }, "SD468410", [{"CMID": "SM3", "optionA": "From"}])
    assert result == "done"
    assert writes[0]["CMID_to"] == "SM24787"
    assert stored == ["{}"]
