import pytest
import CM.routines as routines
import pandas as pd


pytestmark = pytest.mark.usefixtures("metadata_event_vocabulary")


def test_is_valid_json_accepts_json_string_and_mapping():
    assert routines.is_valid_json('{"parent":"AM27636","eventDate":"420","eventType":"FOLLOWS"}') is True
    assert routines.is_valid_json({"parent": "AM27636", "eventDate": "420", "eventType": "FOLLOWS"}) is True
    assert routines.is_valid_json("{bad json}") is False


def test_validateJSON_does_not_flag_map_values_as_invalid(monkeypatch, tmp_path):
    monkeypatch.setattr(routines, "getDriver", lambda _database: object())
    monkeypatch.setattr(
        routines,
        "getQuery",
        lambda _query, _driver: [
            {
                "datasetID": "D1",
                "CMID": "C1",
                "Key": "K1",
                "prop": {"parent": "AM27636", "eventDate": "420", "eventType": "FOLLOWS"},
            },
            {
                "datasetID": "D2",
                "CMID": "C2",
                "Key": "K2",
                "prop": [
                    {"parent": "AM1", "eventDate": "1000", "eventType": "FOLLOWS"},
                    {"parent": "AM2", "eventDate": "1500", "eventType": "FOLLOWS"},
                ],
            },
            {
                "datasetID": "D3",
                "CMID": "C3",
                "Key": "K3",
                "prop": "{bad json}",
            },
        ],
    )

    output_file = tmp_path / "invalid_json.xlsx"
    invalid = routines.validateJSON(database="ArchaMap", property="parentContext", path=str(output_file))

    assert output_file.exists()
    assert [entry["CMID"] for entry in invalid] == ["C3"]


def test_getBadComplexProperties_allows_negative_eventDate(monkeypatch):
    monkeypatch.setattr(routines, "validateJSON", lambda database, property, path: [])
    monkeypatch.setattr(routines, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver, type=None, **kwargs):
        if type == "df":
            if "parentList AS parentValues" in query:
                return pd.DataFrame()
            if "parentCMID AS missingParent" in query:
                return pd.DataFrame()
            return pd.DataFrame()

        if type == "dict" and "r.parentContext AS parentContext" in query:
            return [
                {
                    "datasetID": "D1",
                    "CMID": "C1",
                    "Key": "K1",
                    "parentContext": [
                        {"parent": "AM1", "eventDate": "-5", "eventType": "FOLLOWS"}
                    ],
                }
            ]

        if type == "list":
            return ["AM1"]

        return []

    monkeypatch.setattr(routines, "getQuery", fake_get_query)

    result = routines.getBadComplexProperties(database="ArchaMap", return_type="data")

    assert "invalid_parentContext_json_shape_count" not in result
    assert "invalid_parentContext_json_shape" not in result


def test_getBadComplexProperties_restores_contains_eventType_check(monkeypatch):
    queries = []

    monkeypatch.setattr(routines, "validateJSON", lambda database, property, path: [])
    monkeypatch.setattr(routines, "getDriver", lambda _database: object())

    def fake_get_query(query, driver, type=None, **kwargs):
        queries.append(query)
        if type == "df" and "invalidEventType" in query:
            return pd.DataFrame([
                {
                    "sourceCMID": "SM1",
                    "sourceCMName": "Parent",
                    "targetCMID": "SM2",
                    "targetCMName": "Child",
                    "invalidEventType": "SPLIT ",
                    "eventTypes": ["SPLIT "],
                }
            ])
        return pd.DataFrame()

    monkeypatch.setattr(routines, "getQuery", fake_get_query)

    result = routines.getBadComplexProperties(database="SocioMap", return_type="info")

    assert "Invalid CONTAINS eventType: 1" in result["info"]
    event_type_query = next(query for query in queries if "invalidEventType" in query)
    assert "MATCH (source)-[r:CONTAINS]->(target)" in event_type_query
    assert "valueType(r.eventType) STARTS WITH 'LIST'" in event_type_query
    assert "WITH source, target, r, eventType" in event_type_query
    assert "$allowedEventTypes" in event_type_query


def test_get_duplicate_triplets_suppresses_email_when_send_email_false(monkeypatch):
    class FakeMail:
        pass

    monkeypatch.setattr(routines, "Mail", FakeMail)
    monkeypatch.setattr(routines, "getDriver", lambda _database: object())

    def fake_get_query(query, driver, type=None, **kwargs):
        if type == "df":
            assert "l.groupLabel AS groupLabel" in query
            assert "COUNT(DISTINCT r) AS rel_count" in query
            assert "l.CMName <> 'CATEGORY'" in query
            return pd.DataFrame([
                {
                    "datasetID": "AD354481",
                    "groupLabel": "OBJECT",
                    "Key": "Name == Bolen Side Notch",
                    "CMIDs": ["AM354486", "AM354487"],
                    "rel_count": 2,
                }
            ])
        return []

    def fake_send_email(*args, **kwargs):
        raise AssertionError("Admin duplicate triplet routine should not send email")

    monkeypatch.setattr(routines, "getQuery", fake_get_query)
    monkeypatch.setattr(routines, "sendEmail", fake_send_email)

    result = routines.get_duplicate_triplets(
        database="ArchaMap",
        mail=FakeMail(),
        return_type="data",
        send_email=False,
    )

    assert result["Total"] == 1
    assert result["Duplicate Triplets"][0]["groupLabel"] == "OBJECT"
    assert result["Duplicate Triplets"][0]["CMIDs"] == ["AM354486", "AM354487"]


def test_getDuplicateNodeCMIDs_checks_category_dataset_and_deleted(monkeypatch, tmp_path):
    captured = {}

    monkeypatch.setattr(routines, "getDriver", lambda _database: object())

    def fake_get_query(query, driver, type=None, **kwargs):
        captured["query"] = query
        captured["type"] = type
        return pd.DataFrame([
            {
                "CMID": "AM1",
                "duplicateNodeCount": 2,
                "nodeID": "1",
                "labels": ["CATEGORY"],
                "CMName": "Current",
                "replacementCMID": None,
                "replacementCMName": None,
            },
            {
                "CMID": "AM1",
                "duplicateNodeCount": 2,
                "nodeID": "2",
                "labels": ["DELETED"],
                "CMName": "Old",
                "replacementCMID": "AM2",
                "replacementCMName": "Replacement",
            },
        ])

    monkeypatch.setattr(routines, "getQuery", fake_get_query)
    monkeypatch.setattr(routines.tempfile, "NamedTemporaryFile", lambda **kwargs: open(tmp_path / "dupes.xlsx", "wb"))

    result = routines.getDuplicateNodeCMIDs(database="ArchaMap", return_type="info")

    assert result["info"] == "Duplicate CMID groups: 1; Duplicate node rows: 2"
    assert result["filepath"] == str(tmp_path / "dupes.xlsx")
    assert captured["type"] == "df"
    assert "n:CATEGORY OR n:DATASET OR n:DELETED" in captured["query"]
    assert "trim(toString(n.CMID)) <> \"\"" in captured["query"]
    assert "OPTIONAL MATCH (n)-[:IS]->(replacement)" in captured["query"]


def test_getBadContextual_self_loop_uses_referenceKey_property(monkeypatch):
    queries = []

    monkeypatch.setattr(routines, "getDriver", lambda _database: object())

    def fake_get_query(query, driver, type=None, **kwargs):
        queries.append(query)
        return pd.DataFrame()

    monkeypatch.setattr(routines, "getQuery", fake_get_query)

    result = routines.getBadContextual(database="SocioMap", return_type="info")

    assert result["filepath"] == [None, None, None]
    self_loop_query = next(query for query in queries if "MATCH (n)-[r:CONTAINS]->(n)" in query)
    assert "coalesce(r.referenceKey, [])" in self_loop_query
    assert "referenceKeys" not in self_loop_query
    assert "GADM3.6" in self_loop_query
    assert "GeoNames202005" in self_loop_query


def test_getInappropriateprops_Nodes_Rels_includes_uses_key(monkeypatch):
    queries = []

    monkeypatch.setattr(routines, "getDriver", lambda _database: object())

    def fake_get_query(query, driver, type=None, **kwargs):
        queries.append(query)
        if "MATCH (p:PROPERTY)" in query and "p.type = \"relationship\"" in query:
            return pd.DataFrame([
                {
                    "n": "SM1",
                    "d": "SD1",
                    "Key": "Name == example",
                    "invalidProps": ["badProp"],
                }
            ])
        return pd.DataFrame()

    monkeypatch.setattr(routines, "getQuery", fake_get_query)

    result = routines.getInappropriateprops_Nodes_Rels(
        database="SocioMap",
        return_type="data",
    )

    invalid_uses = result["USES with invalid props"]
    assert invalid_uses[0]["Key"] == "Name == example"
    assert any("r.Key AS Key" in query for query in queries)


def test_parent_context_missing_parent_is_reported_in_nightly_summary(monkeypatch):
    import json

    monkeypatch.setattr(routines, "validateJSON", lambda **kwargs: [])
    monkeypatch.setattr(routines, "getDriver", lambda _database: object())
    contexts = [
        {"parent": "SM3913", "eventType": "SPLIT", "eventDate": "2008"},
        {"parent": "SM4639", "eventType": "HIERARCHY"},
    ]
    source_rows = [
        {"datasetID": "SD11", "CMID": "SM599", "Key": "Name == Barh el Ghazel",
         "parentValues": ["SM3913"], "parentContext": [json.dumps(c) for c in contexts]},
        # Existing scalar JSON and mapping representations must also be checked.
        {"datasetID": "SD12", "CMID": "SM600", "Key": "scalar",
         "parentValues": None, "parentContext": json.dumps(contexts[1])},
        {"datasetID": "SD13", "CMID": "SM601", "Key": "map",
         "parentValues": [], "parentContext": contexts[1]},
        {"datasetID": "SD14", "CMID": "SM602", "Key": "maps",
         "parentValues": "SM3913, SM4639", "parentContext": contexts},
        # Bad JSON must not suppress findings from other valid entries.
        {"datasetID": "SD15", "CMID": "SM603", "Key": "mixed",
         "parentValues": ["SM3913"],
         "parentContext": ["{bad json}", json.dumps(contexts[1]), "{}", "[]", ""]},
    ]

    def fake_get_query(query, driver, type=None, **kwargs):
        if type == "dict" and "r.parentContext AS parentContext" in query:
            assert "r.parent AS parentValues" in query
            return source_rows
        return pd.DataFrame() if type == "df" else []

    monkeypatch.setattr(routines, "getQuery", fake_get_query)
    result = routines.getBadComplexProperties("SocioMap", return_type="data")
    assert result["parentContext_parent_not_in_parent_count"] == 4
    findings = result["parentContext_parent_not_in_parent"]
    assert [r["CMID"] for r in findings] == ["SM599", "SM600", "SM601", "SM603"]
    assert findings[0]["contextParent"] == "SM4639"
    assert findings[0]["parentValues"] == ["SM3913"]

    summary = routines.getBadComplexProperties("SocioMap", return_type="info")
    assert "CMID in parentContext but not in parent: 4" in summary["info"]
    attachment = next(p for p in summary["filepath"] if p and "CMID_in_parentContext_not_in_parent" in p)
    sheet = pd.read_excel(attachment)
    assert sheet["CMID"].tolist() == ["SM599", "SM600", "SM601", "SM603"]
