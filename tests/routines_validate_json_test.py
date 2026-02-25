import CM.routines as routines
import pandas as pd


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
            if "pc AS parentContextEntry" in query:
                raise Exception("force fallback for schema check")
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

    assert result["invalid_parentContext_json_shape_count"] == 0
    assert result["invalid_parentContext_json_shape"] == []
