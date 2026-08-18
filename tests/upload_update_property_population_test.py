import pandas as pd

import CM.upload as upload


def test_update_property_population_estimate_formats_with_string_tokens(monkeypatch):
    queries = []
    updated_rows = []

    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(
        upload,
        "getPropertiesMetadata",
        lambda driver: [
            {"type": "relationship", "property": "populationEstimate", "metaType": "float"},
        ],
    )
    monkeypatch.setattr(upload, "createLog", lambda **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        queries.append(query)
        if "oldVals" in query:
            return [
                {
                    "relID": "rel-1",
                    "CMID": "SM1",
                    "Key": "K1",
                    "datasetID": "SD1",
                    "oldVals": {"populationEstimate": "100"},
                }
            ]
        if "SET r.status = 'update'" in query:
            updated_rows.extend(params["rows"])
            return [
                {
                    "nodeID": "node-1",
                    "relID": "rel-1",
                    "CMID": "SM1",
                    "Key": "K1",
                    "datasetID": "SD1",
                    "populationEstimate": "792",
                }
            ]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    df = pd.DataFrame(
        [
            {
                "CMID": "SM1",
                "datasetID": "SD1",
                "Key": "K1",
                "relID": "rel-1",
                "populationEstimate": 792.0,
            }
        ]
    )

    result = upload.updateProperty(
        df=df,
        optionalProperties=["populationEstimate"],
        isDataset=False,
        database="sociomap",
        user="tester",
        updateType="overwrite",
        propertyType="USES",
    )

    assert isinstance(result, dict)
    assert any("toString(v)" in q for q in queries)
    assert updated_rows == [{
        "CMID": "SM1",
        "datasetID": "SD1",
        "Key": "K1",
        "relID": "rel-1",
        "populationEstimate": "792",
    }]


def test_population_estimate_normalization_removes_only_insignificant_zeros():
    assert upload._normalize_population_estimate(2.0) == "2"
    assert upload._normalize_population_estimate("2.500") == "2.5"
    assert upload._normalize_population_estimate("0.0") == "0"
    assert upload._normalize_population_estimate("2.05") == "2.05"


def test_update_property_uses_relid_does_not_write_locator_fields(monkeypatch):
    captured = {"update_query": ""}

    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(
        upload,
        "getPropertiesMetadata",
        lambda driver: [
            {"type": "relationship", "property": "populationEstimate", "metaType": "float"},
            {"type": "relationship", "property": "Key", "metaType": "string"},
        ],
    )
    monkeypatch.setattr(upload, "createLog", lambda **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "oldVals" in query:
            return [
                {
                    "relID": "rel-1",
                    "CMID": "SM1",
                    "Key": "K1",
                    "datasetID": "SD1",
                    "oldVals": {"populationEstimate": "100"},
                }
            ]
        if "SET r.status = 'update'" in query:
            captured["update_query"] = query
            return [
                {
                    "nodeID": "node-1",
                    "relID": "rel-1",
                    "CMID": "SM1",
                    "Key": "K1",
                    "datasetID": "SD1",
                    "populationEstimate": "250",
                }
            ]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    df = pd.DataFrame(
        [
            {
                "CMID": "SM1",
                "datasetID": "SD1",
                "Key": "K1",
                "relID": "rel-1",
                "populationEstimate": 250,
            }
        ]
    )

    result = upload.updateProperty(
        df=df,
        optionalProperties=["populationEstimate"],
        isDataset=False,
        database="sociomap",
        user="tester",
        updateType="overwrite",
        propertyType="USES",
    )

    assert isinstance(result, dict)
    assert "r.CMID" not in captured["update_query"]
    assert "r.datasetID" not in captured["update_query"]


def test_update_property_population_estimate_update_is_null_safe(monkeypatch):
    captured = {"update_query": ""}

    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(
        upload,
        "getPropertiesMetadata",
        lambda driver: [
            {"type": "relationship", "property": "populationEstimate", "metaType": "float"},
        ],
    )
    monkeypatch.setattr(upload, "createLog", lambda **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "oldVals" in query:
            return [
                {
                    "relID": "rel-1",
                    "CMID": "SM1",
                    "Key": "K1",
                    "datasetID": "SD1",
                    "oldVals": {"populationEstimate": None},
                }
            ]
        if "SET r.status = 'update'" in query:
            captured["update_query"] = query
            return [
                {
                    "nodeID": "node-1",
                    "relID": "rel-1",
                    "CMID": "SM1",
                    "Key": "K1",
                    "datasetID": "SD1",
                    "populationEstimate": "250",
                }
            ]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    df = pd.DataFrame(
        [
            {
                "CMID": "SM1",
                "datasetID": "SD1",
                "Key": "K1",
                "relID": "rel-1",
                "populationEstimate": 250,
            }
        ]
    )

    result = upload.updateProperty(
        df=df,
        optionalProperties=["populationEstimate"],
        isDataset=False,
        database="sociomap",
        user="tester",
        updateType="update",
        propertyType="USES",
    )

    assert isinstance(result, dict)
    assert "CASE WHEN r.populationEstimate IS NULL THEN []" in captured["update_query"]
    assert "CASE WHEN row.populationEstimate IS NULL THEN []" in captured["update_query"]
    assert "apoc.coll.flatten([[v IN apoc.coll.flatten" not in captured["update_query"]
