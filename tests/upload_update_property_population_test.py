import pandas as pd

import CM.upload as upload


def test_update_property_population_estimate_formats_with_string_tokens(monkeypatch):
    queries = []

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
