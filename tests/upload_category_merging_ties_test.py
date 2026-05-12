import pandas as pd

import CM.upload as upload


def test_create_category_merging_ties_infers_distinct_stacks(monkeypatch):
    queries = []

    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        queries.append({"query": query, "params": params, "type": type})
        if "return distinct m.CMID as mergingID, s.CMID as stackID, d.CMID as datasetID" in query:
            return pd.DataFrame(
                [
                    {"mergingID": "M1", "stackID": "S1", "datasetID": "D1"},
                    {"mergingID": "M1", "stackID": "S2", "datasetID": "D2"},
                ]
            )
        if "MERGE (d)-[r:MERGING {stack: row.stackID, Key: row.Key}]->(c)" in query:
            assert params is not None
            assert params["rows"] == [
                {
                    "categoryID": "C_SHARED",
                    "stackID": "S1",
                    "datasetID": "D1",
                    "Key": "Site == A",
                },
                {
                    "categoryID": "C_SHARED",
                    "stackID": "S2",
                    "datasetID": "D2",
                    "Key": "Site == B",
                },
            ]
            return pd.DataFrame([{"count": 2}])
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    dataset = pd.DataFrame(
        [
            {
                "mergingID": "M1",
                "datasetID": "D1",
                "categoryID": "C_SHARED",
                "Key": "Site == A",
            },
            {
                "mergingID": "M1",
                "datasetID": "D2",
                "categoryID": "C_SHARED",
                "Key": "Site == B",
            },
        ]
    )

    result = upload.create_category_merging_ties(database="ArchaMap", user="tester", dataset=dataset)

    assert result["result"]["stackID"].tolist() == ["S1", "S2"]
    assert any(
        "MERGE (d)-[r:MERGING {stack: row.stackID, Key: row.Key}]->(c)"
        in item["query"]
        for item in queries
    )
