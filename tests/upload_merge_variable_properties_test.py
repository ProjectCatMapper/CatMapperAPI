import pandas as pd

import CM.upload as upload


def test_create_mties_variables_persists_filter_and_summary_properties(monkeypatch):
    queries = []

    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        queries.append({"query": query, "params": params, "type": type})
        if "Match (m:MERGING {CMID: row.mergingID})-[:MERGING]->(s:STACK)-[:MERGING]->(d:DATASET {CMID: row.datasetID})" in query:
            return pd.DataFrame([{"mergingID": "M1", "stackID": "S1", "datasetID": "D1"}])
        if "MATCH (m:STACK {CMID: row.stackID})" in query:
            return [{"count": 1}]
        if "MATCH (d:DATASET {CMID: row.datasetID})" in query:
            return [{"count": 1}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    dataset = pd.DataFrame(
        [
            {
                "mergingID": "M1",
                "datasetID": "D1",
                "variableID": "V1",
                "varName": "value_a",
                "stackTransform": '[{"op":"as_numeric","target":"value_a"}]',
                "variableFilter": '[{"op":"drop_na","target":"value_a"}]',
                "summaryStatistic": "mean",
                "summaryFilter": "",
                "summaryWeight": "",
                "datasetTransform": '[{"op":"copy","target":"value_a","sources":["raw_a"]}]',
            }
        ]
    )

    result = upload.create_mties_variables(database="ArchaMap", user="tester", dataset=dataset)

    assert result["result"].iloc[0]["variableID"] == "V1"
    stack_params = queries[1]["params"]["rows"][0]
    assert stack_params["properties"]["variableFilter"] == '[{"op":"drop_na","target":"value_a"}]'
    assert stack_params["properties"]["summaryStatistic"] == "mean"
    dataset_params = queries[2]["params"]["rows"][0]
    assert dataset_params["properties"]["datasetTransform"] == '[{"op":"copy","target":"value_a","sources":["raw_a"]}]'
