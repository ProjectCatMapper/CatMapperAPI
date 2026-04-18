import CM.upload as upload


def test_sync_dataset_variable_merging_metadata_updates_unique_matches(monkeypatch):
    queries = []

    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        queries.append({"query": query, "params": params, "type": type})
        if "collect(DISTINCT {Key: u.Key, categoryType: u.categoryType}) AS rawMetadata" in query:
            return [
                {
                    "relID": "rel-1",
                    "datasetID": "D1",
                    "variableID": "V1",
                    "stackID": "S1",
                    "existingKey": None,
                    "existingCategoryType": None,
                    "rawMetadata": [
                        {
                            "Key": "Feature == kiva",
                            "categoryType": "categorical",
                        }
                    ],
                }
            ]
        if "SET r.Key = row.Key" in query:
            return [{"count": len(params["rows"])}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    result = upload.sync_dataset_variable_merging_metadata(database="ArchaMap", user="tester")

    assert result["updated"] == 1
    assert result["unresolved"] == []
    assert queries[1]["params"]["rows"][0]["Key"] == "Feature == kiva"
    assert queries[1]["params"]["rows"][0]["categoryType"] == "CATEGORICAL"

