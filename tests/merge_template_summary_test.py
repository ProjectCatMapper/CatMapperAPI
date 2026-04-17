import CMroutes.merge_routes as merge_routes


def test_merge_template_summary_for_merging_node(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["DATASET", "MERGING"]}]
        if "OPTIONAL MATCH (s)-[:MERGING]->(d:DATASET)" in query:
            return [
                {
                    "stackID": "S1",
                    "stackCMName": "stack test 1",
                    "datasetCount": 5,
                    "equivalenceTieCount": 100,
                    "keyReassignmentCount": 10,
                    "variableCount": 15,
                }
            ]
        if "MATCH (m:MERGING {CMID: $cmid})-[:MERGING]->(s:STACK)-[:MERGING]->(d:DATASET)" in query:
            return [
                {
                    "stackID": "S1",
                    "stackCMName": "stack test 1",
                    "datasetID": "D1",
                    "datasetCMName": "dataset one",
                }
            ]
        if "MATCH (s:STACK {CMID: stackID})-[r:MERGING]->(target)" in query:
            return [
                {
                    "mergingID": "M1",
                    "mergingCMName": "merge one",
                    "stackID": "S1",
                    "stackCMName": "stack test 1",
                    "relationship": "MERGING",
                    "targetLabels": ["VARIABLE"],
                    "targetCMID": "V1",
                    "targetCMName": "variable one",
                    "tieStackID": "S1",
                    "varName": "value_a",
                    "stackTransform": '[{"op":"as_numeric","target":"value_a"}]',
                    "datasetTransform": '[{"op":"copy","target":"value_a","sources":["raw_a"]}]',
                    "variableFilter": '[{"op":"drop_na","target":"value_a"}]',
                    "summaryStatistic": "mean",
                    "summaryFilter": None,
                    "summaryWeight": None,
                }
            ]
        if "MATCH (c1:CATEGORY)-[e:EQUIVALENT {stack: stackID}]->(c2:CATEGORY)" in query:
            return [{"stackID": "S1", "datasetID": "D1", "Key": "Site == Red Rock && Region == Southwest"}]
        return []

    monkeypatch.setattr(merge_routes, "getQuery", fake_get_query)

    response = client.get("/merge/template/summary/ArchaMap/M1")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["nodeType"] == "MERGING"
    assert payload["stackSummary"][0]["stackID"] == "S1"
    assert payload["stackSummaryTotals"]["datasetCount"] == 5
    assert payload["stackDatasetGroups"][0]["stackID"] == "S1"
    assert payload["stackDatasetGroups"][0]["datasets"][0]["datasetID"] == "D1"
    assert payload["equivalenceTies"][0]["Key"] == "Site_S1 == Red Rock && Region_S1 == Southwest"
    assert payload["equivalenceTies"][0]["extracted Key"] == "Site_S1 && Region_S1"
    assert payload["mergingTies"][0]["variableFilter"] == '[{"op":"drop_na","target":"value_a"}]'
    assert payload["mergingTies"][0]["stackTransform"] == '[{"op":"as_numeric","target":"value_a"}]'


def test_merge_template_summary_for_stack_node(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["DATASET", "STACK"]}]
        if "MATCH (m:MERGING)-[:MERGING]->(:STACK {CMID: $cmid})" in query:
            return [{"mergingTemplateCount": 3}]
        if "MATCH (:STACK {CMID: $cmid})-[:MERGING]->(d:DATASET)" in query:
            return [
                {
                    "datasetID": "D1",
                    "datasetCMName": "dataset one",
                    "equivalenceTieCount": 100,
                    "keyReassignmentCount": 10,
                    "variableCount": 15,
                }
            ]
        if "MATCH (s:STACK {CMID: stackID})-[r:MERGING]->(target)" in query:
            return [
                {
                    "mergingID": "M1",
                    "mergingCMName": "merge one",
                    "stackID": "S1",
                    "stackCMName": "stack one",
                    "relationship": "MERGING",
                    "targetLabels": ["VARIABLE"],
                    "targetCMID": "V1",
                    "targetCMName": "variable one",
                    "tieStackID": "S1",
                    "varName": "value_a",
                    "stackTransform": '[{"op":"copy","target":"value_a","sources":["raw_a"]}]',
                    "datasetTransform": None,
                    "variableFilter": None,
                    "summaryStatistic": "median",
                    "summaryFilter": None,
                    "summaryWeight": None,
                }
            ]
        if "MATCH (c1:CATEGORY)-[e:EQUIVALENT {stack: stackID}]->(c2:CATEGORY)" in query:
            return [{"stackID": "S1", "datasetID": "D1", "Key": "Culture == Pueblo"}]
        return []

    monkeypatch.setattr(merge_routes, "getQuery", fake_get_query)

    response = client.get("/merge/template/summary/ArchaMap/S1")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["nodeType"] == "STACK"
    assert payload["mergingTemplateCount"] == 3
    assert payload["datasetSummary"][0]["datasetID"] == "D1"
    assert payload["stackDatasetGroups"][0]["stackID"] == "S1"
    assert payload["stackDatasetGroups"][0]["datasets"][0]["datasetID"] == "D1"
    assert payload["equivalenceTies"][0]["Key"] == "Culture_S1 == Pueblo"
    assert payload["equivalenceTies"][0]["extracted Key"] == "Culture_S1"
    assert payload["mergingTies"][0]["summaryStatistic"] == "median"
