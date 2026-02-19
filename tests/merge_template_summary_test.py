import CMroutes.merge_routes as merge_routes


def test_merge_template_summary_for_merging_node(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["DATASET", "MERGING"]}]
        if "MATCH (m:MERGING {CMID: $cmid})-[:MERGING]->(s:STACK)" in query:
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
        if "MATCH (s:STACK {CMID: stackID})-[r:MERGING]->(target)" in query:
            return [{"stackID": "S1", "targetCMID": "D1"}]
        if "MATCH (c1:CATEGORY)-[e:EQUIVALENT {stack: stackID}]->(c2:CATEGORY)" in query:
            return [{"stackID": "S1", "datasetID": "D1"}]
        return []

    monkeypatch.setattr(merge_routes, "getQuery", fake_get_query)

    response = client.get("/merge/template/summary/ArchaMap/M1")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["nodeType"] == "MERGING"
    assert payload["stackSummary"][0]["stackID"] == "S1"
    assert payload["stackSummaryTotals"]["datasetCount"] == 5


def test_merge_template_summary_for_stack_node(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None):
        if "RETURN labels(n) AS labels" in query:
            return [{"labels": ["DATASET", "STACK"]}]
        if "RETURN count(DISTINCT m) AS mergingTemplateCount" in query:
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
            return [{"stackID": "S1", "targetCMID": "D1"}]
        if "MATCH (c1:CATEGORY)-[e:EQUIVALENT {stack: stackID}]->(c2:CATEGORY)" in query:
            return [{"stackID": "S1", "datasetID": "D1"}]
        return []

    monkeypatch.setattr(merge_routes, "getQuery", fake_get_query)

    response = client.get("/merge/template/summary/ArchaMap/S1")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["nodeType"] == "STACK"
    assert payload["mergingTemplateCount"] == 3
    assert payload["datasetSummary"][0]["datasetID"] == "D1"
