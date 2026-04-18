import pandas as pd
import pytest

import CM.upload as upload


def _fake_upload_query(rel_count_stack_merging=1, rel_count_stack_dataset=1):
    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='node'" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='relationship'" in query:
            return []
        if 'MATCH (n:PROPERTY) WHERE n.type="relationship" and n.metaType="string" RETURN n.CMName as n' in query:
            return []
        if 'MATCH (n:PROPERTY) WHERE n.type="node" and n.metaType="string" RETURN n.CMName' in query:
            return []
        if "OPTIONAL MATCH (n:" in query and "RETURN row.value AS value, COUNT(n) AS count" in query:
            return [{"value": row["value"], "count": 1} for row in params["rows"]]
        if "OPTIONAL MATCH (s:STACK {CMID: row.stackID})<-[r:MERGING]-(m:MERGING {CMID: row.mergingID})" in query:
            return [
                {
                    "stackID": row["stackID"],
                    "mergingID": row["mergingID"],
                    "rel_count": rel_count_stack_merging,
                }
                for row in params["rows"]
            ]
        if "OPTIONAL MATCH (d:DATASET {CMID: datasetID})<-[r1:MERGING]-(s:STACK)<-[r:MERGING]-(m:MERGING {CMID: mergingID})" in query:
            return [
                {
                    "datasetID": row["datasetID"],
                    "stackID": None,
                    "mergingID": row["mergingID"],
                    "stack_count": 0,
                }
                for row in params["rows"]
            ]
        if "OPTIONAL MATCH (s:STACK {CMID: row.stackID})-[r:MERGING]->(d:DATASET {CMID: row.datasetID})" in query:
            return [
                {
                    "stackID": row["stackID"],
                    "datasetID": row["datasetID"],
                    "rel_count": rel_count_stack_dataset,
                }
                for row in params["rows"]
            ]
        raise AssertionError(f"Unexpected query: {query}")

    return fake_get_query


def test_add_merging_to_datasets_with_explicit_stackid_calls_create_mties(monkeypatch):
    """When stackID is explicitly provided for merging_ties_to_datasets the tie
    pre-validation checks are skipped and create_mties_stacks is invoked instead
    of raising a ValueError."""
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    # _fake_upload_query handles CMID-existence checks; tie checks are skipped by the fix
    monkeypatch.setattr(upload, "getQuery", _fake_upload_query())

    called = {}

    def fake_create_mties_stacks(database, user, dataset):
        called["yes"] = True
        return {"result": dataset}

    monkeypatch.setattr(upload, "create_mties_stacks", fake_create_mties_stacks)

    # Should not raise – the validation tie checks are bypassed for merging_ties_to_datasets
    # when an explicit stackID is present, and create_mties_stacks handles the rest.
    upload.input_Nodes_Uses(
        dataset=[{"mergingID": "M1", "stackID": "S1", "datasetID": "D1"}],
        database="ArchaMap",
        uploadOption="add_merging",
        optionalProperties=[],
        user="tester",
        mergingType="merging_ties_to_datasets",
    )

    assert called.get("yes"), "create_mties_stacks should have been called"


def test_add_merging_to_variables_still_errors_when_stack_merging_tie_missing(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "getQuery", _fake_upload_query(rel_count_stack_merging=0, rel_count_stack_dataset=1))

    with pytest.raises(ValueError, match="Missing MERGING tie between stackID and mergingID"):
        upload.input_Nodes_Uses(
            dataset=[
                {
                    "mergingID": "M1",
                    "stackID": "S1",
                    "datasetID": "D1",
                    "variableID": "V1",
                    "Key": "Site_Num == AZ D:11:2030",
                    "varName": "v",
                }
            ],
            database="ArchaMap",
            uploadOption="add_merging",
            optionalProperties=[],
            user="tester",
            mergingType="merging_ties_to_variables",
        )


def test_add_merging_to_variables_with_explicit_stackid_skips_inferred_bridge_check(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "check_query_cancellation", lambda: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='node'" in query:
            return []
        if "MATCH (p:PROPERTY) WHERE p.type='relationship'" in query:
            return []
        if 'MATCH (n:PROPERTY) WHERE n.type="relationship" and n.metaType="string" RETURN n.CMName as n' in query:
            return []
        if 'MATCH (n:PROPERTY) WHERE n.type="node" and n.metaType="string" RETURN n.CMName' in query:
            return []
        if "OPTIONAL MATCH (n:" in query and "RETURN row.value AS value, COUNT(n) AS count" in query:
            return [{"value": row["value"], "count": 1} for row in params["rows"]]
        if "OPTIONAL MATCH (s:STACK {CMID: row.stackID})<-[r:MERGING]-(m:MERGING {CMID: row.mergingID})" in query:
            return [
                {
                    "stackID": row["stackID"],
                    "mergingID": row["mergingID"],
                    "rel_count": 1,
                }
                for row in params["rows"]
            ]
        if "OPTIONAL MATCH (d:DATASET {CMID: datasetID})<-[r1:MERGING]-(s:STACK)<-[r:MERGING]-(m:MERGING {CMID: mergingID})" in query:
            raise AssertionError("Inferred dataset/merging bridge check should be skipped when stackID is explicit")
        if "OPTIONAL MATCH (s:STACK {CMID: row.stackID})-[r:MERGING]->(d:DATASET {CMID: row.datasetID})" in query:
            return [
                {
                    "stackID": row["stackID"],
                    "datasetID": row["datasetID"],
                    "rel_count": 1,
                }
                for row in params["rows"]
            ]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    called = {}

    def fake_create_mties_variables(database, user, dataset):
        called["yes"] = True
        return {"result": dataset}

    monkeypatch.setattr(upload, "create_mties_variables", fake_create_mties_variables)

    upload.input_Nodes_Uses(
        dataset=[
            {
                "mergingID": "M1",
                "stackID": "S1",
                "datasetID": "D1",
                "variableID": "V1",
                "Key": "Site_Num == AZ D:11:2030",
                "varName": "v",
            }
        ],
        database="ArchaMap",
        uploadOption="add_merging",
        optionalProperties=[],
        user="tester",
        mergingType="merging_ties_to_variables",
    )

    assert called.get("yes"), "create_mties_variables should have been called"


def test_create_mties_stacks_uses_merge_without_properties_and_deduplicates(monkeypatch):
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)

    captured = {}

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        if "MATCH (a:MERGING {CMID: row.mergingID})" in query:
            captured["merging_query"] = query
            captured["merging_rows"] = params["rows"]
            return [{"count": len(params["rows"])}]
        if "MATCH (a:STACK {CMID: row.stackID})" in query and "MATCH (b:DATASET {CMID: row.datasetID})" in query:
            captured["dataset_query"] = query
            captured["dataset_rows"] = params["rows"]
            return [{"count": len(params["rows"])}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(upload, "getQuery", fake_get_query)

    upload.create_mties_stacks(
        database="ArchaMap",
        user="tester",
        dataset=pd.DataFrame(
            [
                {"mergingID": "M1", "stackID": "S1", "datasetID": "D1"},
                {"mergingID": "M1", "stackID": "S1", "datasetID": "D1"},
                {"mergingID": "M1", "stackID": "S2", "datasetID": "D2"},
            ]
        ),
    )

    assert len(captured["merging_rows"]) == 2
    assert len(captured["dataset_rows"]) == 2
    assert "MERGE (a)-[r:MERGING]->(b)" in captured["merging_query"]
    assert "MERGE (a)-[r:MERGING]->(b)" in captured["dataset_query"]
    assert "MERGE (a)-[r:MERGING {" not in captured["merging_query"]
    assert "MERGE (a)-[r:MERGING {" not in captured["dataset_query"]
