from scripts import migrate_equivalent_to_category_merging as migration


def test_split_stack_values_trims_deduplicates_and_handles_lists():
    assert migration.split_stack_values(" S1; S2 ;S1;; ") == ["S1", "S2"]
    assert migration.split_stack_values(["S3; S4", "S3"]) == ["S3", "S4"]
    assert migration.split_stack_values(None) == []


def test_normalize_dry_run_splits_legacy_and_malformed_ties(monkeypatch):
    monkeypatch.setattr(migration, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None, type=None):
        if "MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)" in query:
            return [
                {
                    "relID": "legacy-1",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1; S2",
                    "categoryID": "C1",
                    "properties": {"dataset": "D1", "Key": "Site == A", "stack": "S1; S2", "note": "legacy"},
                }
            ]
        if "MATCH (d:DATASET)-[m:MERGING]->(c:CATEGORY)" in query:
            return [
                {
                    "relID": "merge-1",
                    "datasetID": "D2",
                    "Key": "Site == B",
                    "stack": "S3; S4",
                    "categoryID": "C2",
                    "properties": {"Key": "Site == B", "stack": "S3; S4", "confidence": "high"},
                },
                {
                    "relID": "merge-2",
                    "datasetID": "D3",
                    "Key": "Site == C",
                    "stack": "S5",
                    "categoryID": "C3",
                    "properties": {"Key": "Site == C", "stack": "S5"},
                },
            ]
        if "OPTIONAL MATCH (s:STACK {CMID: stackID})-[:MERGING]->(d:DATASET {CMID: datasetID})" in query:
            return [
                {"datasetID": "D1", "stackID": "S1", "bridgeCount": 1},
                {"datasetID": "D1", "stackID": "S2", "bridgeCount": 0},
                {"datasetID": "D2", "stackID": "S3", "bridgeCount": 1},
                {"datasetID": "D2", "stackID": "S4", "bridgeCount": 1},
                {"datasetID": "D3", "stackID": "S5", "bridgeCount": 1},
            ]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(migration, "getQuery", fake_get_query)

    result = migration.normalize("ArchaMap")

    assert result["dryRun"] is True
    assert result["aborted"] is False
    assert result["legacyEquivalentCount"] == 1
    assert result["malformedMergingCount"] == 1
    assert result["normalizedTieCount"] == 3
    assert result["legacyRelationshipsToDelete"] == 0
    assert result["malformedRelationshipsToDelete"] == 1
    assert result["skippedCount"] == 1
    assert result["skippedSamples"][0]["stackID"] == "S2"


def test_normalize_prefers_existing_merging_target_for_conflict(monkeypatch):
    monkeypatch.setattr(migration, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None, type=None):
        if "MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)" in query:
            return [
                {
                    "relID": "legacy-1",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1",
                    "categoryID": "C_NEW",
                    "properties": {"dataset": "D1", "Key": "Site == A", "stack": "S1"},
                }
            ]
        if "MATCH (d:DATASET)-[m:MERGING]->(c:CATEGORY)" in query:
            return [
                {
                    "relID": "merge-1",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1",
                    "categoryID": "C_EXISTING",
                    "properties": {"Key": "Site == A", "stack": "S1"},
                }
            ]
        if "OPTIONAL MATCH (s:STACK {CMID: stackID})-[:MERGING]->(d:DATASET {CMID: datasetID})" in query:
            return [{"datasetID": "D1", "stackID": "S1", "bridgeCount": 1}]
        if "MATCH (:DATASET {CMID: datasetID})-[u:USES {Key: keyValue}]->(c:CATEGORY)" in query:
            return []
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(migration, "getQuery", fake_get_query)

    result = migration.normalize("ArchaMap")

    assert result["aborted"] is False
    assert result["normalizedTieCount"] == 0
    assert result["resolvedConflictRelationshipCount"] == 1
    assert result["legacyRelationshipsToDelete"] == 1


def test_normalize_resolves_source_conflict_with_unique_uses_target(monkeypatch):
    monkeypatch.setattr(migration, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None, type=None):
        if "MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)" in query:
            return [
                {
                    "relID": "legacy-keep",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1",
                    "categoryID": "C_USES",
                    "properties": {"dataset": "D1", "Key": "Site == A", "stack": "S1"},
                },
                {
                    "relID": "legacy-stale",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1",
                    "categoryID": "C_STALE",
                    "properties": {"dataset": "D1", "Key": "Site == A", "stack": "S1"},
                },
            ]
        if "MATCH (d:DATASET)-[m:MERGING]->(c:CATEGORY)" in query:
            return []
        if "OPTIONAL MATCH (s:STACK {CMID: stackID})-[:MERGING]->(d:DATASET {CMID: datasetID})" in query:
            return [{"datasetID": "D1", "stackID": "S1", "bridgeCount": 1}]
        if "MATCH (:DATASET {CMID: datasetID})-[u:USES {Key: keyValue}]->(c:CATEGORY)" in query:
            return [{"datasetID": "D1", "Key": "Site == A", "categoryIDs": ["C_USES"]}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(migration, "getQuery", fake_get_query)

    result = migration.normalize("ArchaMap")

    assert result["aborted"] is False
    assert result["normalizedTieCount"] == 1
    assert result["resolvedConflictRelationshipCount"] == 1
    assert result["legacyRelationshipsToDelete"] == 2
    assert result["unresolvedConflictCount"] == 0


def test_normalize_skips_unresolved_source_conflicts(monkeypatch):
    monkeypatch.setattr(migration, "getDriver", lambda _database: object())

    def fake_get_query(query, _driver=None, params=None, type=None):
        if "MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)" in query:
            return [
                {
                    "relID": "legacy-1",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1",
                    "categoryID": "C1",
                    "properties": {"dataset": "D1", "Key": "Site == A", "stack": "S1"},
                },
                {
                    "relID": "legacy-2",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1",
                    "categoryID": "C2",
                    "properties": {"dataset": "D1", "Key": "Site == A", "stack": "S1"},
                },
            ]
        if "MATCH (d:DATASET)-[m:MERGING]->(c:CATEGORY)" in query:
            return []
        if "OPTIONAL MATCH (s:STACK {CMID: stackID})-[:MERGING]->(d:DATASET {CMID: datasetID})" in query:
            return [{"datasetID": "D1", "stackID": "S1", "bridgeCount": 1}]
        if "MATCH (:DATASET {CMID: datasetID})-[u:USES {Key: keyValue}]->(c:CATEGORY)" in query:
            return []
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(migration, "getQuery", fake_get_query)

    result = migration.normalize("ArchaMap")

    assert result["aborted"] is False
    assert result["normalizedTieCount"] == 0
    assert result["legacyRelationshipsToDelete"] == 0
    assert result["unresolvedConflictCount"] == 1
    assert result["skippedSamples"][0]["reason"] == "no existing MERGING target or unique USES target to prefer"


def test_normalize_apply_creates_before_deleting(monkeypatch):
    monkeypatch.setattr(migration, "getDriver", lambda _database: object())
    calls = []

    def fake_get_query(query, _driver=None, params=None, type=None):
        if "MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)" in query:
            return [
                {
                    "relID": "legacy-1",
                    "datasetID": "D1",
                    "Key": "Site == A",
                    "stack": "S1",
                    "categoryID": "C1",
                    "properties": {"dataset": "D1", "Key": "Site == A", "stack": "S1", "note": "keep"},
                }
            ]
        if "MATCH (d:DATASET)-[m:MERGING]->(c:CATEGORY)" in query:
            return []
        if "OPTIONAL MATCH (s:STACK {CMID: stackID})-[:MERGING]->(d:DATASET {CMID: datasetID})" in query:
            return [{"datasetID": "D1", "stackID": "S1", "bridgeCount": 1}]
        if "MERGE (d)-[m:MERGING {stack: row.stackID, Key: row.Key}]->(c)" in query:
            calls.append("create")
            assert params["rows"][0]["properties"]["stack"] == "S1"
            assert params["rows"][0]["properties"]["Key"] == "Site == A"
            assert "dataset" not in params["rows"][0]["properties"]
            return [{"count": 1}]
        if "MATCH ()-[r:EQUIVALENT]->()" in query:
            calls.append("delete_legacy")
            assert params["rel_ids"] == ["legacy-1"]
            return [{"count": 1}]
        if "MATCH ()-[r:MERGING]->()" in query:
            calls.append("delete_malformed")
            assert params["rel_ids"] == []
            return [{"count": 0}]
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(migration, "getQuery", fake_get_query)

    result = migration.normalize("ArchaMap", apply=True)

    assert calls == ["create", "delete_legacy"]
    assert result["createdOrMatchedCount"] == 1
    assert result["deletedLegacyEquivalentCount"] == 1
