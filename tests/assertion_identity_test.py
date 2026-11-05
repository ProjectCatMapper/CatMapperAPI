"""Regression coverage for shared/missing histories and mutable audit logs."""

import copy
import uuid

import pytest
from rdflib import RDF

from CM import assertion_identity as identity, linked_data as ld, utils
from CM.admin import _merge_uses_relationship_properties


def group(logs, ids=None):
    ids = ids or [None] * len(logs)
    rows = []
    for index, (log, token) in enumerate(zip(logs, ids)):
        props = {"Key": "Type == example", "Name": ["Literal; source name"]}
        if log is not None:
            props["logID"] = log
        if token is not None:
            props["assertionId"] = token
        rows.append({"relId": str(index), "properties": props,
                     "stableDiscriminator": log, "assertionId": token})
    return {"datasetCmid": "AD1", "conceptCmid": "AM1", "key": "Type == example",
            "multiplicity": len(rows), "identities": rows}


@pytest.mark.parametrize("logs", [[None, None], [[], []], [["shared"], ["shared"]], ["", ""]])
def test_missing_or_shared_logs_get_distinct_ids_without_source_loss(logs):
    source = group(logs)
    before = copy.deepcopy(source)
    changes = identity.plan_assertion_ids([source])
    assert source == before
    assert len(changes) == 2
    assert len({row["assertionId"] for row in changes}) == 2
    for row, change in zip(source["identities"], changes):
        assert change["before"] == row["properties"]
        row["assertionId"] = change["assertionId"]
        row["properties"]["assertionId"] = change["assertionId"]
    identity.validate_collision_groups([source])
    assert identity.plan_assertion_ids([source]) == []


def test_recovery_preserves_existing_unambiguous_urls_and_pinned_ids():
    source = group([["one"], ["two"], None], [None, None, "a" * 16])
    changes = identity.plan_assertion_ids([source])
    assert [row["assertionId"] for row in changes] == [
        identity.legacy_assertion_id(["one"]), identity.legacy_assertion_id(["two"])]
    assert all(row["relId"] != "2" for row in changes)


def test_pinned_urls_survive_log_history_changes_and_do_not_leak_fields():
    rows = [{"datasetCmid": "AD1", "conceptCmid": "AM1", "key": "Type == example",
             "assertionId": token, "stableDiscriminator": ["shared"]}
            for token in ("a" * 16, "b" * 16)]
    original = ld.assertion_iri({**rows[0], "database": "archamap"}, 2)
    rows[0]["stableDiscriminator"] = ["shared", "new-edit"]
    assert ld.assertion_iri({**rows[0], "database": "archamap"}, 2) == original
    graph = ld.project_resource("archamap", {"cmid": "AM1", "labels": ["CATEGORY"]},
                                rows, {("AD1", "Type == example", "AM1"): 2})
    assert len(set(graph.subjects(RDF.type, ld.CAT.DatasetAssertion))) == 2
    assert "assertionId" not in graph.serialize(format="turtle")
    assert "stableDiscriminator" not in graph.serialize(format="turtle")


@pytest.mark.parametrize("token", ["", [], "bad", "../secret", "A" * 16])
def test_invalid_persistent_identity_cannot_fall_back_to_mutable_history(token):
    with pytest.raises(ValueError, match="invalid persistent"):
        identity.assertion_token({"assertionId": token, "stableDiscriminator": ["valid"]})


def test_duplicate_persistent_ids_require_review_instead_of_automatic_reassignment():
    source = group([["one"], ["two"]], ["a" * 16, "a" * 16])
    with pytest.raises(ValueError, match="explicit review"):
        identity.plan_assertion_ids([source])


def test_ambiguous_collision_is_rejected_before_a_partial_page_is_projected(monkeypatch):
    monkeypatch.setattr(ld, "getDriver", lambda _: object())
    source = group([["shared"], ["shared"]])
    def query(query, **kwargs):
        if query == ld.NODE_QUERY:
            return [{"cmid": "AM1", "labels": ["CATEGORY"]}]
        if query == ld.ASSERTION_QUERY:
            return [{"datasetCmid": "AD1", "conceptCmid": "AM1", "key": "Type == example",
                     "stableDiscriminator": ["shared"]}]
        if query == ld.COLLISION_QUERY:
            return [source]
        return []
    monkeypatch.setattr(ld, "getQuery", query)
    with pytest.raises(ld.MalformedPublicResource, match="indistinguishable"):
        ld.fetch_resource_projection("archamap", "AM1", assertion_limit=1)


def test_bulk_export_rejects_collisions_before_yielding_any_fragments(monkeypatch):
    from CM import bulk_rdf
    monkeypatch.setattr(bulk_rdf, "getDriver", lambda _: object())
    monkeypatch.setattr(bulk_rdf, "getQuery", lambda *a, **kw: [group([["same"], ["same"]])])
    monkeypatch.setattr(bulk_rdf, "_stream_query", lambda *a: pytest.fail("Export started before identity checks"))
    with pytest.raises(ld.MalformedPublicResource, match="indistinguishable"):
        next(bulk_rdf.iter_bulk_graphs("archamap"))


def test_collision_geometry_uris_resolve_to_only_their_own_assertion(monkeypatch):
    from CM.assertion_index import remember_assertions
    from rdflib import URIRef
    rows = [{"datasetCmid": "AD1", "conceptCmid": "AM1", "key": "Type == example",
             "assertionId": token, "geoCoords": ['{"type":"Point","coordinates":[1,2]}']}
            for token in ("a" * 16, "b" * 16)]
    remember_assertions("archamap", rows)
    monkeypatch.setattr(ld, "getDriver", lambda _: object())
    monkeypatch.setattr(ld, "getQuery", lambda *a, **kw: rows)
    geometries = []
    for row in rows:
        graph = ld.project_assertion("archamap", row, 2)
        geometry = next(graph.subjects(RDF.type, ld.GEO.Geometry))
        geometries.append(geometry)
        assertion_id, geometry_id = str(geometry).split("/geometry/uses/")[1].split("/")
        fetched = ld.fetch_assertion_projection("archamap", assertion_id, geometry_id)
        assert (URIRef(geometry), RDF.type, ld.GEO.Geometry) in fetched
        assert len(set(fetched.subjects(RDF.type, ld.CAT.DatasetAssertion))) == 1
    assert len(set(geometries)) == 2


def test_explicit_source_merge_retains_one_scalar_identity():
    relationships = [{"relID": str(i), "props": {"Key": "key", "assertionId": token}}
                     for i, token in enumerate(("a" * 16, "b" * 16))]
    assert _merge_uses_relationship_properties(relationships, "AM1", "key", "AD1") == {
        "Key": "key", "assertionId": "a" * 16}


def test_reconciliation_propagates_identity_failure(monkeypatch):
    from CM import USES
    monkeypatch.setattr(USES, "getDriver", lambda _: object())
    monkeypatch.setattr(USES.context_policy, "validate_stored", lambda *a: None)
    monkeypatch.setattr(USES, "getPropertiesMetadata", lambda **kw: [])
    monkeypatch.setattr(USES, "validate_stored_event_contexts", lambda *a: None)
    def fail(*args):
        raise RuntimeError("Identity verification failed")
    monkeypatch.setattr(USES, "ensure_assertion_ids", fail)
    monkeypatch.setattr(USES, "updateAltNames", lambda **kw: pytest.fail("Reconciliation continued"))
    result = USES.processUSES("ArchaMap", ["AM1"])
    assert result == ("Identity verification failed", 500)


def test_guarded_plan_refuses_to_rewrite_an_existing_id_before_any_query(monkeypatch):
    monkeypatch.setattr(utils, "getQuery", lambda *a, **kw: pytest.fail("Invalid plan reached Neo4j"))
    with pytest.raises(ValueError, match="never be rewritten"):
        identity.apply_assertion_id_plan(object(), [{"relId": "r", "assertionId": "b" * 16,
                                                   "before": {"assertionId": "a" * 16}}])


def test_repair_preview_rejects_a_plan_for_a_different_source_tie():
    from scripts.repair_assertion_ids import expected_after
    groups = [group([None, None])]
    changes = identity.plan_assertion_ids(groups)
    changes[0]["before"]["Name"] = ["Changed source"]
    with pytest.raises(ValueError, match="original unpinned"):
        expected_after(groups, changes)


@pytest.mark.realdb
@pytest.mark.parametrize("stale", [False, True])
def test_saved_repair_plan_handles_labels_and_rolls_back_stale_inputs(realdb_driver, monkeypatch, tmp_path, stale):
    from contextlib import nullcontext
    from scripts import repair_assertion_ids as repair
    cmid = "semantic-plan-" + uuid.uuid4().hex
    with realdb_driver.session() as session:
        tx = session.begin_transaction()
        try:
            tx.run("""CREATE (d:DATASET {CMID: $dataset}), (c:CATEGORY {CMID: $concept}),
                              (other:CATEGORY {CMID: $other}),
                              (d)-[:USES {Key:'Type == value', Name:['value']}]->(c),
                              (d)-[:USES {Key:'Type == value', Name:['value']}]->(c),
                              (d)-[:USES {Key:$key, label:['CATEGORY'], Name:['Literal']}]->(other)""",
                   dataset=cmid + "-d", concept=cmid, other=cmid + "-other",
                   key="Key == " + cmid + "-other").consume()
            def query(query, driver=None, params=None, type=None):
                params = dict(params or {})
                if query == identity.COLLISION_GROUP_QUERY:
                    params["cmids"] = [cmid]
                if query == repair.LABEL_DRIFT_QUERY:
                    params["dataset"] = cmid + "-d"
                return tx.run(query, params).data()
            monkeypatch.setattr(utils, "getQuery", query)
            monkeypatch.setattr(repair, "getQuery", query)
            monkeypatch.setattr(repair, "getDriver", lambda _: realdb_driver)
            monkeypatch.setattr(repair, "query_transaction", lambda *a: nullcontext())
            path = tmp_path / "plan.json"
            preview = repair.run("archamap", path)
            assert preview["changes"] == 2 and preview["labels"] == 1
            if stale:
                tx.run("MATCH (:DATASET {CMID:$dataset})-[r:USES]->() SET r.comment='Changed after preview'",
                       dataset=cmid + "-d").consume()
                with pytest.raises(ValueError, match="fresh preview"):
                    repair.run("archamap", path, apply=True)
                count = tx.run("MATCH (:DATASET {CMID:$dataset})-[r:USES]->() WHERE r.assertionId IS NOT NULL RETURN count(r)",
                               dataset=cmid + "-d").single()[0]
                assert count == 0
            else:
                assert repair.run("archamap", path, apply=True)["status"] == "verified"
                assert path.with_name("plan-verified.json").exists()
                with pytest.raises(ValueError, match="refusing to replay"):
                    repair.run("archamap", path, apply=True)
        finally:
            tx.rollback()


@pytest.mark.realdb
@pytest.mark.parametrize("logs", [[None, None], [["shared"], ["shared"]], [["one"], ["two"]]])
def test_realdb_collision_repair_is_lossless_idempotent_and_edit_stable(realdb_driver, monkeypatch, logs):
    """All fixture and identity writes roll back; no existing source is changed."""
    cmid = "semantic-identity-" + uuid.uuid4().hex
    with realdb_driver.session() as session:
        tx = session.begin_transaction()
        try:
            tx.run("""CREATE (d:DATASET {CMID: $dataset}), (c:CATEGORY {CMID: $concept})
                      WITH d,c UNWIND $logs AS log
                      CREATE (d)-[r:USES {Key:'literal == value', Name:['value'], parent:['AM9']}]->(c)
                      SET r.logID = log""", dataset=cmid + "-d", concept=cmid, logs=logs).consume()
            def query(query, driver=None, params=None, type=None, **kwargs):
                return tx.run(query, params or kwargs).data()
            monkeypatch.setattr(utils, "getQuery", query)
            before = query(identity.COLLISION_GROUP_QUERY, params={"cmids": [cmid]})
            assert identity.ensure_assertion_ids(realdb_driver, [cmid]) == 2
            after = query(identity.COLLISION_GROUP_QUERY, params={"cmids": [cmid]})
            for old, new in zip(before[0]["identities"], after[0]["identities"]):
                assert {k: v for k, v in new["properties"].items() if k != "assertionId"} == old["properties"]
            assert identity.ensure_assertion_ids(realdb_driver, [cmid]) == 0
            tokens = [identity.assertion_token(row) for row in after[0]["identities"]]
            assert len(set(tokens)) == 2
            if logs == [["one"], ["two"]]:
                assert tokens == [identity.assertion_token(row) for row in before[0]["identities"]]
            from CM import log
            monkeypatch.setattr(log, "getQuery", query)
            log.createLog([row["relId"] for row in after[0]["identities"]], "relation", "test edit", "0", realdb_driver)
            edited = query(identity.COLLISION_GROUP_QUERY, params={"cmids": [cmid]})
            assert [identity.assertion_token(row) for row in edited[0]["identities"]] == tokens
            assert all(len(row["stableDiscriminator"]) == len(old or []) + 1
                       for row, old in zip(edited[0]["identities"], logs))
        finally:
            tx.rollback()
