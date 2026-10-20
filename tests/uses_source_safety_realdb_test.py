"""Exercise deletion guards against an explicitly disposable Neo4j database."""
import os

import pytest
from CM import USES as uses
from scripts.restore_uses_from_manifest import restore_batch

pytestmark = pytest.mark.realdb


@pytest.fixture
def assertions(realdb_driver, monkeypatch):
    if os.environ.get("CM_REALDB_IS_DISPOSABLE") != "1":
        pytest.skip("Source safety tests require a disposable database.")
    monkeypatch.setattr(uses, "getDriver", lambda _: realdb_driver)
    with realdb_driver.session() as session:
        assert session.run("MATCH (p:PROPERTY) RETURN count(p) AS n").single()["n"] == 0
        session.run("""
            CREATE (d:DATASET:USES_SAFETY_TEST {CMID:'SD950000001',shortName:'test1'}),
                   (e:DATASET:USES_SAFETY_TEST {CMID:'SD950000002',shortName:'test2'}),
                   (c:CATEGORY:DIALECT:USES_SAFETY_TEST {CMID:'SM950000001',CMName:'Child'}),
                   (p:CATEGORY:DIALECT:USES_SAFETY_TEST {CMID:'SM950000002',CMName:'Parent'}),
                   (old:CATEGORY:DIALECT:USES_SAFETY_TEST {CMID:'SM950000003',CMName:'Old parent'}),
                   (d)-[:USES {Key:'id == one',Name:['Child'],label:'DIALECT',
                       parent:['SM950000002'],comment:'preserve',ownerUserId:'tester',
                       modifiedByOtherUser:false,areakm2:12.5}]->(c),
                   (e)-[:USES {Key:'id == two',Name:['Child'],label:'DIALECT',
                       parent:['SM950000002']}]->(c),
                   (old)-[:CONTAINS]->(c),
                   (:PROPERTY:USES_SAFETY_TEST {CMName:'parent',type:'relationship',relationship:'CONTAINS'}),
                   (:PROPERTY:USES_SAFETY_TEST {CMName:'eventType',type:'relationship',allowedValues:['HIERARCHY']}),
                   (:LABEL:USES_SAFETY_TEST {CMName:'DIALECT',label:'DIALECT',public:'TRUE',groupLabel:'LANGUOID'})
        """).consume()
        fixture_ids = session.run("MATCH (n:USES_SAFETY_TEST) RETURN collect(elementId(n)) AS ids").single()["ids"]
    yield realdb_driver
    with realdb_driver.session() as session:
        # updateLabels intentionally replaces non-domain labels, including the
        # fixture marker. Clean up by the IDs captured before reconciliation.
        session.run("MATCH (n) WHERE elementId(n) IN $ids OR n:USES_SAFETY_TEST DETACH DELETE n", ids=fixture_ids).consume()


def source_rows(driver):
    with driver.session() as session:
        return session.run("""
            MATCH (d)-[r:USES]->(c {CMID:'SM950000001'})
            RETURN elementId(r) AS id, properties(r) AS properties ORDER BY id
        """).data()


@pytest.mark.parametrize("area_metadata", [False, True])
def test_full_process_rebuilds_hierarchy_and_preserves_all_source_ties(assertions, area_metadata):
    if area_metadata:
        with assertions.session() as session:
            session.run("CREATE (:PROPERTY:USES_SAFETY_TEST {CMName:'areakm2',type:'relationship'})").consume()
    before = source_rows(assertions)
    result = uses.processUSES("SocioMap", CMID=["SM950000001"])
    assert isinstance(result, dict), result
    assert source_rows(assertions) == before
    with assertions.session() as session:
        parents = session.run("""
            MATCH (p)-[:CONTAINS]->(:CATEGORY {CMID:'SM950000001'})
            RETURN p.CMID AS CMID
        """).value()
    assert parents == ["SM950000002"]


def test_corrupt_metadata_fails_before_changing_source_or_derived_ties(assertions):
    with assertions.session() as session:
        session.run("CREATE (:PROPERTY:USES_SAFETY_TEST {CMName:'areakm2',type:'relationship',relationship:'USES'})").consume()
    before = source_rows(assertions)
    result = uses.processUSES("SocioMap", CMID=["SM950000001"])
    assert result[1] == 500 and "value-only" in result[0]
    assert source_rows(assertions) == before
    with assertions.session() as session:
        parents = session.run("MATCH (p)-[:CONTAINS]->(:CATEGORY {CMID:'SM950000001'}) RETURN p.CMID").value()
    assert parents == ["SM950000003"]


def test_query_guard_preserves_uses_even_if_python_validator_is_bypassed(assertions, monkeypatch):
    # Independently exercise the Cypher guard rather than relying on the allow-list.
    monkeypatch.setattr(uses, "_validate_derived_relationship", lambda prop, rel: rel)
    monkeypatch.setattr(uses, "_contextual_tie_primary_domain_conflicts", lambda *a: [])
    before = source_rows(assertions)
    result = uses.fixUsesRels("SocioMap", "areakm2", "USES", ["SM950000001"])
    assert not isinstance(result, tuple), result
    assert source_rows(assertions) == before


def test_recovery_restores_exact_properties_and_is_idempotent(assertions):
    row = {"sourceCMID": "SD950000001", "targetCMID": "SM950000001",
           "properties": {"Key": "id == recovery", "Name": ["Recovered name"],
                          "label": "DIALECT", "areakm2": 12.5,
                          "modifiedByOtherUser": False, "logID": ["original-log"]}}
    with assertions.session() as session:
        assert len(session.execute_write(restore_batch, [row])) == 1
    with assertions.session() as session:
        assert session.execute_write(restore_batch, [row]) == []
    recovered = [r for r in source_rows(assertions) if r["properties"]["Key"] == "id == recovery"]
    assert len(recovered) == 1 and recovered[0]["properties"] == row["properties"]


def test_recovery_conflict_preserves_every_existing_tie_and_creates_nothing(assertions):
    before = source_rows(assertions)
    rows = [{"sourceCMID": "SD950000001", "targetCMID": "SM950000001",
             "properties": {"Key": "id == recovery", "Name": ["Recovery"]}},
            {"sourceCMID": "SD950000001", "targetCMID": "SM950000001",
             "properties": {"Key": "id == one", "Name": ["Conflicting replacement"]}}]
    with assertions.session() as session:
        with pytest.raises(ValueError, match="conflicts with recovery evidence"):
            session.execute_write(restore_batch, rows)
    assert source_rows(assertions) == before


def test_recovery_refuses_ambiguous_category_ids(assertions):
    before = source_rows(assertions)
    with assertions.session() as session:
        session.run("CREATE (:CATEGORY:USES_SAFETY_TEST {CMID:'SM950000001'})").consume()
        with pytest.raises(ValueError, match="Ambiguous recovery endpoint"):
            session.execute_write(restore_batch, [{
                "sourceCMID": "SD950000001", "targetCMID": "SM950000001",
                "properties": {"Key": "id == recovery", "Name": ["Recovery"]}}])
    assert source_rows(assertions) == before
