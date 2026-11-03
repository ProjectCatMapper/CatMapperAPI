"""Label correctness and database-work regressions on a disposable Neo4j."""
import os

import pytest

from CM import USES as uses

pytestmark = pytest.mark.realdb


@pytest.fixture(params=["5", "25"])
def cypher_version(request):
    return request.param


@pytest.fixture
def label_graph(realdb_driver, monkeypatch, cypher_version):
    if os.environ.get("CM_REALDB_IS_DISPOSABLE") != "1":
        pytest.skip("Label performance tests require a disposable database")
    monkeypatch.setattr(uses, "getDriver", lambda _: realdb_driver)
    original_query = uses.getQuery

    def versioned_query(query, driver, **kwargs):
        return original_query(f"CYPHER {cypher_version} {query}", driver, **kwargs)

    monkeypatch.setattr(uses, "getQuery", versioned_query)
    with realdb_driver.session() as session:
        assert session.run("MATCH (n) RETURN count(n)").single()[0] == 0
        session.run("""
            CREATE INDEX label_test_cmid IF NOT EXISTS
            FOR (c:CATEGORY) ON (c.CMID)
        """).consume()
        session.run("CALL db.awaitIndexes()").consume()
        session.run("""
            CREATE (d:DATASET {CMID:'SD950000001'}),
                   (e:DATASET {CMID:'SD950000002'}),
                   (:LABEL {label:'DIALECT',groupLabel:'LANGUOID',public:'TRUE'}),
                   (:LABEL {label:'PPL',groupLabel:'PPL',public:'TRUE'}),
                   (:LABEL {label:'AREA',groupLabel:'AREA',public:'TRUE'})
            WITH d, e
            UNWIND range(0, 499) AS i
            CREATE (c:CATEGORY:AREA {CMID:'SM95' + toString(i)}),
                   (d)-[:USES {Key:toString(i),label:['DIALECT'],Name:['Source'],
                       modifiedByOtherUser:false,comment:'preserve'}]->(c),
                   (noise:CATEGORY:AREA {CMID:'SM96' + toString(i)}),
                   (d)-[:USES {Key:'noise' + toString(i),label:['AREA']}]->(noise)
            FOREACH (_ IN CASE WHEN i = 0 THEN [1] ELSE [] END |
                CREATE (e)-[:USES {Key:'second-source',label:['PPL']}]->(c))
        """).consume()
        # Reconciliation must still recover categories without the base label.
        session.run("MATCH (c:CATEGORY {CMID:'SM951'}) REMOVE c:CATEGORY").consume()
    yield realdb_driver
    with realdb_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
        session.run("DROP INDEX label_test_cmid IF EXISTS").consume()


def graph_state(driver):
    with driver.session() as session:
        labels = dict(session.run(
            "MATCH (c) WHERE c.CMID STARTS WITH 'SM' RETURN c.CMID, labels(c)"
        ).values())
        sources = session.run("""
            MATCH ()-[r:USES]->()
            RETURN elementId(r) AS id, properties(r) AS properties ORDER BY id
        """).data()
    return {cmid: set(value) for cmid, value in labels.items()}, sources


@pytest.mark.parametrize("scope", ["SM950", ["SM950", "SM951", "SM950"], None, []])
def test_labels_preserve_sources_other_categories_and_idempotency(label_graph, scope):
    before, sources = graph_state(label_graph)
    result = uses.updateLabels("sociomap", CMID=scope)
    assert not isinstance(result, tuple), result
    after, after_sources = graph_state(label_graph)
    assert after_sources == sources
    selected = set(before) if scope is None else set([scope] if isinstance(scope, str) else scope)
    for cmid in before:
        if cmid not in selected or cmid.startswith("SM96"):
            assert after[cmid] == before[cmid]
        else:
            expected = {"CATEGORY", "DIALECT", "LANGUOID"}
            if cmid == "SM950":
                expected.add("PPL")
            assert after[cmid] == expected
    assert not isinstance(uses.updateLabels("sociomap", CMID=scope), tuple)
    assert graph_state(label_graph) == (after, sources)


def _database_hits(profile):
    return profile.get("dbHits", 0) + sum(
        _database_hits(child) for child in profile.get("children", [])
    )


def _operators(plan):
    return [plan["operatorType"]] + [
        operator for child in plan.get("children", []) for operator in _operators(child)
    ]


def test_batch_recovery_does_not_repeat_scans_and_group_queries_use_index(
    label_graph, monkeypatch, cypher_version,
):
    calls = []
    original_query = uses.getQuery

    def record(query, driver, **kwargs):
        calls.append((query, kwargs.get("params", {})))
        return original_query(query, driver, **kwargs)

    monkeypatch.setattr(uses, "getQuery", record)
    cmids = [f"SM95{i}" for i in range(500)]
    result = uses.updateLabels("sociomap", CMID=cmids)
    assert not isinstance(result, tuple), result
    with label_graph.session() as session:
        recovery_query, _ = calls[0]
        one = session.run(f"CYPHER {cypher_version} PROFILE " + recovery_query, CMID=cmids[:1]).consume()
        batch = session.run(f"CYPHER {cypher_version} PROFILE " + recovery_query, CMID=cmids).consume()
        # Count database accesses rather than relying on machine timing. The old
        # correlated scan did 500 times the work for a 500-row batch.
        assert _database_hits(batch.profile) <= 3 * _database_hits(one.profile)
        group_calls = [(q, p) for q, p in calls if "set c:`" in q]
        assert len(group_calls) == 3
        for query, params in group_calls:
            plan = session.run(f"CYPHER {cypher_version} EXPLAIN " + query, **params).consume().plan
            operators = _operators(plan)
            assert any("IndexSeek" in operator for operator in operators), ", ".join(operators)
            assert not any("Scan" in operator for operator in operators), ", ".join(operators)
