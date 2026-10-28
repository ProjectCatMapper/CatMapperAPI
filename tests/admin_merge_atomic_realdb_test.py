"""Opt-in merge checks for a separately created disposable Neo4j with APOC.

Never use the shared application database. Set CM_MERGE_TEST_DISPOSABLE=1 and
CM_MERGE_TEST_URI to a disposable localhost instance on a nonproduction port.
"""
import json
import os
from datetime import datetime
from urllib.parse import urlparse

import pytest
from neo4j import GraphDatabase

from CM import admin, utils
from CM.merge_references import plan_merge_references

pytestmark = pytest.mark.realdb


@pytest.fixture
def realdb_driver(isolated_graph):
    # Route the shared cleanup check to this isolated instance as well.
    return isolated_graph


@pytest.fixture
def isolated_graph(monkeypatch):
    uri = os.environ.get('CM_MERGE_TEST_URI', '')
    if os.environ.get('CM_MERGE_TEST_DISPOSABLE') != '1' or not uri:
        pytest.skip('Requires an explicitly disposable merge-test database')
    target = urlparse(uri)
    if target.hostname not in {'localhost', '127.0.0.1'} or target.port in {None, 7687, 17687, 27687, 37687}:
        pytest.fail('Refusing to use a production or nonlocal database for destructive fixture setup')
    driver = GraphDatabase.driver(uri, auth=None)
    monkeypatch.setenv('CATMAPPER_CONTEXT_POLICY', 'metadata')
    for database in ('sociomap', 'archamap'):
        monkeypatch.setitem(utils._driver_cache, database, driver)
        monkeypatch.setitem(utils._last_verified, database, datetime.now())
    yield driver
    driver.close()


def seed(driver, database='SocioMap'):
    internal = 'AD941' if database == 'ArchaMap' else 'SD11'
    prefix = 'AM' if database == 'ArchaMap' else 'SM'
    ids = {key: prefix + value for key, value in [('keep','1'), ('drop','2'), ('parent','9'), ('child','99')]}
    with driver.session() as session:
        session.run('MATCH (n) DETACH DELETE n').consume()
        session.run('''UNWIND $labels AS p CREATE (l:LABEL) SET l=p''', labels=[
            {'CMName': 'AREA', 'groupLabel': 'AREA', 'label': 'AREA', 'public': 'TRUE',
             'relationship': 'AREA_OF', 'contextualTargetMode': 'ALL_EXCEPT'},
            {'CMName': 'ADM2', 'groupLabel': 'AREA', 'label': 'ADM2', 'public': 'TRUE'},
            {'CMName': 'POLITY', 'groupLabel': 'POLITY', 'label': 'POLITY', 'public': 'TRUE',
             'relationship': 'POLITY_OF', 'contextualTargetMode': 'ONLY', 'contextualTargetDomains': ['ETHNICITY']},
            {'CMName': 'ETHNICITY', 'groupLabel': 'ETHNICITY', 'label': 'ETHNICITY',
             'public': 'TRUE', 'contextualTargetMode': 'NONE'},
        ]).consume()
        props = [
            {'CMName': 'parent', 'relationship': 'CONTAINS', 'metaType': 'list'},
            {'CMName': 'polity', 'relationship': 'POLITY_OF', 'groupLabel': 'POLITY', 'metaType': 'list'},
            {'CMName': 'Name', 'metaType': 'list'}, {'CMName': 'parentContext', 'metaType': 'list'},
            {'CMName': 'Key', 'metaType': 'string'}, {'CMName': 'label', 'metaType': 'string'},
            {'CMName': 'eventType', 'metaType': 'list', 'allowedValues': ['HIERARCHY', 'FOLLOWS', 'SPLIT']},
        ]
        session.run('UNWIND $props AS p CREATE (n:PROPERTY) SET n=p,n.type="relationship"', props=props).consume()
        session.run('''CREATE (keep:CATEGORY:ADM2:AREA {CMID:$keep,CMName:'Mecca'}),
            (drop:CATEGORY:ADM2:AREA {CMID:$drop,CMName:'District'}),
            (parent:CATEGORY:AREA {CMID:$parent,CMName:'County'}),
            (child:CATEGORY:ADM2:AREA {CMID:$child,CMName:'Child'}),
            (internal:DATASET {CMID:$internal,shortName:'Internal'}),
            (external:DATASET {CMID:'external',shortName:'External'}),
            (internal)-[:USES {Key:'Key == keep',Name:['Mecca'],label:'ADM2',parent:[$drop],parentContext:$keepContext}]->(keep),
            (internal)-[:USES {Key:'Key == drop',Name:['District'],label:'ADM2',parent:[$parent],parentContext:$dropContext}]->(drop),
            (external)-[:USES {Key:'Key == child',Name:['Child'],label:'ADM2',parent:[$drop],parentContext:$childContext}]->(child),
            (drop)-[:CONTAINS]->(keep), (parent)-[:CONTAINS]->(drop), (drop)-[:CONTAINS]->(child)
        ''', **ids, internal=internal,
            keepContext=[json.dumps({'parent': ids['drop'], 'eventType': 'HIERARCHY'})],
            dropContext=[json.dumps({'parent': ids['parent'], 'eventType': 'HIERARCHY'})],
            childContext=[json.dumps({'parent': ids['drop'], 'eventType': 'FOLLOWS', 'eventDate': '2000'})]).consume()
    return ids


def snapshot(driver):
    return (utils.getQuery('MATCH (n) RETURN elementId(n) AS id,labels(n) AS labels,properties(n) AS properties ORDER BY id', driver),
            utils.getQuery('MATCH (a)-[r]->(b) RETURN elementId(r) AS id,elementId(a) AS start,elementId(b) AS end,type(r) AS type,properties(r) AS properties ORDER BY id', driver))


@pytest.mark.parametrize('database', ['SocioMap', 'ArchaMap'])
@pytest.mark.parametrize('remove', [True, False])
def test_merge_commits_sources_nodes_events_and_explicit_parent_choice(isolated_graph, database, remove):
    driver = isolated_graph
    ids = seed(driver, database)
    plan = plan_merge_references(driver, ids['keep'], ids['drop'])
    result = admin.mergeNodes(ids['keep'], ids['drop'], '7', database, remove, plan['cyclicParentToken'])
    assert isinstance(result, list), result
    rows = utils.getQuery('''MATCH (:DATASET)-[r:USES]->(n:CATEGORY {CMID:$keep})
        RETURN r.parent AS parent,r.parentContext AS contexts,r.Name AS names,r.Key AS key''', driver, params=ids)
    assert len(rows) == 1
    assert set(rows[0]['parent']) == ({ids['parent']} if remove else {ids['parent'], ids['keep']})
    assert set(rows[0]['names']) == {'Mecca', 'District'}
    assert rows[0]['key'] == 'Key == keep'
    assert {json.loads(value)['parent'] for value in rows[0]['contexts']} == set(rows[0]['parent'])
    assert utils.getQuery('MATCH (n:CATEGORY {CMID:$drop}) RETURN count(n) AS count', driver, params=ids)[0]['count'] == 0
    assert utils.getQuery('MATCH (:DELETED {CMID:$drop})-[:IS]->(:CATEGORY {CMID:$keep}) RETURN count(*) AS count', driver, params=ids)[0]['count'] == 1
    assert utils.getQuery('MATCH (n:CATEGORY {CMID:$keep})-[r:CONTAINS]->(n) RETURN count(r) AS count', driver, params=ids)[0]['count'] == (0 if remove else 1)
    child = utils.getQuery('''MATCH (:CATEGORY {CMID:$keep})-[r:CONTAINS]->(:CATEGORY {CMID:$child})
        RETURN r.eventType AS types,r.eventDate AS dates''', driver, params=ids)
    assert child == [{'types': ['FOLLOWS'], 'dates': ['2000']}]


def test_late_reconciliation_failure_restores_exact_graph(isolated_graph, monkeypatch):
    driver = isolated_graph
    ids = seed(driver)
    before = snapshot(driver)
    plan = plan_merge_references(driver, ids['keep'], ids['drop'])
    original = admin.processUSES
    def fail_after_reconciliation(**kwargs):
        result = original(**kwargs)
        assert not isinstance(result, tuple), result
        return 'injected failure after all reconciliation writes', 500
    monkeypatch.setattr(admin, 'processUSES', fail_after_reconciliation)
    result = admin.mergeNodes(ids['keep'], ids['drop'], '7', 'SocioMap', True, plan['cyclicParentToken'])
    assert result[1] == 500
    assert 'injected failure' in result[0]
    assert snapshot(driver) == before


def test_unconfirmed_or_stale_choice_never_changes_graph(isolated_graph):
    driver = isolated_graph
    ids = seed(driver)
    before = snapshot(driver)
    for choice, token in [(None, None), (True, 'stale'), ('false', 'stale')]:
        result = admin.mergeNodes(ids['keep'], ids['drop'], '7', 'SocioMap', choice, token)
        assert result[1] == 500
        assert snapshot(driver) == before
