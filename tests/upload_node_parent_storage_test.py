"""Node creation respects metadata scope when parent is also a USES field."""

import pandas as pd
import pytest

import CM.upload as upload


@pytest.fixture
def capture_creation(monkeypatch):
    captured = {}
    monkeypatch.setattr(upload, 'getDriver', lambda _: object())
    monkeypatch.setattr(upload, 'updateLog', lambda *a, **k: None)
    monkeypatch.setattr(upload, 'createLog', lambda *a, **k: None)
    monkeypatch.setattr(upload, '_stamp_nodes_with_ownership', lambda *a, **k: None)
    monkeypatch.setattr(upload, '_fetch_label_domain_map', lambda driver, labels: {'PERIOD': 'PERIOD'})
    monkeypatch.setattr(upload, 'getAvailableID', lambda **kw: ['ID1'] * kw['n'])

    # Simulate the actual overlapping metadata: parent is a dataset-node
    # property and a category-relationship property.
    definitions = [
        ('CMName', 'node', 'CATEGORY'), ('CMID', 'node', 'CATEGORY'),
        ('CMName', 'node', 'DATASET'), ('CMID', 'node', 'DATASET'),
        ('parent', 'node', 'DATASET'), ('parent', 'relationship', 'CATEGORY'),
    ]

    def query(query, driver=None, params=None, **kwargs):
        q = query
        if 'return p.CMName as property' in q:
            assert "p.type = 'node'" in q
            scope = params['node_scope']
            return [name for name, kind, node_scope in definitions
                    if kind == 'node' and node_scope == scope]
        if 'call apoc.cypher.doIt' in q:
            captured['query'] = q
            captured['rows'] = params['rows']
            row = params['rows'][0]
            # Return exactly the properties in the generated RETURN clause.
            cols = ['CMName', 'CMID', 'importID']
            if 'a.parent = row.parent' in q:
                cols.append('parent')
            return [{key: row[key] for key in cols} | {'nodeID': 'node-1'}]
        if 'WHERE n:STACK OR n:MERGING' in q:
            return []
        raise AssertionError(q)

    monkeypatch.setattr(upload, 'getQuery', query)
    return captured


@pytest.mark.parametrize('parent', ['', 'AM31607'])
def test_category_creation_does_not_write_uses_parent_to_node(capture_creation, parent):
    frame = pd.DataFrame([{'CMName': 'Period', 'label': 'PERIOD',
                           'importID': 1, 'parent': parent}])
    upload.createNodes(frame, 'archamap', False, '1', uniqueID='importID')
    assert 'a.parent = row.parent' not in capture_creation['query']
    # The caller retains the original value for the later USES creation.
    assert frame.loc[0, 'parent'] == parent


@pytest.mark.parametrize(('parent', 'expected'), [
    ('', []), ('AD123', ['AD123']), ('AD123; AD456', ['AD123', 'AD456']),
    (['AD123', 'AD456'], ['AD123', 'AD456']),
])
def test_dataset_parent_is_stored_as_list(capture_creation, parent, expected):
    frame = pd.DataFrame([{'CMName': 'Dataset', 'label': 'DATASET',
                           'importID': 1, 'parent': parent}])
    upload.createNodes(frame, 'archamap', True, '1', uniqueID='importID')
    assert 'a.parent = row.parent' in capture_creation['query']
    assert capture_creation['rows'][0]['parent'] == expected
