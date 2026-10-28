import json

import pytest

from CM import admin, context_policy, merge_references as references
from CMroutes import admin_routes


def source(cmid='SM1', **properties):
    return {'relID': 'rel-1', 'CMID': cmid, 'datasetID': 'SD11',
            'Key': 'Key == example', 'properties': properties}


@pytest.fixture
def plan_sources(monkeypatch):
    rows = []
    def query(text, driver, **kwargs):
        if 'RETURN m.CMName AS property' in text:
            return ['polity', 'occupation', 'variable', 'country', 'district', 'parent', 'language', 'religion']
        assert 'properties(r) AS properties' in text
        return rows
    monkeypatch.setattr(references, 'getQuery', query)
    return rows


def test_area_merge_does_not_validate_nonexistent_polity(plan_sources, monkeypatch):
    plan_sources.append(source(parent=['SM2']))
    plan = references.plan_merge_references(object(), 'SM1', 'SM2')
    assert plan['updates'] == [{'relID': 'rel-1', 'properties': {'parent': ['SM1']}}]
    # Exercise the real metadata validator in the mode that exposed CATMA-322.
    monkeypatch.setenv('CATMAPPER_CONTEXT_POLICY', 'metadata')
    policy = context_policy.parse_policy([
        {'label': 'AREA', 'groupLabel': 'AREA', 'relationship': 'AREA_OF',
         'contextualTargetMode': 'ALL_EXCEPT'},
        {'label': 'POLITY', 'groupLabel': 'POLITY', 'relationship': 'POLITY_OF',
         'contextualTargetMode': 'ONLY', 'contextualTargetDomains': ['ETHNICITY']},
        {'label': 'ETHNICITY', 'groupLabel': 'ETHNICITY', 'contextualTargetMode': 'NONE'},
    ], [{'property': 'polity', 'groupLabel': 'POLITY', 'relationship': 'POLITY_OF'}])
    monkeypatch.setattr(context_policy, 'load_policy', lambda driver: policy)
    monkeypatch.setattr(context_policy, 'getQuery', lambda *args, **kwargs: [
        {'cmid': cmid, 'labels': ['CATEGORY', 'AREA']} for cmid in kwargs['params']['cmids']])
    references.validate_merge_plan(object(), plan)
    with pytest.raises(ValueError, match='POLITY_OF'):
        context_policy.validate_rows(object(), [{'CMID': 'SM1', 'polity': 'SM1'}])


@pytest.mark.parametrize('remove', [True, False])
def test_explicit_cycle_choice_preserves_other_parents_and_events(plan_sources, remove):
    untouched = '{"parent":"SM9","eventType":"SPLIT","eventDate":"2008","note":"SM2"}'
    plan_sources.append(source(parent=['SM2', 'SM9'], parentContext=[
        '{"parent":"SM2","eventType":"HIERARCHY"}', untouched]))
    plan = references.plan_merge_references(object(), 'SM1', 'SM2', remove_cyclic_parents=remove)
    assert len(plan['cyclicParentReferences']) == 2
    references.validate_merge_choice(plan, remove, plan['cyclicParentToken'])
    changes = plan['updates'][0]['properties']
    assert changes['parent'] == (['SM9'] if remove else ['SM1', 'SM9'])
    if remove:
        assert changes['parentContext'] == [untouched]
    else:
        assert json.loads(changes['parentContext'][0]) == {'parent': 'SM1', 'eventType': 'HIERARCHY'}
        assert changes['parentContext'][1] == untouched
    assert plan_sources[0]['properties']['parent'] == ['SM2', 'SM9']


@pytest.mark.parametrize('cmid,parent', [('SM1', 'SM2'), ('SM2', 'SM1'), ('SM2', 'SM2'), ('SM1', 'SM1')])
def test_cycle_detection_covers_both_merge_nodes_and_existing_self_parents(plan_sources, cmid, parent):
    plan_sources.append(source(cmid, parent=[parent], parentContext=[json.dumps({'parent': parent})]))
    plan = references.plan_merge_references(object(), 'SM1', 'SM2', remove_cyclic_parents=True)
    assert plan['updates'][0]['properties'] == {'parent': None, 'parentContext': None}


def test_context_only_reference_is_rewritten_exactly(plan_sources):
    untouched = '{"parent":"SM20","eventType":"HIERARCHY","note":"SM2"}'
    plan_sources.append(source('SM99', parentContext=[
        '{"parent":"SM2","eventType":"FOLLOWS","eventDate":"2000"}', untouched]))
    plan = references.plan_merge_references(object(), 'SM1', 'SM2')
    assert plan['cyclicParentReferences'] == []
    values = plan['updates'][0]['properties']['parentContext']
    assert json.loads(values[0]) == {'parent': 'SM1', 'eventType': 'FOLLOWS', 'eventDate': '2000'}
    assert values[1] == untouched


@pytest.mark.parametrize('choice', [None, 'false', 0, 1, [], {}])
def test_cycle_requires_explicit_boolean_choice(plan_sources, choice):
    plan_sources.append(source(parent=['SM2']))
    plan = references.plan_merge_references(object(), 'SM1', 'SM2')
    with pytest.raises(ValueError, match='Yes or No'):
        references.validate_merge_choice(plan, choice, plan['cyclicParentToken'])


def test_changed_cycle_invalidates_previous_confirmation(plan_sources):
    plan_sources.append(source(parent=['SM2']))
    before = references.plan_merge_references(object(), 'SM1', 'SM2')
    plan_sources[0]['properties']['parentContext'] = ['{"parent":"SM2"}']
    after = references.plan_merge_references(object(), 'SM1', 'SM2')
    with pytest.raises(ValueError, match='Preview the merge again'):
        references.validate_merge_choice(after, True, before['cyclicParentToken'])
    plan_sources.clear()
    after = references.plan_merge_references(object(), 'SM1', 'SM2')
    with pytest.raises(ValueError, match='Parent references changed'):
        references.validate_merge_choice(after, True, before['cyclicParentToken'])


def test_replace_property_with_no_actual_changes_skips_policy_values(monkeypatch):
    monkeypatch.setattr(admin, 'getDriver', lambda database: object())
    monkeypatch.setattr(admin, 'getQuery', lambda *args, **kwargs: [])
    validated = []
    monkeypatch.setattr(context_policy, 'validate_rows', lambda driver, rows: validated.extend(rows))
    assert admin.replaceProperty(['SM1'], 'polity', 'SM2', 'SM1', 'SocioMap').startswith('Completed')
    assert validated == []


def test_invalid_actual_replacement_is_rejected_before_writes(monkeypatch):
    monkeypatch.setattr(admin, 'getDriver', lambda database: object())
    def query(text, *args, **kwargs):
        assert 'SET ' not in text
        return [{'CMID': 'SM9', 'relID': 'rel', 'value': ['SM2', 'SM8']}]
    monkeypatch.setattr(admin, 'getQuery', query)
    def validate(driver, rows):
        assert rows == [{'CMID': 'SM9', 'language': ['SM1', 'SM8']}]
        raise ValueError('wrong reference domain')
    monkeypatch.setattr(context_policy, 'validate_rows', validate)
    assert admin.replaceProperty(['SM9'], 'language', 'SM2', 'SM1', 'SocioMap') == ('wrong reference domain', 500)


def test_preview_requires_auth_and_does_not_write(client, monkeypatch, plan_sources):
    monkeypatch.setattr(admin_routes, 'verify_request_auth', lambda **kwargs: {'userid': '7', 'role': 'user'})
    monkeypatch.setattr(admin_routes, 'getDriver', lambda database: object())
    monkeypatch.setattr(admin_routes, 'getNodeMergeSummary', lambda cmid, driver: {
        'CMID': cmid, 'CMName': cmid, 'primaryDomain': 'AREA'})
    plan_sources.append(source(parent=['SM2']))
    response = client.post('/admin/merge-preview', json={
        'database': 'SocioMap', 'keepcmid': 'SM1', 'deletecmid': 'SM2'})
    assert response.status_code == 200
    assert len(response.json['cyclicParentReferences']) == 1
    assert len(response.json['cyclicParentToken']) == 64
    def deny(**kwargs):
        raise ValueError('User not authorized')
    monkeypatch.setattr(admin_routes, 'verify_request_auth', deny)
    assert client.post('/admin/merge-preview', json={}).status_code in (400, 401, 403)


@pytest.mark.parametrize('choice', [True, False])
def test_merge_route_passes_choice_to_atomic_merge(choice, monkeypatch):
    seen = {}
    def merge(*args, **kwargs):
        seen.update(kwargs)
        return ['completed']
    monkeypatch.setattr(admin_routes, 'mergeNodes', merge)
    for action, payload, data in [
        ('merge nodes', {'s1_2': 'SM1', 's1_3': 'SM2', 'removeCyclicParents': choice, 'cyclicParentToken': 'token'}, {}),
        ('mergeNodes', {}, {'keepcmid': 'SM1', 'deletecmid': 'SM2', 'removeCyclicParents': choice, 'cyclicParentToken': 'token'}),
    ]:
        assert admin_routes._execute_admin_edit('SocioMap', action, '7', payload, data) == ['completed']
        assert seen == {'remove_cyclic_parents': choice, 'cyclic_parent_token': 'token'}


def test_missing_cycle_choice_never_enters_review_queue(client, monkeypatch, plan_sources):
    plan_sources.append(source(parent=['SM2']))
    monkeypatch.setattr(admin_routes, 'verify_request_auth', lambda **kwargs: {'userid': '7', 'role': 'user'})
    monkeypatch.setattr(admin_routes, 'getDriver', lambda database: object())
    monkeypatch.setattr(admin_routes, 'getNodeMergeSummary', lambda cmid, driver: {'primaryDomain': 'AREA'})
    monkeypatch.setattr(admin_routes, '_create_change_review', lambda **kwargs: pytest.fail('Invalid merge was queued'))
    monkeypatch.setattr(admin_routes, '_authorize_admin_edit_function', lambda **kwargs: pytest.fail('Ownership checked before merge validation'))
    response = client.post('/admin/edit', headers={'Authorization': 'Bearer test'}, json={
        'database': 'SocioMap', 'fun': 'merge nodes', 'input': {'s1_2': 'SM1', 's1_3': 'SM2'}})
    assert response.status_code >= 400
    assert 'Yes or No' in response.get_data(as_text=True)


def test_failed_merge_is_raised_for_approval_worker(monkeypatch):
    monkeypatch.setattr(admin_routes, 'mergeNodes', lambda *args, **kwargs: ('reconciliation failed', 500))
    with pytest.raises(ValueError, match='reconciliation failed'):
        admin_routes._execute_merge('SocioMap', 'SM1', 'SM2', '7', {})
