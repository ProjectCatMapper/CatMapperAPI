"""Plan exact USES reference replacements before a category merge writes."""
import hashlib
import json

from . import context_policy
from .event_types import parent_context_entries, validate_event_rows
from .utils import getQuery


def replace_exact(value, old, new):
    if isinstance(value, list):
        return [new if item == old else item for item in value]
    return new if value == old else value


def replace_context(value, old, new, *, remove_parent=None):
    """Preserve untouched JSON and event fields; remove only chosen parents."""
    result = []
    entries = value if isinstance(value, list) else [value]
    for entry in entries:
        decoded = parent_context_entries(entry)
        changed = False
        updated = []
        for context in decoded:
            context = dict(context)
            if context.get('parent') == old:
                context['parent'] = new
                changed = True
            if remove_parent is not None and context.get('parent') == remove_parent:
                changed = True
                continue
            updated.append(context)
        if not changed:
            result.append(entry)
        else:
            result.extend(json.dumps(context) if isinstance(entry, str) else context
                          for context in updated)
    if isinstance(value, list):
        return result or None
    return result[0] if len(result) == 1 else result or None


def plan_merge_references(driver, keep, discard, *, remove_cyclic_parents=False, lock=False):
    props = getQuery('MATCH (m:PROPERTY) WHERE m.relationship IS NOT NULL '
                     'RETURN m.CMName AS property', driver, type='list')
    props = list(dict.fromkeys(props + ['parentContext']))
    query = """
        MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        WHERE c.CMID IN $cmids OR ANY(prop IN $contextProps WHERE
            ANY(value IN CASE WHEN valueType(r[prop]) STARTS WITH 'LIST<'
                THEN r[prop] ELSE [r[prop]] END
                WHERE toString(value) CONTAINS $discard))
    """
    if lock:
        query += """
            WITH c,r,d ORDER BY elementId(r)
            WITH collect({category:c,uses:r,dataset:d}) AS records
            CALL apoc.lock.rels([record IN records | record.uses])
            UNWIND records AS record
            WITH record.category AS c,record.uses AS r,record.dataset AS d
        """
    query += """
        RETURN elementId(r) AS relID,c.CMID AS CMID,d.CMID AS datasetID,
            r.Key AS Key,properties(r) AS properties
        ORDER BY relID
    """
    rows = getQuery(query, driver, params={'cmids': [keep, discard],
                    'contextProps': props, 'discard': discard})
    updates, validation, cycles = [], [], []
    for row in rows:
        final_cmid = keep if row['CMID'] == discard else row['CMID']
        original = row['properties']
        proposed = {}
        for prop in props:
            if prop not in original:
                continue
            value = original[prop]
            if prop == 'parentContext':
                updated = replace_context(value, discard, keep)
                self_parent = final_cmid == keep and any(
                    entry.get('parent') == keep for entry in parent_context_entries(updated))
            else:
                updated = replace_exact(value, discard, keep)
                self_parent = prop == 'parent' and final_cmid == keep and keep in (
                    updated if isinstance(updated, list) else [updated])
            if self_parent:
                cycles.append({key: row[key] for key in ('relID', 'CMID', 'datasetID', 'Key')} | {
                    'property': prop, 'value': value})
                if remove_cyclic_parents:
                    if prop == 'parentContext':
                        updated = replace_context(value, discard, keep, remove_parent=keep)
                    elif isinstance(updated, list):
                        updated = [parent for parent in updated if parent != keep] or None
                    else:
                        updated = None
            proposed[prop] = updated
        changes = {prop: value for prop, value in proposed.items() if value != original[prop]}
        if changes:
            updates.append({'relID': row['relID'], 'properties': changes})
        if changes or row['CMID'] in (keep, discard):
            validation.append({'CMID': final_cmid, **proposed})
    fingerprint = hashlib.sha256(json.dumps([keep, discard, cycles],
        sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return {'updates': updates, 'validationRows': validation,
            'cyclicParentReferences': cycles, 'cyclicParentToken': fingerprint}


def validate_merge_choice(plan, choice, token):
    if choice is not None and type(choice) is not bool:
        raise ValueError('Choose Yes or No for removing cyclical parent references.')
    if plan['cyclicParentReferences'] and (type(choice) is not bool or token != plan['cyclicParentToken']):
        raise ValueError('Preview the merge again and choose Yes or No for removing cyclical parent references.')
    if token is not None and token != plan['cyclicParentToken']:
        raise ValueError('Parent references changed. Preview the merge again.')


def validate_merge_plan(driver, plan):
    context_policy.validate_rows(driver, plan['validationRows'])
    validate_event_rows(plan['validationRows'], driver)


def apply_reference_updates(driver, updates):
    if updates:
        getQuery('''UNWIND $updates AS update MATCH ()-[r:USES]->()
            WHERE elementId(r) = update.relID SET r += update.properties''',
            driver, params={'updates': updates})
