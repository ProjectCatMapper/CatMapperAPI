"""Exercise real Neo4j reconciliation inside a transaction that always rolls back."""
import json
import uuid

import pytest
import pandas as pd
import CM.upload as upload
import CM.metadata as metadata
import CM.log as log
import CM.event_types as events
import CM.USES as uses


@pytest.mark.realdb
def test_metadata_driven_pair_reconciliation_rolls_back(realdb_driver, realdb_database, monkeypatch):
    prefix = f"AMpytest_{uuid.uuid4().hex}"
    with realdb_driver.session() as session:
        transaction = session.begin_transaction()
        try:
            # The vocabulary change and fixtures are visible only in this tx.
            transaction.run("""
                MATCH (p:PROPERTY {CMName: 'eventType', type: 'relationship'})
                SET p.allowedValues = ['CUSTOM', 'HIERARCHY']
            """).consume()
            transaction.run("""
                CREATE (old:CATEGORY:TEST_TMP {CMID: $old}),
                       (current:CATEGORY:TEST_TMP {CMID: $current}),
                       (child:CATEGORY:TEST_TMP {CMID: $child}),
                       (dataset:DATASET:TEST_TMP {CMID: $dataset}),
                       (old)-[:CONTAINS {eventType: ['SPLIT'], eventDate: ['1999']}]->(child),
                       (current)-[:CONTAINS]->(child),
                       (dataset)-[:USES {Key: 'ID == 1', parent: [$old, $current], parentContext: $context}]->(child)
            """, old=prefix+'old', current=prefix+'current', child=prefix+'child',
                dataset=prefix+'dataset', context=[
                    json.dumps({'parent': prefix+'current', 'eventType': 'CUSTOM', 'eventDate': '2018'}),
                    json.dumps({'parent': prefix+'current', 'eventType': 'HIERARCHY'}),
                ]).consume()

            def query(query, driver=None, params=None, type="dict", **kwargs):
                rows = transaction.run(query, **(params or {}), **kwargs).data()
                if type == "list":
                    return [value for row in rows for value in row.values()]
                if type in {"df", "dataframe"}:
                    return pd.DataFrame(rows)
                return rows

            monkeypatch.setattr(events, "getQuery", query)
            monkeypatch.setattr(uses, "getQuery", query)
            monkeypatch.setattr(uses, "getDriver", lambda _: realdb_driver)
            result = uses.updateContains(realdb_database, prefix+'child')
            assert isinstance(result, dict), result
            rows = transaction.run("""
                MATCH (parent)-[r:CONTAINS]->(:CATEGORY {CMID: $child})
                RETURN parent.CMID AS parent, r.eventType AS eventType, r.eventDate AS eventDate
            """, child=prefix+'child').data()
            by_parent = {row['parent']: row for row in rows}
            assert by_parent[prefix+'old']['eventType'] is None
            assert by_parent[prefix+'old']['eventDate'] is None
            assert by_parent[prefix+'current']['eventType'] == ['CUSTOM', 'HIERARCHY']
            assert by_parent[prefix+'current']['eventDate'] == ['2018', 'NULL']
            # Reconciliation is idempotent and scoped to the requested child.
            assert uses.updateContains(realdb_database, prefix+'child')['query1'] == [{'updatedRelationships': 0}]
            # Remove the remaining event while other parentContext survives.
            transaction.run("""
                MATCH (:DATASET {CMID: $dataset})-[r:USES]->()
                SET r.parentContext = $context
            """, dataset=prefix+'dataset', context=[json.dumps({'parent': prefix+'old', 'eventDate': '2005'})]).consume()
            assert isinstance(uses.updateContains(realdb_database, prefix+'child'), dict)
            assert transaction.run("""
                MATCH ()-[r:CONTAINS]->(:CATEGORY {CMID: $child})
                RETURN count(r.eventType) AS remaining
            """, child=prefix+'child').single()['remaining'] == 0
            # A direct source-property edit must refresh derived events immediately,
            # without invoking a nightly routine or calling updateContains manually.
            transaction.run("""
                MATCH (child:CATEGORY {CMID: $child})
                SET child:AREA, child.CMName = 'Test child'
                WITH child
                MATCH (:DATASET {CMID: $dataset})-[r:USES]->(child)
                SET r.label = ['AREA'], r.Name = ['Test child']
            """, child=prefix+'child', dataset=prefix+'dataset').consume()
            rel_id = transaction.run("""
                MATCH (:DATASET {CMID: $dataset})-[r:USES]->()
                RETURN elementId(r) AS id
            """, dataset=prefix+'dataset').single()['id']
            for module in (upload, metadata, log):
                monkeypatch.setattr(module, "getQuery", query)
            monkeypatch.setattr(upload, "getDriver", lambda _: realdb_driver)
            monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
            result = upload.updateProperty(pd.DataFrame([{
                'relID': rel_id, 'CMID': prefix+'child', 'datasetID': prefix+'dataset',
                'Key': 'ID == 1', 'parent': [prefix+'old'],
                'parentContext': json.dumps({'parent': prefix+'old', 'eventType': 'CUSTOM', 'eventDate': '2019'}),
            }]), ['parent', 'parentContext'], False, realdb_database, '0', 'overwrite')
            assert isinstance(result, dict), result
            refreshed = transaction.run("""
                MATCH (:CATEGORY {CMID: $old})-[r:CONTAINS]->(:CATEGORY {CMID: $child})
                RETURN r.eventType AS eventType, r.eventDate AS eventDate
            """, old=prefix+'old', child=prefix+'child').single()
            assert refreshed['eventType'] == ['CUSTOM']
            assert refreshed['eventDate'] == ['2019']
            assert transaction.run("""
                MATCH (:DATASET {CMID: $dataset})-[r:USES]->()
                RETURN r.status AS status
            """, dataset=prefix+'dataset').single()['status'] is None
        finally:
            transaction.rollback()
    # Verify rollback independently: no fixtures or temporary vocabulary survive.
    with realdb_driver.session() as session:
        assert session.run("MATCH (n {CMID: $child}) RETURN count(n) AS count", child=prefix+'child').single()['count'] == 0
        assert session.run("""
            MATCH (p:PROPERTY {CMName: 'eventType', type: 'relationship'})
            RETURN p.allowedValues AS values
        """).single()['values'] == ['SPLIT', 'MERGED', 'SPLITMERGE', 'HIERARCHY', 'FOLLOWS']
