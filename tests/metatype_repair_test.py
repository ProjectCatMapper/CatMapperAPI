import uuid

import pytest

import CM.routines as routines


def test_invalid_boolean_is_reported_without_writing(monkeypatch):
    calls = []
    monkeypatch.setattr(routines, "getDriver", lambda database: object())
    monkeypatch.setattr(routines, "getPropertiesMetadata", lambda driver: [
        {"property": "modifiedByOtherUser", "metaType": "boolean", "type": kind}
        for kind in ("node", "relationship")
    ])

    def query(query, driver, type):
        calls.append(query)
        assert "SET " not in query
        return [1]

    monkeypatch.setattr(routines, "getQuery", query)
    result = routines.fixMetaTypes("archamap")
    assert result["status"] == "partial"
    assert len(result["errors"]) == 2
    assert all("cannot be safely converted" in error for error in result["errors"])
    assert len(calls) == 2


@pytest.mark.realdb
@pytest.mark.parametrize("relationship", [False, True])
def test_boolean_repair_preserves_values_and_is_idempotent(realdb_driver, monkeypatch, relationship):
    # Only this random test property is repaired; all fixture writes roll back.
    prop = "semanticAuditTest" + uuid.uuid4().hex
    alias = "rel" if relationship else "n"
    match = "MATCH ()-[rel]->()" if relationship else "MATCH (n)"
    with realdb_driver.session() as session:
        tx = session.begin_transaction()
        try:
            pattern = "()-[rel:SEMANTIC_AUDIT_TEST]->()" if relationship else "(n)"
            tx.run(
                f"UNWIND $values AS value CREATE {pattern} SET {alias}.`{prop}` = value",
                values=[True, False, "true", "false"],
            ).consume()

            def query(query, driver, type):
                return [record[0] for record in tx.run(query)]

            monkeypatch.setattr(routines, "getQuery", query)
            assert routines._repair_property_metatype(realdb_driver, prop, "BOOLEAN", relationship=relationship) == 2
            assert routines._repair_property_metatype(realdb_driver, prop, "BOOLEAN", relationship=relationship) == 0
            rows = tx.run(
                f"{match} WHERE {alias}.`{prop}` IS NOT NULL "
                f"RETURN {alias}.`{prop}` AS value, valueType({alias}.`{prop}`) AS type"
            ).data()
            assert all(row["type"] == "BOOLEAN NOT NULL" for row in rows)
            assert sorted(row["value"] for row in rows) == [False, False, True, True]

            tx.run(f"CREATE {pattern} SET {alias}.`{prop}` = 'invalid'").consume()
            with pytest.raises(ValueError, match="cannot be safely converted"):
                routines._repair_property_metatype(realdb_driver, prop, "BOOLEAN", relationship=relationship)
            assert tx.run(f"{match} WHERE {alias}.`{prop}` = 'invalid' RETURN count(*)").single()[0] == 1
        finally:
            tx.rollback()
