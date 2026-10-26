"""Every supported metatype needs explicit preservation and repair examples."""
import json
import uuid

import pytest

import CM.routines as routines
from CM.metatypes import METATYPE_STORAGE_TYPES, metadata_metatypes, storage_types


# Deliberately independent of the registry: a newly registered type fails the
# ordinary suite until its maintenance round-trip examples are supplied here.
ROUND_TRIP_CASES = {
    "STRING": {"valid": ["alpha", "42", ""], "repair": [42], "expected": ["42"]},
    "LIST": {"valid": [["alpha", "beta"], []], "repair": ["gamma"], "expected": [["gamma"]]},
    "BOOLEAN": {"valid": [True, False], "repair": ["true", "false"], "expected": [True, False]},
}


def test_every_metatype_has_maintenance_round_trip_examples():
    assert set(METATYPE_STORAGE_TYPES) == set(ROUND_TRIP_CASES)
    for case in ROUND_TRIP_CASES.values():
        assert case["valid"] and case["repair"] and case["expected"]


@pytest.mark.parametrize("meta_type", ["INTEGER", "FLOAT", "new_type", ""])
def test_unknown_metatype_stops_before_any_query(monkeypatch, meta_type):
    monkeypatch.setattr(routines, "getDriver", lambda database: object())
    monkeypatch.setattr(routines, "getPropertiesMetadata", lambda driver: [
        {"property": "first", "type": "node", "metaType": "string"},
        {"property": "new", "type": "node", "metaType": meta_type},
    ])
    monkeypatch.setattr(routines, "getQuery", lambda *args, **kwargs: pytest.fail("must validate before querying/writing"))
    result, status = routines.fixMetaTypes("archamap")
    assert status == 500 and result["status"] == "failed"
    assert "Unsupported metatype" in result["errors"][0]


def test_conflicting_storage_definitions_are_rejected():
    with pytest.raises(ValueError, match="Conflicting metatypes"):
        metadata_metatypes([
            {"property": "lock", "type": "node", "metaType": "boolean"},
            {"property": "lock", "type": "node", "metaType": "string"},
        ])


def test_invalid_boolean_is_reported_without_writing(monkeypatch):
    monkeypatch.setattr(routines, "getDriver", lambda database: object())
    monkeypatch.setattr(routines, "getPropertiesMetadata", lambda driver: [
        {"property": "modifiedByOtherUser", "metaType": "boolean", "type": kind}
        for kind in ("node", "relationship")
    ])

    def query(query, driver, type, params=None):
        assert "SET " not in query
        if type == "list":
            return [1]
        return [{"key": "modifiedByOtherUser", "types": ["STRING NOT NULL"], "count": 1}]

    monkeypatch.setattr(routines, "getQuery", query)
    result, status = routines.fixMetaTypes("archamap")
    assert status == 500 and result["status"] == "failed"
    assert sum("cannot be safely converted" in error for error in result["errors"]) == 2
    assert sum("unsupported storage types" in error for error in result["errors"]) == 2


@pytest.mark.parametrize("return_type", ["data", "info"])
def test_successful_formatter_cannot_hide_readback_type_mismatch(monkeypatch, return_type):
    monkeypatch.setattr(routines, "getDriver", lambda database: object())
    monkeypatch.setattr(routines, "getPropertiesMetadata", lambda driver: [
        {"property": "lock", "metaType": "boolean", "type": "node"}
    ])
    monkeypatch.setattr(routines, "_repair_property_metatype", lambda *args, **kwargs: 1)
    monkeypatch.setattr(routines, "getQuery", lambda *args, **kwargs: [
        {"key": "lock", "types": ["STRING NOT NULL"], "count": 1}
    ])
    result, status = routines.fixMetaTypes("archamap", return_type=return_type)
    assert status == 500 and result["status"] == "failed"
    assert "unsupported storage types" in result["errors"][0]


@pytest.mark.realdb
@pytest.mark.parametrize("meta_type", sorted(ROUND_TRIP_CASES))
@pytest.mark.parametrize("relationship", [False, True])
def test_maintenance_round_trip_preserves_valid_values_and_is_idempotent(realdb_driver, monkeypatch, relationship, meta_type):
    case = ROUND_TRIP_CASES[meta_type]
    prop = "semanticAuditTest" + uuid.uuid4().hex
    alias = "rel" if relationship else "n"
    match = "MATCH ()-[rel:SEMANTIC_AUDIT_TEST]->()" if relationship else "MATCH (n:SEMANTIC_AUDIT_TEST)"
    pattern = "(:SEMANTIC_AUDIT_TEST)-[rel:SEMANTIC_AUDIT_TEST]->(:SEMANTIC_AUDIT_TEST)" if relationship else "(n:SEMANTIC_AUDIT_TEST)"
    monkeypatch.setattr(routines, "getDriver", lambda database: realdb_driver)
    monkeypatch.setattr(routines, "getPropertiesMetadata", lambda driver: [
        {"property": prop, "metaType": meta_type, "type": "relationship" if relationship else "node"}
    ])
    with realdb_driver.session() as session:
        tx = session.begin_transaction()
        try:
            # All writes roll back. Restrict maintenance queries to fixture labels
            # and relationship types; production properties are never visited.
            updates = []
            def query(query, driver, type, params=None):
                query = query.replace("MATCH (n)", "MATCH (n:SEMANTIC_AUDIT_TEST)")
                query = query.replace("-[rel]->", "-[rel:SEMANTIC_AUDIT_TEST]->")
                query = query.replace("-[r]->", "-[r:SEMANTIC_AUDIT_TEST]->")
                result = tx.run(query, params or {}).data()
                if type == "list":
                    result = [next(iter(row.values())) for row in result]
                if "SET " in query:
                    updates.append(result[0])
                return result
            monkeypatch.setattr(routines, "getQuery", query)

            tx.run(f"UNWIND $values AS value CREATE {pattern} SET {alias}.`{prop}` = value", values=case["valid"]).consume()
            def values():
                return [r["value"] for r in tx.run(f"{match} WHERE {alias}.`{prop}` IS NOT NULL RETURN {alias}.`{prop}` AS value")]
            before = sorted(map(json.dumps, values()))
            assert routines.fixMetaTypes("test")["status"] == "success"
            assert sorted(map(json.dumps, values())) == before
            assert updates == [0]

            tx.run(f"UNWIND $values AS value CREATE {pattern} SET {alias}.`{prop}` = value", values=case["repair"]).consume()
            updates.clear()
            result = routines.fixMetaTypes("test")
            assert result["status"] == "success", result
            assert updates == [len(case["repair"])]
            assert sorted(map(json.dumps, values())) == sorted(map(json.dumps, case["valid"] + case["expected"]))
            updates.clear()
            assert routines.fixMetaTypes("test")["status"] == "success"
            assert updates == [0]

            if meta_type == "BOOLEAN":
                tx.run(f"CREATE {pattern} SET {alias}.`{prop}` = 'invalid'").consume()
                updates.clear()
                result, status = routines.fixMetaTypes("test")
                assert status == 500 and result["status"] == "failed"
                assert updates == []
                assert "invalid" in values()
        finally:
            tx.rollback()
