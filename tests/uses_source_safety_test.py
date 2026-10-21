"""Source USES assertions must survive derived-link reconciliation."""
import importlib.util
from pathlib import Path

import pytest
import CM.USES as uses


@pytest.mark.parametrize("relationship", ["USES", "uses", "MERGING", "HAS_LOG", "IS", "CONTAINS` DELETE r //", "UNKNOWN", None])
def test_direct_cleanup_rejects_non_derived_types_before_queries(monkeypatch, relationship):
    monkeypatch.setattr(uses, "getDriver", lambda _: pytest.fail("Unsafe cleanup reached a driver"))
    result = uses.fixUsesRels("SocioMap", "malformed", relationship, ["SM1"])
    assert result[1] == 500
    assert "Unsafe derived relationship" in result[0]


@pytest.mark.parametrize("relationship", ["USES", "AREA_OF", "CONTAINS"])
def test_surface_area_cannot_trigger_relationship_updates(monkeypatch, relationship):
    monkeypatch.setattr(uses, "getDriver", lambda _: pytest.fail("Value-only property reached a driver"))
    result = uses.fixUsesRels("SocioMap", "areakm2", relationship, ["SM1"])
    assert result[1] == 500
    assert "value-only" in result[0]


def test_process_rejects_corrupt_metadata_before_reconciliation(monkeypatch):
    monkeypatch.setattr(uses, "getDriver", lambda _: object())
    monkeypatch.setattr(uses, "getPropertiesMetadata", lambda **_: [
        {"property": "parent", "relationship": "CONTAINS"},
        {"property": "areakm2", "relationship": "USES"},
    ])
    for name in ["validate_stored_event_contexts", "updateAltNames", "updateLabels", "mergeDupRelations", "fixUsesRels", "updateContains", "getQuery"]:
        monkeypatch.setattr(uses, name, lambda *a, **kw: pytest.fail("Corrupt metadata reached reconciliation"))
    result = uses.processUSES("SocioMap", ["SM1"])
    assert result[1] == 500 and "value-only" in result[0]


@pytest.mark.parametrize("surface_area_present", [False, True])
def test_surface_area_presence_does_not_change_reconciliation(monkeypatch, surface_area_present):
    monkeypatch.setattr(uses, "getDriver", lambda _: object())
    metadata = [{"property": "parent", "relationship": "CONTAINS"}]
    if surface_area_present:
        metadata.append({"property": "areakm2", "relationship": None})
    monkeypatch.setattr(uses, "getPropertiesMetadata", lambda **_: metadata)
    monkeypatch.setattr(uses, "validate_stored_event_contexts", lambda *a: None)
    for name in ["updateAltNames", "updateLabels", "mergeDupRelations", "updateContains"]:
        monkeypatch.setattr(uses, name, lambda **_: [])
    called = []
    monkeypatch.setattr(uses, "fixUsesRels", lambda **kw: called.append(kw) or [])
    monkeypatch.setattr(uses, "getQuery", lambda *a, **kw: [])
    result = uses.processUSES("SocioMap", ["SM1"])
    assert isinstance(result, dict)
    assert [(r["property"], r["relationship"]) for r in called] == [("parent", "CONTAINS")]


@pytest.mark.parametrize("relationship", sorted(uses.DERIVED_CONTEXTUAL_RELATIONSHIPS))
def test_cleanup_has_query_guards_for_source_assertions(monkeypatch, relationship):
    monkeypatch.setattr(uses, "getDriver", lambda _: object())
    monkeypatch.setattr(uses, "_contextual_tie_primary_domain_conflicts", lambda *a: [])
    queries = []
    monkeypatch.setattr(uses, "getQuery", lambda query, **kw: queries.append(query) or [])
    result = uses.fixUsesRels("SocioMap", "parent", relationship, ["SM1"])
    assert not isinstance(result, tuple)
    deletion = [q for q in queries if "delete rel" in q.lower()]
    assert len(deletion) == 1
    assert "type(rel) <> 'USES'" in deletion[0]
    assert "match (n:CATEGORY)" in deletion[0]
    assert "NOT startNode(rel):DATASET" in deletion[0]
    assert "NOT endNode(rel):DATASET" in deletion[0]


def test_registration_never_assigns_relationship_updates():
    path = Path(__file__).resolve().parents[1] / "scripts/register_areakm2_property.py"
    spec = importlib.util.spec_from_file_location("register_areakm2", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.FIELDS["type"] == "relationship"
    assert "relationship" not in module.FIELDS
