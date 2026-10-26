import copy
import importlib.util
from pathlib import Path

import pytest

from CM import context_policy as policy


spec = importlib.util.spec_from_file_location("contextual_migration", Path(__file__).parents[1] / "scripts/contextual_policy_migration.py")
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def fixture_policy(database):
    names = {"AREA", "ATTRIBUTE", "VARIABLE", "GENERIC"}
    names |= ({"LANGUOID", "RELIGION", "POLITY", "OCCUPATION", "ETHNICITY"}
              if database == "sociomap" else {"PERIOD", "CULTURE", "CERAMIC", "PROJECTILE_POINT", "BIOTA", "BUILT", "MATERIAL"})
    labels = [{"id": name, "properties": {"CMName": name, "groupLabel": name}} for name in names]
    labels += [{"id": "PPL", "properties": {"CMName": "PPL", "groupLabel": "AREA"}},
               {"id": "SITE", "properties": {"CMName": "SITE", "groupLabel": "AREA"}}]
    properties = [{"properties": {"CMName": ("language" if name == "LANGUOID" else "district" if name == "AREA" else name.lower()),
                                  "type": "relationship", "groupLabel": name, "relationship": rule[0]}}
                  for name, rule in migration.RULES[database].items()]
    return migration.parsed_policy(migration.desired_labels(labels, database), properties)


@pytest.mark.parametrize("database", ["sociomap", "archamap"])
def test_complete_directional_matrix(database):
    data = fixture_policy(database)
    for source in data["domains"]:
        for target in data["domains"]:
            if source == "AREA":
                expected = True
            elif source == "VARIABLE":
                expected = target == "ATTRIBUTE"
            elif database == "sociomap":
                forbidden = {"LANGUOID": {"AREA", "LANGUOID"},
                             "RELIGION": {"AREA", "LANGUOID", "RELIGION"},
                             "POLITY": {"AREA", "LANGUOID", "RELIGION", "POLITY"}}
                expected = (target == "ETHNICITY" if source == "OCCUPATION" else
                            target not in forbidden[source] if source in forbidden else False)
            else:
                expected = (target not in {"AREA", "PERIOD"} if source == "PERIOD" else
                            target not in {"AREA", "PERIOD", "CULTURE"} if source == "CULTURE" else False)
            assert (target in data["domains"][source]["allowed"]) == expected, (source, target)


def test_missing_metadata_fails_closed(monkeypatch):
    monkeypatch.setattr(policy, "getQuery", lambda *a, **k: [])
    with pytest.raises(ValueError, match="missing"):
        policy.load_policy(object())


@pytest.mark.parametrize("mode,targets", [(None, []), ("ONLY", "AREA"), ("ALL_EXCEPT", ["UNKNOWN"]), ("NONE", ["AREA"])])
def test_invalid_policy_rejected(mode, targets):
    with pytest.raises(ValueError):
        policy.parse_policy([{"label": "AREA", "groupLabel": "AREA", "relationship": "AREA_OF",
                              "contextualTargetMode": mode, "contextualTargetDomains": targets}], [])


def test_mismatched_property_source_rejected():
    with pytest.raises(ValueError, match="PROPERTY"):
        policy.parse_policy([{"label": "AREA", "groupLabel": "AREA", "relationship": "AREA_OF",
                              "contextualTargetMode": "ALL_EXCEPT", "contextualTargetDomains": []}],
                            [{"property": "language", "groupLabel": "AREA", "relationship": "LANGUOID_OF"}])


@pytest.mark.parametrize("database,label,prop,ref_domain,allowed", [
    ("sociomap", "PPL", "language", "LANGUOID", False),
    ("sociomap", "ETHNICITY", "occupation", "OCCUPATION", True),
    ("sociomap", "RELIGION", "polity", "POLITY", False),
    ("archamap", "CERAMIC", "period", "PERIOD", True),
    ("archamap", "SITE", "culture", "CULTURE", False),
    ("archamap", "CULTURE", "period", "PERIOD", True),
    ("archamap", "PERIOD", "culture", "CULTURE", False),
    ("archamap", "CERAMIC", "culture", "PROJECTILE_POINT", False),
])
def test_upload_new_category_and_multivalue_direction(monkeypatch, database, label, prop, ref_domain, allowed):
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    monkeypatch.setattr(policy, "getQuery", lambda *a, **k: [
        {"cmid": "AM1", "labels": ["CATEGORY", ref_domain]},
        {"cmid": "AM2", "labels": ["CATEGORY", ref_domain]},
    ])
    rows = [{"label": label, prop: "AM1; AM2", "_spreadsheet_row": 17}]
    if allowed:
        policy.validate_rows(object(), rows, fixture_policy(database))
    else:
        with pytest.raises(ValueError, match="row 17"):
            policy.validate_rows(object(), rows, fixture_policy(database))


def test_subtype_change_validates_existing_source_context(monkeypatch):
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    def query(text, *args, **kwargs):
        if "properties(u)" in text:
            return [{"CMID": "SM1", "properties": {"language": ["SM2"]}}]
        return [{"cmid": "SM1", "labels": ["CATEGORY", "ETHNICITY"]},
                {"cmid": "SM2", "labels": ["CATEGORY", "LANGUOID"]}]
    monkeypatch.setattr(policy, "getQuery", query)
    with pytest.raises(ValueError, match="LANGUOID_OF"):
        policy.validate_rows(object(), [{"CMID": "SM1", "label": "PPL"}], fixture_policy("sociomap"))


def test_reconciliation_checks_policy_before_any_writes(monkeypatch):
    from CM import USES
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    monkeypatch.setattr(USES, "getDriver", lambda *a: object())
    monkeypatch.setattr(policy, "stored_conflicts", lambda *a, **k: [
        {"datasetID": "SD1", "CMID": "SM1", "property": "language", "reference": "SM2"}])
    monkeypatch.setattr(USES, "getPropertiesMetadata", lambda **k: pytest.fail("Reconciliation continued"))
    result = USES.processUSES("SocioMap")
    assert result[1] == 500
    assert "require review" in result[0]


def test_legacy_rollout_mode_preserves_current_writes(monkeypatch):
    monkeypatch.delenv("CATMAPPER_CONTEXT_POLICY", raising=False)
    monkeypatch.setattr(policy, "load_policy", lambda *a: pytest.fail("Loaded policy during legacy rollout"))
    policy.validate_rows(object(), [{"language": "SM1"}])
    policy.validate_stored(object())


def test_site_cleanup_requires_equivalent_live_provenance():
    edge = {"source": "AM1", "target": "AM2", "properties": {"referenceKey": ["D Key: Type: Example"]},
            "replacements": [{"id": "edge", "properties": {"referenceKey": ["D Key: Type == Example"]}}]}
    support = [{"site": "AM1", "target": "AM2", "referenceKey": "D Key: Type == Example"}]
    original = copy.deepcopy(edge)
    migration.verify_site_provenance([edge], support)
    assert edge == original
    with pytest.raises(ValueError, match="provenance"):
        migration.verify_site_provenance([edge], [])


def test_taxonomy_retained_when_legacy_relationship_removed():
    rows = [{"id": "species", "properties": {"CMName": "SPECIES", "groupLabel": "BIOTA", "relationship": "FAUNA_OF"}}]
    result = migration.desired_labels(rows, "archamap")
    assert result == [{"id": "species", "properties": {"CMName": "SPECIES", "groupLabel": "BIOTA"}}]
    assert rows[0]["properties"]["relationship"] == "FAUNA_OF"


@pytest.mark.parametrize("writer", ["create", "update", "upload"])
def test_write_boundaries_reject_before_mutation(monkeypatch, writer):
    from CM import upload
    import pandas as pd
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    monkeypatch.setattr(policy, "load_policy", lambda *a: fixture_policy("sociomap"))
    monkeypatch.setattr(policy, "getQuery", lambda *a, **k: [
        {"cmid": "SM1", "labels": ["CATEGORY", "AREA", "PPL"]},
        {"cmid": "SM2", "labels": ["CATEGORY", "LANGUOID"]}])
    monkeypatch.setattr(upload, "getDriver", lambda *a: object())
    monkeypatch.setattr(upload, "getQuery", lambda *a, **k: pytest.fail("Graph writer reached"))
    monkeypatch.setattr(upload, "updateLog", lambda *a, **k: None)
    monkeypatch.setattr(upload, "validate_event_rows", lambda *a, **k: None)
    monkeypatch.setattr(upload, "validate_upload_ownership_scope", lambda *a, **k: None)
    rows = [{"CMID": "SM1", "datasetID": "SD1", "Key": "Name == Village", "language": "SM2"}]
    if writer == "create":
        with pytest.raises(ValueError, match="Prohibited contextual"):
            upload.createUSES(pd.DataFrame(rows), "SocioMap", "test")
        return
    elif writer == "update":
        result = upload.updateProperty(pd.DataFrame(rows), ["language"], False, "SocioMap", "test", "overwrite")
    else:
        with pytest.raises(ValueError, match="Prohibited contextual"):
            upload.input_Nodes_Uses(rows, "SocioMap", "add_uses", user="test")
        return
    assert "Prohibited contextual" in str(result)


def test_invalid_context_never_becomes_approval_request(monkeypatch):
    from CMroutes import admin_routes
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    monkeypatch.setattr(policy, "load_policy", lambda *a: fixture_policy("sociomap"))
    monkeypatch.setattr(policy, "getQuery", lambda *a, **k: [
        {"cmid": "SM1", "labels": ["CATEGORY", "AREA"]},
        {"cmid": "SM2", "labels": ["CATEGORY", "LANGUOID"]}])
    monkeypatch.setattr(admin_routes, "getDriver", lambda *a: object())
    monkeypatch.setattr(admin_routes, "getQuery", lambda *a, **k: [{"properties": {"Key": "Name == Village"}}])
    payload = {"s1_1": "add", "s1_2": "SM1", "s1_3": "SM2", "s1_7": 1, "s1_8": "language",
               "s1_4": [[{"CMID": "SM1"}, {"id": "r1", "Key": "Name == Village"}, {"CMID": "SD1"}]]}
    with pytest.raises(admin_routes.InvalidUsesEdit, match="Prohibited contextual"):
        admin_routes._validate_uses_edit_request("SocioMap", "add/edit/delete USES property", payload)
    payload["s1_1"] = "delete"
    monkeypatch.setattr(admin_routes, "getQuery", lambda *a, **k: [{"properties": {"Key": "Name == Village", "language": ["SM2"]}}])
    admin_routes._validate_uses_edit_request("SocioMap", "add/edit/delete USES property", payload)


def test_policy_endpoint_uses_database_metadata(client, monkeypatch):
    from CMroutes import metadata_routes
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    monkeypatch.setattr(metadata_routes, "getDriver", lambda *a: object())
    monkeypatch.setattr(policy, "load_policy", lambda *a: fixture_policy("archamap"))
    response = client.get("/api/databases/ArchaMap/metadata/contextual-policy")
    assert response.status_code == 200
    assert response.json["enabled"] is True
    assert response.json["domains"]["PERIOD"]["allowed"] == sorted(fixture_policy("archamap")["domains"]["PERIOD"]["allowed"])


def test_relationship_audit_keeps_domains_without_contextual_relationship(monkeypatch):
    from CM import routines
    captured = []
    monkeypatch.setattr(routines, "getDriver", lambda *a: object())
    def query(text, *a, **k):
        captured.append(text)
        if "RETURN DISTINCT l.groupLabel AS group" in text:
            return [{"group": "BIOTA", "relationship": None}, {"group": "PROJECTILE_POINT", "relationship": None}]
        return []
    monkeypatch.setattr(routines, "getQuery", query)
    result = routines.getBadRelations("ArchaMap")
    assert result["bad_relationship_labels_count"] == 0
    assert any("childLabel = $group" in text for text in captured)
    assert not any(":None" in text for text in captured)
    assert "l.CMName = l.groupLabel" in captured[0]


def test_new_primary_domain_defaults_to_no_contextual_relationships(monkeypatch):
    from CM import admin
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    monkeypatch.setattr(admin, "getDriver", lambda *a: object())
    created = []
    def query(text, driver=None, params=None):
        if "numericID" in text:
            return [{"numericID": 100}]
        if text.startswith("CREATE"):
            created.append(params["properties"])
        return []
    monkeypatch.setattr(admin, "getQuery", query)
    admin.createLabel("ArchaMap", "test", {"s1_2": "NEW_DOMAIN", "s1_7": "NA", "s1_5": "NEW_DOMAIN"})
    assert created[0]["contextualTargetMode"] == "NONE"
    assert created[0]["contextualTargetDomains"] == []
    assert created[0]["public"] == "TRUE"
    assert "relationship" not in created[0]


def test_new_subtypes_cannot_reintroduce_legacy_relationships(monkeypatch):
    from CM import admin
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    monkeypatch.setattr(admin, "getDriver", lambda *a: object())
    monkeypatch.setattr(admin, "getQuery", lambda *a, **k: pytest.fail("Metadata write reached"))
    with pytest.raises(ValueError, match="inherit"):
        admin.createLabel("ArchaMap", "test", {"s1_2": "SUBSPECIES", "s1_7": "BIOTA", "s1_3": "FAUNA_OF"})


def test_reclassifying_a_referenced_language_checks_incoming_source_values(monkeypatch):
    monkeypatch.setenv("CATMAPPER_CONTEXT_POLICY", "metadata")
    def query(text, *a, **k):
        if "RETURN DISTINCT c.CMID" in text:
            return [{"CMID": "SM1", "properties": {"language": ["SM2"]}}]
        if "properties(u)" in text:
            return []
        return [{"cmid": "SM1", "labels": ["CATEGORY", "ETHNICITY"]},
                {"cmid": "SM2", "labels": ["CATEGORY", "LANGUOID"]}]
    monkeypatch.setattr(policy, "getQuery", query)
    with pytest.raises(ValueError, match="LANGUOID_OF"):
        policy.validate_rows(object(), [{"CMID": "SM2", "label": "PPL"}], fixture_policy("sociomap"))
