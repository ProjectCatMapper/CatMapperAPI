import json

import pytest
from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import DCAT, DCTERMS, OWL, SKOS, XSD

from CM import linked_data


def category_record(**overrides):
    record = {
        "labels": ["CATEGORY", "ETHNICITY", "VARIABLE"],
        "cmid": "SM1",
        "name": "Aymara",
        "names": ["Aymara", "Aymará"],
        "description": "A public concept.",
        "ownerUserId": "must-not-leak",
        "log": ["must-not-leak"],
        "embedding": [0.1, 0.2],
    }
    record.update(overrides)
    return record


def assertion_row(**overrides):
    row = {
        "datasetCmid": "SD1",
        "conceptCmid": "SM1",
        "key": "ethnicity_code",
        "sourceName": ["Aymara", "Aimara"],
        "categoryType": "CATEGORICAL",
        "comment": "Reviewed source value",
        "descriptor": "self identification",
        "sourceUrl": "https://example.org/source",
        "recordStart": "1900-01",
        "recordEnd": "2000",
        "yearStart": "1900",
        "yearEnd": "not-a-year",
        "country": ["SM100"],
        "parentContext": ["SM200"],
        "stableDiscriminator": ["opaque-internal-value"],
        "ownerUserId": "must-not-leak",
        "logID": "must-not-leak",
    }
    row.update(overrides)
    return row


def test_projection_uses_canonical_iri_multi_roles_and_allowlist():
    graph = linked_data.project_resource("sociomap", category_record(), [assertion_row()])
    subject = URIRef("https://catmapper.org/sociomap/SM1")
    assertion = linked_data.assertion_iri({**assertion_row(), "database": "sociomap"})

    assert (subject, RDF.type, linked_data.CAT.Concept) in graph
    assert (subject, RDF.type, linked_data.CAT.Variable) in graph
    assert (subject, RDF.type, SKOS.Concept) in graph
    assert (subject, linked_data.CAT.cmid, Literal("SM1")) in graph
    assert (assertion, linked_data.CAT.assertionDataset, URIRef("https://catmapper.org/sociomap/SD1")) in graph
    assert (assertion, linked_data.CAT.yearStart, Literal(1900, datatype=XSD.integer)) in graph
    assert not list(graph.objects(assertion, linked_data.CAT.yearEnd))
    serialized = graph.serialize(format="nt")
    for forbidden in ("ownerUserId", "must-not-leak", "embedding", "logID", "elementId"):
        assert forbidden not in serialized


def test_database_iris_are_separate_and_unicode_labels_are_preserved():
    record = category_record(name="Aymará – Qhichwa", names=["Aymará – Qhichwa", "Аймара"])
    sociomap = linked_data.project_resource("sociomap", record)
    archamap = linked_data.project_resource("archamap", {**record, "cmid": "AM1"})
    socio_subject = URIRef("https://catmapper.org/sociomap/SM1")
    archa_subject = URIRef("https://catmapper.org/archamap/AM1")

    assert socio_subject != archa_subject
    assert (socio_subject, SKOS.prefLabel, Literal("Aymará – Qhichwa", lang="en")) in sociomap
    assert (socio_subject, SKOS.altLabel, Literal("Аймара")) in sociomap
    assert (archa_subject, linked_data.CAT.inDatabase, linked_data.CAT.ArchaMap) in archamap


def test_multiple_keys_and_one_to_many_assertions_remain_distinct():
    rows = [
        assertion_row(key="ethnicity_code", conceptCmid="SM1"),
        assertion_row(key="language_code", conceptCmid="SM1"),
        assertion_row(key="ethnicity_code", conceptCmid="SM2"),
    ]
    graph = linked_data.project_resource("sociomap", category_record(), rows)
    assertions = set(graph.subjects(RDF.type, linked_data.CAT.DatasetAssertion))

    assert len(assertions) == 3
    assert {str(value) for assertion in assertions for value in graph.objects(assertion, linked_data.CAT.key)} == {
        "ethnicity_code",
        "language_code",
    }
    assert {str(value) for assertion in assertions for value in graph.objects(assertion, linked_data.CAT.assertionConcept)} == {
        "https://catmapper.org/sociomap/SM1",
        "https://catmapper.org/sociomap/SM2",
    }


def test_unapproved_external_identity_fields_emit_no_mapping_or_owl_identity():
    record = category_record(
        EQUIVALENT=["https://www.wikidata.org/entity/Q123"],
        exactMatch=["https://example.org/concept"],
    )
    graph = linked_data.project_resource("sociomap", record)
    subject = URIRef("https://catmapper.org/sociomap/SM1")

    assert not list(graph.objects(subject, OWL.sameAs))
    assert not list(graph.objects(subject, OWL.equivalentClass))
    assert not list(graph.objects(subject, SKOS.exactMatch))
    assert not list(graph.objects(subject, SKOS.closeMatch))


def test_dataset_stack_is_both_semantic_classes():
    graph = linked_data.project_resource(
        "archamap",
        {
            "labels": ["DATASET", "STACK"],
            "cmid": "AD1",
            "name": "Stacked dataset",
            "yearPublished": "2024",
        },
    )
    subject = URIRef("https://catmapper.org/archamap/AD1")

    assert (subject, RDF.type, linked_data.CAT.Dataset) in graph
    assert (subject, RDF.type, linked_data.CAT.Stack) in graph
    assert (subject, RDF.type, DCAT.Dataset) in graph
    assert (subject, DCTERMS.issued, Literal("2024", datatype=XSD.gYear)) in graph


def test_assertion_identity_is_repeatable_and_collision_safe():
    row = {**assertion_row(), "database": "archamap"}
    base = linked_data.assertion_iri(row, multiplicity=1)
    repeated = linked_data.assertion_iri(row, multiplicity=1)
    collision_a = linked_data.assertion_iri(row, multiplicity=2)
    collision_b = linked_data.assertion_iri(
        {**row, "stableDiscriminator": ["different-stable-value"]},
        multiplicity=2,
    )

    assert base == repeated
    assert collision_a != collision_b
    assert "opaque-internal-value" not in str(collision_a)
    with pytest.raises(linked_data.MalformedPublicResource):
        linked_data.assertion_iri({**row, "stableDiscriminator": None}, multiplicity=2)


def test_only_approved_contains_meanings_are_projected():
    hierarchy = [
        {
            "direction": "out",
            "otherCmid": "AM2",
            "sourceLabels": ["CATEGORY", "GENUS", "BIOTA"],
            "targetLabels": ["CATEGORY", "SPECIES", "BIOTA"],
        },
        {
            "direction": "out",
            "otherCmid": "AM3",
            "sourceLabels": ["CATEGORY", "ADM0", "AREA"],
            "targetLabels": ["CATEGORY", "ADM1", "AREA"],
        },
    ]
    graph = linked_data.project_resource(
        "archamap",
        {"labels": ["CATEGORY", "GENUS", "BIOTA"], "cmid": "AM1", "name": "Genus"},
        hierarchy=hierarchy,
    )
    subject = URIRef("https://catmapper.org/archamap/AM1")

    assert (subject, linked_data.CAT.containsConcept, URIRef("https://catmapper.org/archamap/AM2")) in graph
    assert (subject, linked_data.CAT.containsConcept, URIRef("https://catmapper.org/archamap/AM3")) not in graph


def test_turtle_and_jsonld_are_semantically_equivalent(monkeypatch):
    graph = linked_data.project_resource("sociomap", category_record(), [assertion_row()])
    context = linked_data.load_jsonld_context()
    turtle = linked_data.serialize_graph(graph, "text/turtle")
    jsonld = json.loads(linked_data.serialize_graph(graph, "application/ld+json"))
    assert jsonld["@context"] == linked_data.CONTEXT_IRI
    jsonld["@context"] = context

    from_turtle = Graph().parse(data=turtle, format="turtle")
    from_jsonld = Graph().parse(data=json.dumps(jsonld), format="json-ld")
    assert set(from_turtle) == set(from_jsonld)
