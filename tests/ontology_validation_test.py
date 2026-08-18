import hashlib
from pathlib import Path

from owlrl import DeductiveClosure, OWLRL_Semantics
from rdflib import Graph, Literal, RDF, URIRef
from rdflib.collection import Collection
from rdflib.namespace import DCTERMS, OWL, XSD

from CM.linked_data import CAT


ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY = ROOT / "ontology" / "catmapper.ttl"
VERSIONS = ROOT / "ontology" / "versions"


def _ontology_graph():
    return Graph().parse(ONTOLOGY, format="turtle")


def test_ontology_release_metadata_and_signatures():
    graph = _ontology_graph()
    series = URIRef("https://catmapper.org/ontology/catmapper")
    version = URIRef("https://catmapper.org/ontology/catmapper/2026.08.18")
    prior = URIRef("https://catmapper.org/ontology/catmapper/2026.07.21")

    assert (series, RDF.type, OWL.Ontology) in graph
    assert (series, OWL.versionIRI, version) in graph
    assert (series, OWL.versionInfo, Literal("2026.08.18")) in graph
    assert (series, OWL.priorVersion, prior) in graph
    assert (series, DCTERMS.issued, Literal("2026-08-18", datatype=XSD.date)) in graph

    categories = [OWL.ObjectProperty, OWL.DatatypeProperty, OWL.AnnotationProperty]
    for term in set(graph.subjects(RDF.type, None)):
        signatures = [category for category in categories if (term, RDF.type, category) in graph]
        assert len(signatures) <= 1, f"{term} has incompatible property signatures: {signatures}"
    assert not list(graph.subjects(RDF.type, OWL.AllDisjointClasses))


def test_property_chain_inference_and_no_identity_inference():
    graph = _ontology_graph()
    dataset = URIRef("https://catmapper.org/sociomap/SD1")
    assertion = URIRef("https://catmapper.org/sociomap/assertion/uses/example")
    concept = URIRef("https://catmapper.org/sociomap/SM1")
    graph.add((dataset, RDF.type, CAT.Dataset))
    graph.add((assertion, RDF.type, CAT.DatasetAssertion))
    graph.add((dataset, CAT.hasDatasetAssertion, assertion))
    graph.add((assertion, CAT.assertionConcept, concept))

    DeductiveClosure(OWLRL_Semantics, axiomatic_triples=False).expand(graph)

    assert (dataset, CAT.usesConcept, concept) in graph
    assert not [target for target in graph.objects(concept, OWL.sameAs) if target != concept]
    assert not list(graph.triples((concept, OWL.equivalentClass, None)))
    assert not list(graph.subjects(RDF.type, OWL.Nothing))


def test_immutable_release_checksums():
    manifest = ROOT / "ontology" / "checksums.sha256"
    for line in manifest.read_text(encoding="utf-8").splitlines():
        expected, relative = line.split(None, 1)
        path = ROOT / "ontology" / relative.strip()
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected


def test_context_compact_expand_round_trip():
    context_path = ROOT / "ontology" / "context.jsonld"
    source = {
        "@context": __import__("json").loads(context_path.read_text(encoding="utf-8"))["@context"],
        "@id": "https://catmapper.org/sociomap/SM1",
        "@type": "cat:Concept",
        "cmid": "SM1",
        "inDatabase": "cat:SocioMap",
        "name": {"en": "Aymara"},
    }
    first = Graph().parse(data=__import__("json").dumps(source), format="json-ld")
    compacted = first.serialize(format="json-ld", context=source["@context"], auto_compact=True)
    second = Graph().parse(data=compacted, format="json-ld")
    assert set(first) == set(second)
