"""Shared Neo4j-to-RDF projection for CatMapper public linked data."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import quote

from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef
from rdflib.namespace import DCAT, DCTERMS, OWL, PROV, SKOS, XSD

from .utils import getDriver, getQuery


ONTOLOGY_VERSION = "2026.08.18"
ONTOLOGY_IRI = "https://catmapper.org/ontology/catmapper"
CONTEXT_IRI = "https://catmapper.org/contexts/catmapper"
RESOURCE_BASE = "https://catmapper.org"
CAT = Namespace(f"{ONTOLOGY_IRI}#")
MAX_ASSERTIONS_PER_RESPONSE = 500
MAX_HIERARCHY_LINKS = 500

DATABASES = {
    "sociomap": CAT.SocioMap,
    "archamap": CAT.ArchaMap,
}

STRUCTURAL_LABELS = {
    "CATEGORY",
    "DATASET",
    "DELETED",
    "MERGING",
    "STACK",
    "VARIABLE",
}

CONTEXT_PROPERTIES = {
    "country": CAT.geographicContext,
    "district": CAT.geographicContext,
    "parent": CAT.contextConcept,
    "parentContext": CAT.parentContextConcept,
    "language": CAT.contextConcept,
    "religion": CAT.contextConcept,
    "occupation": CAT.contextConcept,
    "polity": CAT.contextConcept,
    "variable": CAT.variableContext,
    "period": CAT.contextConcept,
    "culture": CAT.contextConcept,
}

_SAFE_CATALOG_ID = re.compile(r"^[A-Za-z0-9._~-]+$")
_INTEGER = re.compile(r"^[+-]?[0-9]+$")


NODE_QUERY = """
MATCH (n {CMID: $cmid})
WHERE n:CATEGORY OR n:DATASET OR n:DELETED
OPTIONAL MATCH (n:DELETED)-[:IS]->(replacement)
RETURN labels(n) AS labels,
       n.CMID AS cmid,
       n.CMName AS name,
       n.names AS names,
       n.description AS description,
       n.DatasetCitation AS datasetCitation,
       n.DatasetLocation AS datasetLocation,
       n.DatasetScope AS datasetScope,
       n.DatasetVersion AS datasetVersion,
       n.ApplicableYears AS applicableYears,
       n.Note AS note,
       n.shortName AS shortName,
       n.yearPublished AS yearPublished,
       replacement.CMID AS replacementCmid
LIMIT 2
"""

ASSERTION_QUERY = """
MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
WHERE d.CMID = $cmid OR c.CMID = $cmid
RETURN d.CMID AS datasetCmid,
       c.CMID AS conceptCmid,
       r.Key AS key,
       r.Name AS sourceName,
       properties(r)['categoryType'] AS categoryType,
       properties(r)['comment'] AS comment,
       properties(r)['descriptor'] AS descriptor,
       properties(r)['url'] AS sourceUrl,
       properties(r)['recordStart'] AS recordStart,
       properties(r)['recordEnd'] AS recordEnd,
       properties(r)['yearStart'] AS yearStart,
       properties(r)['yearEnd'] AS yearEnd,
       properties(r)['country'] AS country,
       properties(r)['district'] AS district,
       properties(r)['parent'] AS parent,
       properties(r)['parentContext'] AS parentContext,
       properties(r)['language'] AS language,
       properties(r)['religion'] AS religion,
       properties(r)['occupation'] AS occupation,
       properties(r)['polity'] AS polity,
       properties(r)['variable'] AS variable,
       properties(r)['period'] AS period,
       properties(r)['culture'] AS culture,
       r.logID AS stableDiscriminator
ORDER BY datasetCmid, key, conceptCmid,
         coalesce(apoc.convert.toJson(r.logID), '')
SKIP $offset
LIMIT $limit
"""

COLLISION_QUERY = """
MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
WHERE d.CMID = $cmid OR c.CMID = $cmid
WITH d.CMID AS datasetCmid, r.Key AS key, c.CMID AS conceptCmid, count(r) AS multiplicity
WHERE multiplicity > 1
RETURN datasetCmid, key, conceptCmid, multiplicity
"""

HIERARCHY_QUERY = """
MATCH (n:CATEGORY {CMID: $cmid})
OPTIONAL MATCH (n)-[:CONTAINS]->(child:CATEGORY)
WITH n, collect({direction: 'out', otherCmid: child.CMID,
                 sourceLabels: labels(n), targetLabels: labels(child)}) AS outgoing
OPTIONAL MATCH (parent:CATEGORY)-[:CONTAINS]->(n)
WITH outgoing, collect({direction: 'in', otherCmid: parent.CMID,
                        sourceLabels: labels(parent), targetLabels: labels(n)}) AS incoming
RETURN outgoing + incoming AS links
"""

DOMAIN_SCHEME_QUERY = """
MATCH (label:LABEL)
WHERE toLower(label.CMName) = $slug
RETURN label.CMName AS label, label.groupLabel AS groupLabel
LIMIT 2
"""

PUBLIC_RESOURCE_PAGE_QUERY = """
MATCH (n)
WHERE (n:CATEGORY OR n:DATASET OR n:DELETED)
  AND n.CMID IS NOT NULL
  AND n.CMID > $after
RETURN n.CMID AS cmid, labels(n) AS labels
ORDER BY n.CMID
LIMIT $limit
"""


class LinkedDataError(Exception):
    """Base class for safe linked-data projection failures."""


class ResourceNotFound(LinkedDataError):
    pass


class MalformedPublicResource(LinkedDataError):
    pass


def normalize_database(database: str) -> str:
    normalized = str(database or "").strip().lower()
    if normalized not in DATABASES:
        raise ValueError("Invalid database. Use 'sociomap' or 'archamap'.")
    return normalized


def canonical_resource_iri(database: str, cmid: str) -> URIRef:
    database = normalize_database(database)
    cmid = str(cmid or "").strip()
    if not cmid or not _SAFE_CATALOG_ID.fullmatch(cmid):
        raise MalformedPublicResource("Resource has an invalid public CMID.")
    return URIRef(f"{RESOURCE_BASE}/{database}/{cmid}")


def domain_scheme_iri(database: str, label: str) -> URIRef:
    database = normalize_database(database)
    slug = quote(str(label).strip().lower(), safe="-._~")
    return URIRef(f"{RESOURCE_BASE}/{database}/scheme/{slug}")


def _as_values(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = [value]
    result = []
    seen = set()
    for item in values:
        text = str(item).strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _add_text(graph, subject, predicate, value, *, lang=None):
    for text in _as_values(value):
        graph.add((subject, predicate, Literal(text, lang=lang)))


def _add_integer(graph, subject, predicate, value):
    values = _as_values(value)
    if len(values) != 1 or not _INTEGER.fullmatch(values[0]):
        return
    graph.add((subject, predicate, Literal(int(values[0]), datatype=XSD.integer)))


def _bind_namespaces(graph: Graph) -> None:
    graph.bind("cat", CAT)
    graph.bind("dcat", DCAT)
    graph.bind("dcterms", DCTERMS)
    graph.bind("owl", OWL)
    graph.bind("prov", PROV)
    graph.bind("skos", SKOS)
    graph.bind("xsd", XSD)


def _identity_tuple(assertion):
    return (
        str(assertion.get("database") or ""),
        str(assertion.get("datasetCmid") or ""),
        str(assertion.get("key") or ""),
        str(assertion.get("conceptCmid") or ""),
    )


def assertion_iri(assertion, multiplicity=1) -> URIRef:
    parts = _identity_tuple(assertion)
    if any(not part.strip() for part in parts):
        raise MalformedPublicResource("USES assertion is missing dataset, key, or concept identity.")
    base_digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()
    suffix = ""
    if int(multiplicity or 1) > 1:
        discriminator = assertion.get("stableDiscriminator")
        if discriminator in (None, "", []):
            raise MalformedPublicResource(
                "Duplicate USES assertion lacks a stable public identity discriminator."
            )
        encoded = json.dumps(discriminator, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        suffix = "-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]
    return URIRef(f"{RESOURCE_BASE}/{parts[0]}/assertion/uses/{base_digest}{suffix}")


def _semantic_contains_allowed(database, source_labels, target_labels):
    source = set(source_labels or [])
    target = set(target_labels or [])
    if database == "sociomap":
        if "ETHNICITY" in source and "ETHNICITY" in target:
            return True
        ranks = ["FAMILY", "LANGUAGE", "DIALECT"]
    else:
        if "PERIOD" in source and "PERIOD" in target:
            return True
        ranks = ["FAMILY", "GENUS", "SPECIES", "SUBSPECIES"]
    source_rank = next((i for i, rank in enumerate(ranks) if rank in source), None)
    target_rank = next((i for i, rank in enumerate(ranks) if rank in target), None)
    return source_rank is not None and target_rank is not None and source_rank < target_rank


def _add_assertion(graph, database, row, multiplicities):
    row = dict(row)
    row["database"] = database
    identity = (
        str(row.get("datasetCmid") or ""),
        str(row.get("key") or ""),
        str(row.get("conceptCmid") or ""),
    )
    assertion = assertion_iri(row, multiplicities.get(identity, 1))
    dataset = canonical_resource_iri(database, row.get("datasetCmid"))
    concept = canonical_resource_iri(database, row.get("conceptCmid"))

    graph.add((assertion, RDF.type, CAT.DatasetAssertion))
    graph.add((assertion, CAT.assertionDataset, dataset))
    graph.add((assertion, CAT.assertionConcept, concept))
    graph.add((dataset, CAT.hasDatasetAssertion, assertion))
    graph.add((assertion, CAT.key, Literal(str(row["key"]))))
    graph.add((assertion, CAT.projectionOntologyVersion, Literal(ONTOLOGY_VERSION)))

    _add_text(graph, assertion, CAT.sourceName, row.get("sourceName"))
    _add_text(graph, assertion, CAT.categoryType, row.get("categoryType"))
    _add_text(graph, assertion, CAT.comment, row.get("comment"))
    _add_text(graph, assertion, CAT.descriptor, row.get("descriptor"))
    _add_text(graph, assertion, CAT.sourceUrl, row.get("sourceUrl"))
    _add_text(graph, assertion, CAT.recordStart, row.get("recordStart"))
    _add_text(graph, assertion, CAT.recordEnd, row.get("recordEnd"))
    _add_integer(graph, assertion, CAT.yearStart, row.get("yearStart"))
    _add_integer(graph, assertion, CAT.yearEnd, row.get("yearEnd"))

    for field, predicate in CONTEXT_PROPERTIES.items():
        for context_cmid in _as_values(row.get(field)):
            try:
                context_iri = canonical_resource_iri(database, context_cmid)
            except MalformedPublicResource:
                continue
            graph.add((assertion, predicate, context_iri))


def project_assertion(database, row, multiplicity=1):
    """Project exactly one allowlisted USES relationship into a standalone graph."""
    database = normalize_database(database)
    graph = Graph()
    _bind_namespaces(graph)
    identity = (
        str(row.get("datasetCmid") or ""),
        str(row.get("key") or ""),
        str(row.get("conceptCmid") or ""),
    )
    _add_assertion(graph, database, row, {identity: int(multiplicity or 1)})
    return graph


def project_hierarchy_link(database, row):
    """Project one approved broader-to-narrower CONTAINS relationship."""
    database = normalize_database(database)
    graph = Graph()
    _bind_namespaces(graph)
    if not _semantic_contains_allowed(database, row.get("sourceLabels"), row.get("targetLabels")):
        return graph
    source = canonical_resource_iri(database, row.get("sourceCmid"))
    target = canonical_resource_iri(database, row.get("targetCmid"))
    graph.add((source, CAT.containsConcept, target))
    return graph


def project_resource(database, record, assertions=None, multiplicities=None, hierarchy=None):
    """Build one format-neutral RDF graph from an allowlisted source record."""
    database = normalize_database(database)
    record = dict(record or {})
    labels = set(record.get("labels") or [])
    cmid = record.get("cmid")
    subject = canonical_resource_iri(database, cmid)
    graph = Graph()
    _bind_namespaces(graph)

    if "DELETED" in labels:
        graph.add((subject, RDF.type, CAT.DeprecatedResource))
    else:
        if "CATEGORY" in labels:
            graph.add((subject, RDF.type, CAT.Concept))
            graph.add((subject, RDF.type, SKOS.Concept))
        if "DATASET" in labels:
            graph.add((subject, RDF.type, CAT.Dataset))
            graph.add((subject, RDF.type, DCAT.Dataset))
        if "VARIABLE" in labels:
            graph.add((subject, RDF.type, CAT.Variable))
        if "STACK" in labels:
            graph.add((subject, RDF.type, CAT.Stack))

    if not any(label in labels for label in ("CATEGORY", "DATASET", "DELETED")):
        raise MalformedPublicResource("Resource is not eligible for public RDF publication.")

    graph.add((subject, CAT.cmid, Literal(str(cmid))))
    graph.add((subject, CAT.inDatabase, DATABASES[database]))
    graph.add((subject, CAT.projectionOntologyVersion, Literal(ONTOLOGY_VERSION)))
    _add_text(graph, subject, SKOS.prefLabel, record.get("name"), lang="en")
    for name in _as_values(record.get("names")):
        if name != str(record.get("name") or "").strip():
            graph.add((subject, SKOS.altLabel, Literal(name)))
    _add_text(graph, subject, DCTERMS.description, record.get("description") or record.get("note"))

    if "DATASET" in labels:
        _add_text(graph, subject, DCTERMS.bibliographicCitation, record.get("datasetCitation"))
        _add_text(graph, subject, DCTERMS.source, record.get("datasetLocation"))
        _add_text(graph, subject, DCTERMS.extent, record.get("datasetScope"))
        _add_text(graph, subject, DCTERMS.hasVersion, record.get("datasetVersion"))
        _add_text(graph, subject, DCTERMS.temporal, record.get("applicableYears"))
        _add_text(graph, subject, SKOS.altLabel, record.get("shortName"))
        year = _as_values(record.get("yearPublished"))
        if len(year) == 1 and re.fullmatch(r"[0-9]{4}", year[0]):
            graph.add((subject, DCTERMS.issued, Literal(year[0], datatype=XSD.gYear)))

    if "CATEGORY" in labels:
        for label in sorted(labels - STRUCTURAL_LABELS):
            scheme = domain_scheme_iri(database, label)
            graph.add((subject, CAT.inDomainScheme, scheme))
            graph.add((scheme, RDF.type, CAT.DomainScheme))
            graph.add((scheme, RDF.type, SKOS.ConceptScheme))
            graph.add((scheme, RDFS.label, Literal(label, lang="en")))

    replacement = record.get("replacementCmid")
    if "DELETED" in labels and replacement:
        replacement_iri = canonical_resource_iri(database, replacement)
        graph.add((subject, CAT.replacedBy, replacement_iri))
        graph.add((subject, DCTERMS.isReplacedBy, replacement_iri))

    multiplicities = multiplicities or {}
    for assertion in assertions or []:
        _add_assertion(graph, database, assertion, multiplicities)

    for link in hierarchy or []:
        source_labels = link.get("sourceLabels") or []
        target_labels = link.get("targetLabels") or []
        other_cmid = link.get("otherCmid")
        if not other_cmid or not _semantic_contains_allowed(database, source_labels, target_labels):
            continue
        other = canonical_resource_iri(database, other_cmid)
        if link.get("direction") == "out":
            graph.add((subject, CAT.containsConcept, other))
        else:
            graph.add((other, CAT.containsConcept, subject))

    return graph


def _collision_map(rows):
    return {
        (str(row.get("datasetCmid") or ""), str(row.get("key") or ""), str(row.get("conceptCmid") or "")):
            int(row.get("multiplicity") or 1)
        for row in rows or []
    }


def fetch_resource_projection(
    database,
    cmid,
    *,
    assertion_offset=0,
    assertion_limit=100,
    include_assertions=True,
):
    database = normalize_database(database)
    limit = max(0, min(int(assertion_limit), MAX_ASSERTIONS_PER_RESPONSE))
    offset = max(0, int(assertion_offset))
    driver = getDriver(database)
    rows = getQuery(NODE_QUERY, driver=driver, params={"cmid": cmid}, type="dict")
    if not rows:
        raise ResourceNotFound("Public CatMapper resource not found.")
    if len(rows) > 1:
        raise MalformedPublicResource("CMID is not unique in the selected database.")

    assertion_rows = getQuery(
        ASSERTION_QUERY,
        driver=driver,
        params={"cmid": cmid, "offset": offset, "limit": limit + 1},
        type="dict",
    ) if limit and include_assertions else []
    has_more = len(assertion_rows) > limit
    assertion_rows = assertion_rows[:limit]
    collisions = getQuery(
        COLLISION_QUERY,
        driver=driver,
        params={"cmid": cmid},
        type="dict",
    ) if assertion_rows else []

    hierarchy_rows = getQuery(
        HIERARCHY_QUERY,
        driver=driver,
        params={"cmid": cmid},
        type="dict",
    )
    hierarchy = (hierarchy_rows[0].get("links") or [])[:MAX_HIERARCHY_LINKS] if hierarchy_rows else []
    hierarchy = [link for link in hierarchy if link and link.get("otherCmid")]

    graph = project_resource(
        database,
        rows[0],
        assertion_rows,
        _collision_map(collisions),
        hierarchy,
    )
    return {
        "graph": graph,
        "record": rows[0],
        "assertionOffset": offset,
        "assertionCount": len(assertion_rows),
        "hasMoreAssertions": has_more,
    }


def iter_public_resource_records(database, *, page_size=1000):
    database = normalize_database(database)
    driver = getDriver(database)
    after = ""
    while True:
        rows = getQuery(
            PUBLIC_RESOURCE_PAGE_QUERY,
            driver=driver,
            params={"after": after, "limit": int(page_size)},
            type="dict",
        )
        if not rows:
            return
        for row in rows:
            yield row
        after = str(rows[-1].get("cmid") or "")


def fetch_domain_scheme_projection(database, slug):
    database = normalize_database(database)
    slug = str(slug or "").strip().lower()
    if not slug or not re.fullmatch(r"[a-z0-9._~-]+", slug):
        raise ResourceNotFound("Public CatMapper domain scheme not found.")
    driver = getDriver(database)
    rows = getQuery(
        DOMAIN_SCHEME_QUERY,
        driver=driver,
        params={"slug": slug},
        type="dict",
    )
    if not rows:
        raise ResourceNotFound("Public CatMapper domain scheme not found.")
    if len(rows) > 1:
        raise MalformedPublicResource("Domain scheme identifier is ambiguous.")

    label = str(rows[0].get("label") or "").strip()
    subject = domain_scheme_iri(database, label)
    graph = Graph()
    _bind_namespaces(graph)
    graph.add((subject, RDF.type, CAT.DomainScheme))
    graph.add((subject, RDF.type, SKOS.ConceptScheme))
    graph.add((subject, CAT.inDatabase, DATABASES[database]))
    graph.add((subject, CAT.projectionOntologyVersion, Literal(ONTOLOGY_VERSION)))
    graph.add((subject, RDFS.label, Literal(label, lang="en")))
    group_label = str(rows[0].get("groupLabel") or "").strip()
    if group_label and group_label != label:
        graph.add((subject, DCTERMS.isPartOf, domain_scheme_iri(database, group_label)))
    return graph


def load_jsonld_context():
    candidates = []
    configured = Path(str(__import__("os").environ.get("CATMAPPER_ONTOLOGY_DIR", "")))
    if str(configured):
        candidates.append(configured / "context.jsonld")
    candidates.append(Path(__file__).resolve().parents[2] / "ontology" / "context.jsonld")
    for path in candidates:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))["@context"]
    raise FileNotFoundError("CatMapper JSON-LD context is not installed.")


def serialize_graph(graph: Graph, media_type: str) -> str:
    if media_type == "text/turtle":
        return graph.serialize(format="turtle")
    if media_type == "application/ld+json":
        compacted = graph.serialize(
            format="json-ld",
            context=load_jsonld_context(),
            auto_compact=True,
            indent=2,
        )
        payload = json.loads(compacted)
        payload["@context"] = CONTEXT_IRI
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    raise ValueError("Unsupported RDF media type.")
