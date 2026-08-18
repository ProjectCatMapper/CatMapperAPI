"""Atomic streaming snapshot generation for CatMapper public RDF."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from rdflib import Graph

from .linked_data import (
    ONTOLOGY_VERSION,
    normalize_database,
    project_assertion,
    project_hierarchy_link,
    project_resource,
)
from .utils import getDriver, getQuery


LICENSE = "https://creativecommons.org/licenses/by/4.0/"
VALIDATION_BATCH_TRIPLES = 50_000

NODE_EXPORT_QUERY = """
MATCH (n)
WHERE (n:CATEGORY OR n:DATASET OR n:DELETED) AND n.CMID IS NOT NULL
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
"""

ASSERTION_EXPORT_QUERY = """
MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
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
"""

COLLISION_EXPORT_QUERY = """
MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
WITH d.CMID AS datasetCmid, r.Key AS key, c.CMID AS conceptCmid, count(r) AS multiplicity
WHERE multiplicity > 1
RETURN datasetCmid, key, conceptCmid, multiplicity
"""

HIERARCHY_EXPORT_QUERY = """
MATCH (source:CATEGORY)-[:CONTAINS]->(target:CATEGORY)
RETURN source.CMID AS sourceCmid,
       target.CMID AS targetCmid,
       labels(source) AS sourceLabels,
       labels(target) AS targetLabels
"""


def _stream_query(driver, query):
    """Yield records without materializing a database-wide result set."""
    with driver.session() as session:
        for record in session.run(query):
            yield dict(record)


def iter_bulk_graphs(database):
    """Stream independently serializable graph fragments for a full snapshot."""
    database = normalize_database(database)
    driver = getDriver(database)
    collisions = {
        (
            str(row.get("datasetCmid") or ""),
            str(row.get("key") or ""),
            str(row.get("conceptCmid") or ""),
        ): int(row.get("multiplicity") or 1)
        for row in getQuery(COLLISION_EXPORT_QUERY, driver=driver, type="dict")
    }

    for row in _stream_query(driver, NODE_EXPORT_QUERY):
        labels = set(row.get("labels") or [])
        yield "resource", labels, project_resource(database, row)

    for row in _stream_query(driver, ASSERTION_EXPORT_QUERY):
        identity = (
            str(row.get("datasetCmid") or ""),
            str(row.get("key") or ""),
            str(row.get("conceptCmid") or ""),
        )
        yield "assertion", set(), project_assertion(database, row, collisions.get(identity, 1))

    for row in _stream_query(driver, HIERARCHY_EXPORT_QUERY):
        graph = project_hierarchy_link(database, row)
        if len(graph):
            yield "hierarchy", set(), graph


def _atomic_json(path: Path, payload):
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fchmod(stream.fileno(), 0o644)
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _write_validated_batch(graph, compressed):
    if not len(graph):
        return 0
    serialized = graph.serialize(format="nt")
    parsed = Graph().parse(data=serialized, format="nt")
    if len(parsed) != len(graph):
        raise ValueError("RDF batch changed during N-Triples round-trip validation.")
    compressed.write(serialized.encode("utf-8"))
    return len(graph)


def generate_snapshot(
    database,
    output_directory,
    *,
    source_version=None,
    graph_iterator=iter_bulk_graphs,
):
    database = normalize_database(database)
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    generated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    generated_slug = f"{generated[:10]}T{generated[11:19].replace(':', '')}Z"
    stem = f"catmapper-{database}-{generated_slug}"
    snapshot_path = output_directory / f"{stem}.nt.gz"
    manifest_path = output_directory / f"{stem}.manifest.json"
    temporary = tempfile.NamedTemporaryFile(
        prefix=f".{snapshot_path.name}.",
        dir=output_directory,
        delete=False,
    )
    temporary_path = Path(temporary.name)
    temporary.close()

    counts = Counter()
    triple_count = 0
    batch = Graph()
    seen_scheme_triples = set()
    try:
        with temporary_path.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
                for kind, labels, graph in graph_iterator(database):
                    if kind == "resource":
                        counts["resources"] += 1
                        if "CATEGORY" in labels:
                            counts["categories"] += 1
                        if "DATASET" in labels:
                            counts["datasets"] += 1
                        if "DELETED" in labels:
                            counts["deleted"] += 1
                    elif kind == "assertion":
                        counts["datasetAssertions"] += 1
                    elif kind == "hierarchy":
                        counts["hierarchyLinks"] += 1

                    for triple in graph:
                        if "/scheme/" in str(triple[0]):
                            if triple in seen_scheme_triples:
                                continue
                            seen_scheme_triples.add(triple)
                        batch.add(triple)
                    if len(batch) >= VALIDATION_BATCH_TRIPLES:
                        triple_count += _write_validated_batch(batch, compressed)
                        batch = Graph()
                triple_count += _write_validated_batch(batch, compressed)
                compressed.flush()
            raw.flush()
            os.fsync(raw.fileno())
        os.chmod(temporary_path, 0o644)
        os.replace(temporary_path, snapshot_path)
    except BaseException:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise

    checksum = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
    manifest = {
        "database": database,
        "generatedAt": generated,
        "ontologyVersion": ONTOLOGY_VERSION,
        "sourceVersion": source_version or "unspecified-read-only-source",
        "included": ["CATEGORY", "DATASET", "DELETED", "USES", "approved CONTAINS"],
        "excluded": ["private fields", "ownership", "vectors", "raw logs", "unapproved mappings"],
        "format": "application/n-triples+gzip",
        "file": snapshot_path.name,
        "tripleCount": triple_count,
        "resourceCounts": dict(sorted(counts.items())),
        "sha256": checksum,
        "license": LICENSE,
        "generator": f"CatMapperAPI linked-data projection {ONTOLOGY_VERSION}",
    }
    _atomic_json(manifest_path, manifest)
    return snapshot_path, manifest_path, manifest
