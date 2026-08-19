import json
from pathlib import Path

from rdflib import Graph

import CMroutes.linked_data_routes as routes
from CM import linked_data


ONTOLOGY_DIR = Path(__file__).resolve().parents[2] / "ontology"


def _projection(*, deleted=False, replacement=True, has_more=False):
    record = {
        "labels": ["DELETED"] if deleted else ["CATEGORY", "ETHNICITY"],
        "cmid": "SM1",
        "name": "Aymara",
        "replacementCmid": "SM2" if deleted and replacement else None,
    }
    return {
        "graph": linked_data.project_resource("sociomap", record),
        "record": record,
        "assertionOffset": 0,
        "assertionCount": 1,
        "hasMoreAssertions": has_more,
    }


def test_ontology_and_context_routes_are_parseable(client, monkeypatch):
    monkeypatch.setenv("CATMAPPER_ONTOLOGY_DIR", str(ONTOLOGY_DIR))

    stable = client.get("/ontology/catmapper")
    dated = client.get("/ontology/catmapper/2026.08.18")
    context = client.get("/contexts/catmapper")
    unknown = client.get("/ontology/catmapper/1900.01.01")

    assert stable.status_code == dated.status_code == context.status_code == 200
    assert stable.mimetype == dated.mimetype == "text/turtle"
    assert context.mimetype == "application/ld+json"
    assert "immutable" in dated.headers["Cache-Control"]
    assert unknown.status_code == 404
    Graph().parse(data=stable.get_data(as_text=True), format="turtle")
    assert "@context" in context.get_json()


def test_entity_content_negotiation_headers_and_telemetry(client, monkeypatch, caplog):
    monkeypatch.setattr(routes, "fetch_resource_projection", lambda *args, **kwargs: _projection(has_more=True))
    routes._LOGGER.addHandler(caplog.handler)

    try:
        turtle = client.get("/linked-data/sociomap/SM1", headers={"Accept": "text/turtle"})
        jsonld = client.get("/linked-data/sociomap/SM1", headers={"Accept": "application/ld+json"})
        unsupported = client.get("/linked-data/sociomap/SM1", headers={"Accept": "application/rdf+xml"})
    finally:
        routes._LOGGER.removeHandler(caplog.handler)

    assert turtle.status_code == jsonld.status_code == 200
    assert turtle.mimetype == "text/turtle"
    assert jsonld.mimetype == "application/ld+json"
    assert turtle.headers["Vary"] == "Accept"
    assert "max-age=300" in turtle.headers["Cache-Control"]
    assert "max-age=300" in jsonld.headers["Cache-Control"]
    assert 'rel="canonical"' in turtle.headers.getlist("Link")[0]
    next_link = next(value for value in turtle.headers.getlist("Link") if 'rel="next"' in value)
    assert "/sociomap/SM1?assertion_offset=1&assertion_limit=100" in next_link
    assert "/linked-data/" not in next_link
    assert unsupported.status_code == 406
    assert b"https://catmapper.org/sociomap/SM1" in turtle.data
    assert any(
        "linked_data_response database=sociomap cmid=SM1 status=200 media_type=text/turtle"
        in record.getMessage()
        for record in caplog.records
    )


def test_explicit_format_and_deleted_tombstone_behavior(client, monkeypatch):
    monkeypatch.setattr(routes, "fetch_resource_projection", lambda *args, **kwargs: _projection(deleted=True, replacement=False))
    tombstone = client.get("/linked-data/sociomap/SM1?format=ttl")
    assert tombstone.status_code == 410
    assert b"DeprecatedResource" in tombstone.data

    monkeypatch.setattr(routes, "fetch_resource_projection", lambda *args, **kwargs: _projection(deleted=True, replacement=True))
    replacement = client.get("/linked-data/sociomap/SM1?format=jsonld")
    assert replacement.status_code == 200
    assert b"replacedBy" in replacement.data


def test_safe_errors_do_not_expose_internal_exception(client, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("secret database failure detail")

    monkeypatch.setattr(routes, "fetch_resource_projection", fail)
    response = client.get("/linked-data/sociomap/SM1", headers={"Accept": "text/turtle"})
    assert response.status_code == 500
    assert b"secret database" not in response.data


def test_domain_scheme_identifier_is_machine_readable(client, monkeypatch):
    graph = Graph().parse(
        data="""
        @prefix cat: <https://catmapper.org/ontology/catmapper#> .
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        <https://catmapper.org/sociomap/scheme/ethnicity>
            a cat:DomainScheme, skos:ConceptScheme .
        """,
        format="turtle",
    )
    monkeypatch.setattr(routes, "fetch_domain_scheme_projection", lambda *args, **kwargs: graph)
    response = client.get(
        "/linked-data/sociomap/scheme/ethnicity",
        headers={"Accept": "text/turtle"},
    )
    assert response.status_code == 200
    assert response.mimetype == "text/turtle"
    assert b"https://catmapper.org/sociomap/scheme/ethnicity" in response.data
