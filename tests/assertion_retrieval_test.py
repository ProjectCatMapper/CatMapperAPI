import json

import pytest
from rdflib import Graph, RDF, URIRef

from CM import linked_data as ld
from CM.assertion_index import AssertionIndexUnavailable, lookup_assertion, remember_assertions


def row(**changes):
    return dict(datasetCmid="SD1", conceptCmid="SM1", key="code", sourceName=["Example"], **changes)


def seed(monkeypatch, rows, database="sociomap"):
    remember_assertions(database, rows)
    monkeypatch.setattr(ld, "getDriver", lambda database: database)
    calls = []

    def query(query, *, driver, params, type):
        assert query == ld.ASSERTION_LOOKUP_QUERY
        assert params == {"datasetCmid": "SD1", "conceptCmid": "SM1", "key": "code"}
        calls.append(params)
        return rows

    monkeypatch.setattr(ld, "getQuery", query)
    return calls


def identifier(source, database="sociomap", multiplicity=1):
    return str(ld.assertion_iri({**source, "database": database}, multiplicity)).rsplit("/", 1)[1]


def test_persistent_index_separates_databases_and_rejects_missing_storage():
    with pytest.raises(AssertionIndexUnavailable):
        lookup_assertion("sociomap", "a" * 64)
    remember_assertions("sociomap", [row(), row()])
    assert lookup_assertion("sociomap", identifier(row())) == {"datasetCmid": "SD1", "conceptCmid": "SM1", "key": "code"}
    assert lookup_assertion("archamap", identifier(row())) is None


@pytest.mark.parametrize("database", ["sociomap", "archamap"])
def test_assertion_uri_projects_live_values_and_negotiates(client, monkeypatch, database):
    source = row()
    calls = seed(monkeypatch, [source], database)
    digest = identifier(source, database)
    path = f"/linked-data/{database}/assertion/uses/{digest}"
    uri = URIRef(f"https://catmapper.org/{database}/assertion/uses/{digest}")
    for headers, suffix in [({}, ""), ({"Accept": "*/*"}, ""), ({"Accept": "text/turtle"}, ""), ({}, "?format=ttl")]:
        response = client.get(path + suffix, headers=headers)
        assert response.status_code == 200
        assert response.mimetype == "text/turtle"
        graph = Graph().parse(data=response.text, format="turtle")
        assert (uri, RDF.type, ld.CAT.DatasetAssertion) in graph
        assert response.headers["Vary"] == "Accept"
        assert str(uri) in response.headers["Link"]
        assert "max-age=300" in response.headers["Cache-Control"]
    response = client.get(path, headers={"Accept": "application/ld+json"})
    assert response.status_code == 200
    assert response.mimetype == "application/ld+json"
    assert client.get(path, headers={"Accept": "text/html"}).status_code == 406
    assert client.get(path, headers={"Accept": "application/rdf+xml"}).status_code == 406
    assert client.head(path).status_code == 200
    assert client.head(path).data == b""
    assert calls


def test_collision_uris_are_resolved_individually(client, monkeypatch):
    sources = [row(stableDiscriminator=["private-one"]), row(stableDiscriminator=["private-two"])]
    seed(monkeypatch, sources)
    for source in sources:
        digest = identifier(source, multiplicity=2)
        response = client.get(f"/linked-data/sociomap/assertion/uses/{digest}")
        assert response.status_code == 200
        graph = Graph().parse(data=response.text, format="turtle")
        assert len(set(graph.subjects(RDF.type, ld.CAT.DatasetAssertion))) == 1
        assert "private-one" not in response.text and "private-two" not in response.text
    assert client.get(f"/linked-data/sociomap/assertion/uses/{identifier(row())}").status_code == 404
    assert client.get(f"/linked-data/sociomap/assertion/uses/{identifier(row())}-{'f' * 16}").status_code == 404


def test_stale_missing_malformed_and_unavailable_are_not_html(client, monkeypatch):
    path = "/linked-data/sociomap/assertion/uses/"
    assert client.get(path + "a" * 64).status_code == 503
    remember_assertions("sociomap", [row()])
    assert client.get(path + "a" * 64).status_code == 404
    assert client.get(path + "malformed").status_code == 404
    assert client.get(path + "a" * 64 + "?format=invalid").status_code == 406
    seed(monkeypatch, [])
    assert client.get(path + identifier(row())).status_code == 404


def test_geometry_uri_returns_exact_geometry_and_live_assertion(client, monkeypatch):
    source = row(geoCoords=[json.dumps({"type": "Point", "coordinates": [1, 2]})])
    seed(monkeypatch, [source])
    graph = ld.project_assertion("sociomap", source)
    geometry = next(graph.subjects(RDF.type, ld.GEO.Geometry))
    path = str(geometry).replace("https://catmapper.org/", "/linked-data/")
    response = client.get(path, headers={"Accept": "text/turtle"})
    assert response.status_code == 200
    parsed = Graph().parse(data=response.text, format="turtle")
    assert (geometry, RDF.type, ld.GEO.Geometry) in parsed
    assert list(parsed.objects(geometry, ld.GEO.asGeoJSON))
    assert client.get(path.rsplit("/", 1)[0] + "/" + "f" * 24).status_code == 404


def test_category_retrieval_registers_published_assertion(monkeypatch):
    queries = []
    monkeypatch.setattr(ld, "getDriver", lambda database: database)

    def query(text, **kwargs):
        queries.append(text)
        if text == ld.NODE_QUERY:
            return [{"cmid": "SM1", "labels": ["CATEGORY"], "name": "Example"}]
        if text == ld.ASSERTION_QUERY:
            return [row()]
        return []

    monkeypatch.setattr(ld, "getQuery", query)
    ld.fetch_resource_projection("sociomap", "SM1")
    assert lookup_assertion("sociomap", identifier(row()))["key"] == "code"


def test_index_is_only_a_locator_and_does_not_serve_stale_content(client, monkeypatch):
    source = row()
    seed(monkeypatch, [source])
    path = "/linked-data/sociomap/assertion/uses/" + identifier(source)
    assert "Example" in client.get(path).text
    source["sourceName"] = ["Updated source label"]
    response = client.get(path)
    assert response.status_code == 200
    assert "Updated source label" in response.text
    assert "Example" not in response.text


def test_bulk_publication_registers_assertion_identity(monkeypatch):
    from CM import bulk_rdf
    monkeypatch.setattr(bulk_rdf, "getDriver", lambda database: database)
    monkeypatch.setattr(bulk_rdf, "getQuery", lambda *args, **kwargs: [])

    def stream(driver, query):
        return iter([row()]) if query == bulk_rdf.ASSERTION_EXPORT_QUERY else iter([])

    monkeypatch.setattr(bulk_rdf, "_stream_query", stream)
    assert list(bulk_rdf.iter_bulk_graphs("sociomap"))
    assert lookup_assertion("sociomap", identifier(row()))["datasetCmid"] == "SD1"


def test_only_successful_backfills_are_marked_complete():
    from CM.assertion_index import backfill_complete
    assert not backfill_complete("sociomap")
    remember_assertions("sociomap", [row()])
    assert not backfill_complete("sociomap")
    assert backfill_complete("sociomap", mark=True)
    assert backfill_complete("sociomap")
    assert not backfill_complete("archamap")


def test_empty_database_backfill_can_be_marked_complete():
    from CM.assertion_index import backfill_complete
    assert backfill_complete("sociomap", mark=True)
    assert lookup_assertion("sociomap", "f" * 64) is None
