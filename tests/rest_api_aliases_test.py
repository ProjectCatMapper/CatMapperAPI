import CMroutes.download_routes as download_routes
import CMroutes.explore_routes as explore_routes
import CMroutes.metadata_routes as metadata_routes


def test_rest_download_csv_urls_alias_matches_legacy(client, monkeypatch):
    """Canonical REST and legacy CSV URL routes return the same payload."""
    monkeypatch.setattr(
        download_routes,
        "get_backup_csv_urls",
        lambda database, mostRecent=True: [f"{database}:{mostRecent}"],
    )

    legacy = client.get("/CSVURLs/archamap?mostRecent=false")
    canonical = client.get("/api/databases/archamap/downloads/csv-urls?mostRecent=false")

    assert legacy.status_code == 200
    assert canonical.status_code == 200
    assert canonical.get_json() == legacy.get_json() == {"urls": ["archamap:False"]}


def test_rest_metadata_domains_alias_matches_legacy(client, monkeypatch):
    """Canonical REST and legacy domain metadata routes share implementation."""
    monkeypatch.setattr(
        metadata_routes,
        "get_public_domains",
        lambda database: [{"domain": "AREA", "database": database}],
    )

    legacy = client.get("/metadata/domains/SocioMap")
    canonical = client.get("/api/databases/SocioMap/metadata/domains")

    assert legacy.status_code == 200
    assert canonical.status_code == 200
    assert canonical.get_json() == legacy.get_json()


def test_rest_node_details_alias_matches_legacy(client, monkeypatch):
    """Canonical REST and legacy CMID node detail routes return equivalent data."""
    monkeypatch.setattr(explore_routes, "getDriver", lambda database: object())

    def fake_get_query(query, driver=None, cmid=None, **kwargs):
        if "nodeProperties" in query:
            return [{"nodeID": "node-1", "nodeProperties": "CMID", "nodeValues": cmid}]
        return [{"relID": "rel-1", "relProperties": "Key", "relValues": "Name == Example"}]

    monkeypatch.setattr(explore_routes, "getQuery", fake_get_query)

    legacy = client.get("/CMID/archamap/AM1")
    canonical = client.get("/api/databases/archamap/nodes/AM1")

    assert legacy.status_code == 200
    assert canonical.status_code == 200
    assert canonical.get_json() == legacy.get_json()


def test_api_prefixed_legacy_alias_is_registered(client, monkeypatch):
    """The /api prefix also works for legacy paths during migration."""
    monkeypatch.setattr(
        metadata_routes,
        "get_public_subdomains",
        lambda database: [{"domain": "AREA", "subdomain": "NATURAL"}],
    )

    response = client.get("/api/metadata/subdomains/ArchaMap")

    assert response.status_code == 200
    assert response.get_json() == [{"domain": "AREA", "subdomain": "NATURAL"}]
