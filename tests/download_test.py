import CMroutes.download_routes as download_routes
from CM import download as download_module
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd


def test_csv_urls_endpoint_returns_urls(client, monkeypatch):
    monkeypatch.setattr(
        download_routes,
        "get_backup_csv_urls",
        lambda database, mostRecent=True: [f"https://example.com/{database}/{mostRecent}"],
    )

    response = client.get("/CSVURLs/ArchaMap?mostRecent=false")

    assert response.status_code == 200
    assert response.get_json()["urls"] == ["https://example.com/ArchaMap/False"]


def test_advanced_download_requires_properties(client):
    response = client.post("/download/advanced/ArchaMap", json={"CMIDs": ["AM1"], "domain": "SITE"})

    assert response.status_code == 400
    assert response.get_json()["error"] == "Properties must be provided"


def test_advanced_download_returns_data(client, monkeypatch):
    monkeypatch.setattr(
        download_routes,
        "getAdvancedDownload",
        lambda database, domain, properties, cmids: [{"database": database, "domain": domain, "count": len(cmids)}],
    )

    response = client.post(
        "/download/advanced/ArchaMap",
        json={"CMIDs": ["AM1", "AM2"], "domain": "SITE", "properties": ["Name"]},
    )

    assert response.status_code == 200
    assert response.get_json()["data"][0]["count"] == 2


def test_advanced_download_requires_cmids(client):
    response = client.post(
        "/download/advanced/ArchaMap",
        json={"CMIDs": [], "domain": "SITE", "properties": ["Name"]},
    )

    assert response.status_code == 400
    assert response.get_json()["error"] == "No CMIDs were provided for the advanced download request."


def test_advanced_download_returns_explicit_no_results_error(client, monkeypatch):
    def fake_get_advanced_download(database, domain, properties, cmids):
        raise LookupError("No downloadable records were found for the requested CMIDs: AM404")

    monkeypatch.setattr(download_routes, "getAdvancedDownload", fake_get_advanced_download)

    response = client.post(
        "/download/advanced/ArchaMap",
        json={"CMIDs": ["AM404"], "domain": "SITE", "properties": ["Name"]},
    )

    assert response.status_code == 400
    assert response.get_json()["error"] == "No downloadable records were found for the requested CMIDs: AM404"


def test_get_backup_csv_urls_falls_back_to_local_files(monkeypatch, tmp_path):
    client_calls = []
    local_dir = tmp_path / "download"
    local_dir.mkdir()
    file_path = local_dir / "metadata_2026-03-24.csv"
    file_path.write_bytes(b"a" * 1048576)

    class FakePaginator:
        def paginate(self, **kwargs):
            if len(client_calls) == 1:
                raise NoCredentialsError()
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
                "ListObjectsV2",
            )

    class FakeClient:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

    def fake_boto_client(service_name, **kwargs):
        assert service_name == "s3"
        client_calls.append("config" in kwargs)
        return FakeClient()

    monkeypatch.setattr(download_module.boto3, "client", fake_boto_client)
    monkeypatch.setitem(
        download_module.BACKUP_SOURCE_MAP,
        "ArchaMap",
        {"s3_prefix": "archamap-backups/download", "local_dir": str(local_dir)},
    )

    urls = download_module.get_backup_csv_urls("ArchaMap")

    assert client_calls == [False, True]
    assert urls == [
        (
            "https://sociomap-backups.s3.us-west-1.amazonaws.com/archamap-backups/download/metadata_2026-03-24.csv",
            1.0,
        )
    ]


def test_get_advanced_download_raises_explicit_error_when_queries_return_no_rows(monkeypatch):
    monkeypatch.setattr(download_module, "getDriver", lambda database: object())

    def fake_get_query(query, driver, params=None, type="dict", max_retries=3, **kwargs):
        if type == "df":
            return pd.DataFrame()
        if type == "dict":
            return [{"property": "Name", "type": "node"}]
        raise AssertionError(f"Unexpected query type: {type}")

    monkeypatch.setattr(download_module, "getQuery", fake_get_query)

    try:
        download_module.getAdvancedDownload("ArchaMap", "SITE", ["Name"], ["AM404"])
    except LookupError as exc:
        assert str(exc) == "No downloadable records were found for the requested CMIDs: AM404"
    else:
        raise AssertionError("Expected LookupError when advanced download finds no rows")
