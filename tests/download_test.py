import CMroutes.download_routes as download_routes


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
