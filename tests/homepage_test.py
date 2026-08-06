import CMroutes.homepage_routes as homepage_routes


def test_archamap_homepage_artifact_count_includes_glass(client, monkeypatch):
    monkeypatch.setattr(homepage_routes, "getDriver", lambda database: f"driver-{database}")

    captured_labels = []

    def fake_get_query(query, driver=None, params=None, **kwargs):
        assert driver == "driver-ArchaMap"
        captured_labels.extend((params or {}).get("labels", []))
        return [
            {"label": label, "node_count": 7 if label == "GLASS" else 1}
            for label in (params or {}).get("labels", [])
        ]

    monkeypatch.setattr(homepage_routes, "getQuery", fake_get_query)

    response = client.get("/homepagecount/ArchaMap")

    assert response.status_code == 200
    assert "GLASS" in captured_labels
    artifact_row = next(row for row in response.get_json() if row["label"] == "Artifact")
    assert artifact_row["node_count"] == 12
