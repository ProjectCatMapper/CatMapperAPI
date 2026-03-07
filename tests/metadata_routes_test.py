import CMroutes.metadata_routes as metadata_routes


def test_upload_properties_endpoint_groups_node_and_uses_properties(client, monkeypatch):
    monkeypatch.setattr(metadata_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(
        metadata_routes,
        "getPropertiesMetadata",
        lambda driver: [
            {"property": "CMName", "description": "Category name", "type": "node"},
            {"property": "country", "description": "Country tag", "type": "relationship"},
            {"property": "CMName", "description": "Category name", "type": "node"},
            {"property": "", "description": "skip", "type": "node"},
        ],
    )

    response = client.get("/metadata/uploadProperties/archamap")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["database"] == "archamap"
    assert payload["nodeProperties"] == [{"property": "CMName", "description": "Category name"}]
    assert payload["usesProperties"] == [{"property": "country", "description": "Country tag"}]


def test_upload_properties_endpoint_returns_error_payload_on_exception(client, monkeypatch):
    monkeypatch.setattr(metadata_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(
        metadata_routes,
        "getPropertiesMetadata",
        lambda driver: (_ for _ in ()).throw(RuntimeError("metadata unavailable")),
    )

    response = client.get("/metadata/uploadProperties/sociomap")

    assert response.status_code == 500
    assert response.get_json()["error"] == "metadata unavailable"
