def test_root_returns_api_page(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.content_type.startswith("text/html")


def test_docs_returns_swagger_ui_page(client):
    response = client.get("/docs")
    assert response.status_code == 200
    assert response.content_type.startswith("text/html")
