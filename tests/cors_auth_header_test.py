def test_cors_preflight_allows_authorization_header(client, monkeypatch):
    # Avoid credential verification path; this test only checks CORS behavior.
    monkeypatch.setattr("CMroutes.user_routes._verify_profile_credentials", lambda userid, credentials: True)
    monkeypatch.setattr("CMroutes.user_routes._get_user_entries", lambda userid, field_name: [])

    response = client.open(
        "/profile/bookmarks/100",
        method="OPTIONS",
        headers={
            "Origin": "https://test.catmapper.org",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization,content-type",
        },
    )

    assert response.status_code in (200, 204)
    allow_headers = response.headers.get("Access-Control-Allow-Headers", "").lower()
    assert "authorization" in allow_headers
