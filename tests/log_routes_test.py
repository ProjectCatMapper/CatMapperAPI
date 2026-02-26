import CMroutes.log_routes as log_routes


def test_logs_route_returns_logs_and_queries_both_uses_sides(client, monkeypatch):
    captured = {}

    monkeypatch.setattr(log_routes, "getDriver", lambda database: object())

    def fake_get_query(query, driver, CMID=None, **kwargs):
        captured["query"] = query
        captured["cmid"] = CMID
        return [{"ID": "1", "user": "tester"}]

    monkeypatch.setattr(log_routes, "getQuery", fake_get_query)

    response = client.get("/logs/archamap/AD339121")

    assert response.status_code == 200
    payload = response.get_json() or {}
    assert payload.get("logs") == [{"ID": "1", "user": "tester"}]
    assert captured.get("cmid") == "AD339121"
    assert "(c.CMID = $CMID OR d.CMID = $CMID)" in (captured.get("query") or "")


def test_logs_route_passes_through_query_error_dict(client, monkeypatch):
    monkeypatch.setattr(log_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(log_routes, "getQuery", lambda **kwargs: {"error": "boom"})

    response = client.get("/logs/archamap/AD339121")

    assert response.status_code == 200
    assert response.get_json() == {"error": "boom"}
