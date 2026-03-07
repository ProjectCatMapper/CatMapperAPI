import CM.users as users_module


def test_enable_user_pending_lookup_filters_database_case_insensitively(monkeypatch):
    monkeypatch.setattr(users_module, "getDriver", lambda _database: object())
    captured = {}

    def fake_get_query(query, driver, params=None, **kwargs):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(users_module, "getQuery", fake_get_query)

    users_module.enableUser(database="sociomap", process="pending", userid=None, approver="100")

    assert "toLower(toString(db)) = toLower($database)" in captured["query"]
    assert captured["params"] == {"database": "sociomap"}
