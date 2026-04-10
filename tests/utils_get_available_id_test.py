import CM.utils as utils


def test_get_available_id_reserves_deleted_dataset_ids(monkeypatch):
    monkeypatch.setattr(utils, "getDriver", lambda _database: object())

    captured = {}

    def fake_get_query(query, driver=None, params=None, type=None, **kwargs):
        captured["query"] = query
        captured["params"] = params
        captured["type"] = type
        # Simulate active AD1/AD2 plus a deleted AD3 so the next ID must be AD4.
        return [1, 2, 3]

    monkeypatch.setattr(utils, "getQuery", fake_get_query)

    result = utils.getAvailableID(label="DATASET", n=1, database="ArchaMap")

    assert result == ["AD4"]
    assert "WHERE (n:DATASET OR n:DELETED)" in captured["query"]
    assert captured["params"]["pattern"] == "^AD[0-9]+$"
    assert captured["params"]["prefix"] == "AD"

