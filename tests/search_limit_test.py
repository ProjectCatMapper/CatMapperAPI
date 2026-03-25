import pytest

import CM.search as search_module


def test_search_uses_requested_limit_in_query(monkeypatch):
    monkeypatch.setattr(search_module, "getDriver", lambda database: object())

    result = search_module.search(
        database="ArchaMap",
        term="afghanistan",
        property="Name",
        domain="ALL NODES",
        yearStart=None,
        yearEnd=None,
        context=None,
        country=None,
        query="true",
        dataset=None,
        limit="30000",
    )

    assert "limit 30000" in result["query"]


def test_search_rejects_non_integer_limit(monkeypatch):
    monkeypatch.setattr(search_module, "getDriver", lambda database: object())

    with pytest.raises(Exception, match="limit must be an integer"):
        search_module.search(
            database="ArchaMap",
            term="afghanistan",
            property="Name",
            domain="ALL NODES",
            yearStart=None,
            yearEnd=None,
            context=None,
            country=None,
            query="true",
            dataset=None,
            limit="thirty-thousand",
        )
