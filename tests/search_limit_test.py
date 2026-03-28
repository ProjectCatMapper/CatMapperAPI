import pytest
import importlib

search_module = importlib.import_module("CM.search")


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


def test_search_year_range_uses_overlap_logic_in_query(monkeypatch):
    monkeypatch.setattr(search_module, "getDriver", lambda database: object())
    monkeypatch.setattr(search_module, "validate_domain_label", lambda domain, **kwargs: domain)

    result = search_module.search(
        database="ArchaMap",
        term="afghanistan",
        property="Name",
        domain="DISTRICT",
        yearStart="1990",
        yearEnd="2000",
        context=None,
        country=None,
        query="true",
        dataset=None,
    )

    query = result["query"]
    assert "apoc.coll.min(years) <= inputYearEnd" in query
    assert "apoc.coll.max(years) >= inputYearStart" in query
    assert "apoc.coll.min(rStarts) >= inputYearStart" not in query
    assert "apoc.coll.max(rEnds) <= inputYearEnd" not in query


def test_search_rejects_year_range_when_start_after_end(monkeypatch):
    monkeypatch.setattr(search_module, "getDriver", lambda database: object())
    monkeypatch.setattr(search_module, "validate_domain_label", lambda domain, **kwargs: domain)

    with pytest.raises(Exception, match="yearStart must be less than or equal to yearEnd"):
        search_module.search(
            database="ArchaMap",
            term="afghanistan",
            property="Name",
            domain="DISTRICT",
            yearStart="2001",
            yearEnd="2000",
            context=None,
            country=None,
            query="true",
            dataset=None,
        )
