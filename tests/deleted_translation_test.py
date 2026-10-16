import importlib

import pytest

search_module = importlib.import_module("CM.search")


@pytest.fixture
def mocked_search(monkeypatch):
    monkeypatch.setattr(search_module, "getDriver", lambda database: object())
    monkeypatch.setattr(search_module, "validate_domain_label", lambda domain, **kwargs: domain)
    monkeypatch.setattr(search_module, "getQuery", lambda *args, **kwargs: [])
    return search_module


@pytest.mark.parametrize("property", ["CMID", "Key", "Name", "ISO", "glottocode", "CMName"])
def test_translate_resolves_deleted_chain_before_domain_and_context_filters(mocked_search, property):
    result = mocked_search.translate(
        database="ArchaMap", property=property, domain="CERAMIC_TYPE",
        key="false", term="source", country="", context="context_id", dataset="",
        yearStart=None, yearEnd=None, query="true",
        table=[{"source": "AM1", "context_id": "AM2"}], countsamename=None,
    )
    query = result[0]["query"]
    redirect = query.index("match replacementPath")
    assert "[:IS*0..]" in query
    assert "not replacement:DELETED" in query
    assert "nodes(replacementPath)[..-1]" in query
    assert query.index("where 'CERAMIC_TYPE' in labels(a)") > redirect
    assert query.index("{CMID: row.context}") > redirect
    assert "with distinct row, replacement as a, matching, score" in query
    if property == "Key":
        assert "labels(a) and isMatch" not in query


@pytest.mark.parametrize("property", ["CMID", "Key", "Name", "CMName"])
def test_search_resolves_deleted_chain_before_domain_filter(mocked_search, property):
    result = mocked_search.search(
        database="ArchaMap", term="AM1", property=property, domain="CERAMIC_TYPE",
        yearStart=None, yearEnd=None, context=None, country=None,
        query="true", dataset=None,
    )
    query = result["query"]
    assert query.index("match replacementPath") < query.index("where 'CERAMIC_TYPE' in labels(a)")
    assert "with distinct replacement as a, matching, score" in query
    assert "optional match (a)-[:IS]->(b)" not in query


def test_search_count_uses_same_replacement_resolution(mocked_search, monkeypatch):
    queries = []
    def capture(query, *args, **kwargs):
        queries.append(query)
        return []
    monkeypatch.setattr(mocked_search, "getQuery", capture)
    mocked_search.search(
        database="ArchaMap", term="AM1", property="CMID", domain="CATEGORY",
        yearStart=None, yearEnd=None, context=None, country=None,
        query="false", dataset=None,
    )
    assert len(queries) == 2
    assert all("match replacementPath" in query for query in queries)


def test_translation_preserves_source_and_reports_active_identifier(mocked_search, monkeypatch):
    def resolved_result(query, *args, **kwargs):
        assert "match replacementPath" in query
        return [{
            "CMuniqueCategoryID": 0, "CMuniqueRowID": [0], "term": "AM1",
            "CMID": "AM3", "CMName": "Current type", "label": "CERAMIC_TYPE",
            "matching": "AM1", "matchingDistance": 0, "CMcountry": [], "Key": "",
        }]
    monkeypatch.setattr(mocked_search, "getQuery", resolved_result)
    data, _, _ = mocked_search.translate(
        database="ArchaMap", property="CMID", domain="CERAMIC_TYPE",
        key="false", term="source", country="", context="", dataset="",
        yearStart=None, yearEnd=None, query="false",
        table=[{"source": "AM1"}], countsamename=None,
    )
    assert data["source"].tolist() == ["AM1"]
    assert data["CMID_source"].tolist() == ["AM3"]
    assert data["matching_source"].tolist() == ["AM1"]
