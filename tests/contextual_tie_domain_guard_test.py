from CM import USES as uses_module


def test_validate_contextual_tie_primary_domains_uses_endpoint_labels(monkeypatch):
    monkeypatch.setattr(
        uses_module,
        "getQuery",
        lambda query, driver=None, params=None, **kwargs: [
            {"targetCMID": "SM-FAMILY", "sharedLabels": ["LANGUOID"]}
        ],
    )

    try:
        uses_module.validate_contextual_tie_primary_domains(
            object(),
            "SM-DIALECT",
            ["SM-FAMILY"],
            "LANGUAGE_OF",
        )
    except ValueError as exc:
        assert "LANGUAGE_OF" in str(exc)
        assert "LANGUOID" in str(exc)
    else:
        raise AssertionError("Expected shared LANGUOID label to be rejected")


def test_contextual_tie_conflict_query_checks_shared_neo4j_labels(monkeypatch):
    captured = {}

    def fake_get_query(query, driver=None, params=None, **kwargs):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(uses_module, "getQuery", fake_get_query)

    result = uses_module._contextual_tie_primary_domain_conflicts(
        object(),
        ["SM-DIALECT"],
        "language",
        "LANGUAGE_OF",
    )

    assert result == []
    assert "label IN labels(target)" in captured["query"]
    assert "label <> 'CATEGORY'" in captured["query"]
    assert "AREA_OF" in captured["query"]
    assert captured["params"] == {
        "source_cmids": ["SM-DIALECT"],
        "property": "language",
        "relationship": "LANGUAGE_OF",
    }


def test_fix_uses_rels_does_not_create_same_domain_tie(monkeypatch):
    monkeypatch.setattr(uses_module, "getDriver", lambda database: object())
    queries = []

    def fake_get_query(query, driver=None, params=None, **kwargs):
        queries.append(query)
        if "sharedLabels" in query:
            return [
                {
                    "sourceCMID": "SM-DIALECT",
                    "targetCMID": "SM-FAMILY",
                    "sharedLabels": ["LANGUOID"],
                }
            ]
        raise AssertionError("Relationship creation query must not run after a conflict")

    monkeypatch.setattr(uses_module, "getQuery", fake_get_query)

    result = uses_module.fixUsesRels(
        "SocioMap",
        "language",
        "LANGUAGE_OF",
        CMID=["SM-DIALECT"],
    )

    assert result[1] == 500
    assert "same primary domain" in result[0]
    assert "LANGUOID" in result[0]
    assert len(queries) == 1
