from CM import USES, upload


def test_add_cmname_rel_updates_only_requested_uses_ties(monkeypatch):
    driver = object()
    captured = {}

    monkeypatch.setattr(USES, "getDriver", lambda database: driver)

    def fake_get_query(query, query_driver, params=None, type=None, **kwargs):
        captured.update(
            query=query,
            driver=query_driver,
            params=params,
            type=type,
        )
        return ["rel-2"]

    monkeypatch.setattr(USES, "getQuery", fake_get_query)
    monkeypatch.setattr(
        USES,
        "createLog",
        lambda **kwargs: captured.update(log=kwargs),
    )
    monkeypatch.setattr(
        USES,
        "updateAltNames",
        lambda database, CMID=None: captured.update(
            alt_names=(database, CMID)
        ),
    )

    result = USES.addCMNameRel(
        "ArchaMap",
        CMID=["AM1", "AM2"],
        relIDs=["rel-1", "rel-2", "rel-1", None],
    )

    assert result is None
    assert "elementId(r) IN $relIDs" in captured["query"]
    assert "NOT c.CMName IN coalesce(r.Name, [])" in captured["query"]
    assert "coalesce(r.Name, []) + [c.CMName]" in captured["query"]
    assert captured["params"] == {"relIDs": ["rel-1", "rel-2"]}
    assert captured["type"] == "list"
    assert captured["log"]["id"] == ["rel-2"]
    assert captured["alt_names"] == ("ArchaMap", ["AM1", "AM2"])


def test_add_cmname_rel_skips_empty_relationship_scope(monkeypatch):
    monkeypatch.setattr(USES, "getDriver", lambda database: object())
    monkeypatch.setattr(
        USES,
        "getQuery",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("query should not run")
        ),
    )

    assert USES.addCMNameRel("SocioMap", CMID=["SM1"], relIDs=[]) is None


def test_upload_passes_specific_created_relationships_to_add_cmname(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        upload,
        "addCMNameRel",
        lambda database, CMID=None, relIDs=None: captured.update(
            database=database,
            cmids=CMID,
            rel_ids=relIDs,
        ),
    )

    upload._add_cmnames_to_uploaded_uses(
        "ArchaMap",
        {
            "result": [
                {"CMID": "AM1", "relID": "rel-1"},
                {"CMID": "AM2", "relID": "rel-2"},
            ]
        },
    )

    assert captured == {
        "database": "ArchaMap",
        "cmids": ["AM1", "AM2"],
        "rel_ids": ["rel-1", "rel-2"],
    }
