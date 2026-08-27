import pytest

from CM import USES, upload


@pytest.mark.parametrize(
    ("database", "default_dataset"),
    [("SocioMap", "SD11"), ("ArchaMap", "AD941")],
)
def test_add_cmname_rel_only_targets_default_dataset(
    monkeypatch, database, default_dataset
):
    driver = object()
    queries = []

    monkeypatch.setattr(USES, "getDriver", lambda database: driver)

    def fake_get_query(query, query_driver, params=None, type=None, **kwargs):
        queries.append({"query": query, "driver": query_driver})
        return []

    monkeypatch.setattr(USES, "getQuery", fake_get_query)
    monkeypatch.setattr(USES, "createLog", lambda **kwargs: None)
    monkeypatch.setattr(USES, "updateAltNames", lambda *args, **kwargs: None)

    assert USES.addCMNameRel(database, CMID=["CM1", "CM2"]) is None

    assert len(queries) == 2
    assert all(default_dataset in item["query"] for item in queries)
    assert all("elementId(r) IN $relIDs" not in item["query"] for item in queries)
    assert "NOT c.CMName IN coalesce(r.Name, [])" in queries[0]["query"]
    assert "coalesce(r.Name, []) + [c.CMName]" in queries[0]["query"]
    assert "NOT (c)<-[:USES]-(:DATASET" in queries[1]["query"]


def test_upload_passes_only_created_cmids_to_default_uses(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        upload,
        "addCMNameRel",
        lambda database, CMID=None: captured.update(
            database=database,
            cmids=CMID,
        ),
    )

    upload._add_cmnames_to_default_uses(
        "ArchaMap",
        {
            "result": [
                {"CMID": "AM1", "relID": "uploaded-rel-1"},
                {"CMID": "AM2", "relID": "uploaded-rel-2"},
            ]
        },
    )

    assert captured == {"database": "ArchaMap", "cmids": ["AM1", "AM2"]}
