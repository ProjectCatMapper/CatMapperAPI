import CMroutes.merge_routes as merge_routes


def test_submit_merge_rejects_category_cmids(client):
    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "AM1,SD2",
            "mergelevel": 1,
            "categoryLabel": "CATEGORY",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "Standard",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {},
        },
    )

    assert response.status_code == 400
    assert "Only DATASET CMIDs are allowed" in response.get_json()["error"]


def test_submit_merge_rejects_more_than_two_for_extended(client):
    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1,SD2,AD3",
            "mergelevel": 2,
            "categoryLabel": "CATEGORY",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "Extended",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {},
        },
    )

    assert response.status_code == 400
    assert "at most two dataset CMIDs" in response.get_json()["error"]


def test_submit_merge_allows_two_datasets_for_extended(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "proposeMerge", lambda **kwargs: {"ok": True, "datasets": kwargs["dataset_choices"]})

    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1,AD2",
            "mergelevel": 2,
            "categoryLabel": "CATEGORY",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "Extended",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {},
        },
    )

    assert response.status_code == 200
    assert response.get_json()["ok"] is True
