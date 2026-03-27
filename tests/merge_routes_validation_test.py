import CMroutes.merge_routes as merge_routes
import CM.merge as merge_module


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


def test_submit_merge_allows_more_than_two_for_extended(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_routes, "validate_domain_label", lambda label, driver=None: label)
    monkeypatch.setattr(merge_routes, "proposeMerge", lambda **kwargs: {"ok": True, "datasets": kwargs["dataset_choices"]})

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

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["datasets"] == ["SD1", "SD2", "AD3"]


def test_submit_merge_allows_two_datasets_for_extended(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_routes, "validate_domain_label", lambda label, driver=None: label)
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


def test_merge_syntax_route_accepts_dict_result(client, monkeypatch):
    monkeypatch.setattr(
        merge_module,
        "createSyntax",
        lambda template, database="SocioMap", syntax="R", dirpath=None, download=True: {
            "zip": "/tmp/merged_output.zip",
            "hash": "abc123",
        },
    )

    response = client.post(
        "/merge/syntax/ArchaMap",
        json={"template": [{"mergingID": "AM1", "datasetID": "AM2", "filePath": "/tmp"}]},
    )

    assert response.status_code == 200
    payload = response.get_json() or {}
    assert payload.get("msg") == "Syntax created successfully"
    assert payload.get("download", {}).get("hash") == "abc123"


def test_merge_syntax_route_handles_tuple_error_payload(client, monkeypatch):
    monkeypatch.setattr(
        merge_module,
        "createSyntax",
        lambda template, database="SocioMap", syntax="R", dirpath=None, download=True: (
            {"error": "synthetic merge syntax failure"},
            500,
        ),
    )

    response = client.post(
        "/merge/syntax/ArchaMap",
        json={"template": [{"mergingID": "AM1", "datasetID": "AM2", "filePath": "/tmp"}]},
    )

    assert response.status_code == 500
    payload = response.get_json() or {}
    assert payload.get("msg") == "Syntax creation failed"
    assert "synthetic merge syntax failure" in str(payload.get("error", ""))
