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


def test_submit_merge_accepts_common_dataset_separators(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_routes, "validate_domain_label", lambda label, driver=None: label)
    monkeypatch.setattr(merge_routes, "proposeMerge", lambda **kwargs: {"ok": True, "datasets": kwargs["dataset_choices"]})

    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1 AD2\nSD3\tAD4;SD5|AD6",
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
    assert payload["datasets"] == ["SD1", "AD2", "SD3", "AD4", "SD5", "AD6"]


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


def test_submit_merge_passes_extended_ancestor_only(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_routes, "validate_domain_label", lambda label, driver=None: label)
    monkeypatch.setattr(merge_routes, "proposeMerge", lambda **kwargs: {"ok": True, "payload": kwargs})

    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1,AD2",
            "mergelevel": 2,
            "ancestorOnly": True,
            "categoryLabel": "CATEGORY",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "Extended",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {},
        },
    )

    assert response.status_code == 200
    payload = response.get_json()["payload"]
    assert payload["ancestor_only"] is True


def test_submit_merge_crossdomain_requires_source_and_target(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())

    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1,AD2",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "CrossDomain",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {},
            "primaryDataset": "SD1",
        },
    )

    assert response.status_code == 400
    assert "sourceDomain and targetDomain are required" in response.get_json()["error"]


def test_submit_merge_crossdomain_requires_primary_dataset_in_choices(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_routes, "validate_domain_label", lambda label, driver=None: label)

    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1,AD2",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "CrossDomain",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {},
            "sourceDomain": "LANGUAGE",
            "targetDomain": "ETHNICITY",
            "primaryDataset": "SD9",
            "maxHops": 3,
        },
    )

    assert response.status_code == 400
    assert "primaryDataset must be included in datasetChoices" in response.get_json()["error"]


def test_submit_merge_crossdomain_validates_hops(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_routes, "validate_domain_label", lambda label, driver=None: label)

    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1,AD2",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "CrossDomain",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {},
            "sourceDomain": "LANGUAGE",
            "targetDomain": "ETHNICITY",
            "primaryDataset": "SD1",
            "maxHops": 99,
        },
    )

    assert response.status_code == 400
    assert "maxHops must be between 1 and 6" in response.get_json()["error"]


def test_submit_merge_crossdomain_passes_new_fields_to_propose_merge(client, monkeypatch):
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())
    monkeypatch.setattr(merge_routes, "validate_domain_label", lambda label, driver=None: label)
    monkeypatch.setattr(merge_routes, "proposeMerge", lambda **kwargs: {"ok": True, "payload": kwargs})

    response = client.post(
        "/proposeMergeSubmit",
        json={
            "datasetChoices": "SD1,AD2,SD3",
            "intersection": True,
            "database": "ArchaMap",
            "equivalence": "CrossDomain",
            "resultFormat": "key-to-key",
            "selectedKeyvariable": {"SD1": "lang"},
            "sourceDomain": "LANGUAGE",
            "targetDomain": "ETHNICITY",
            "returnDomain": "ETHNICITY",
            "primaryDataset": "SD1",
            "maxHops": 4,
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    merged_payload = payload["payload"]
    assert merged_payload["source_domain"] == "LANGUAGE"
    assert merged_payload["target_domain"] == "ETHNICITY"
    assert merged_payload["return_domain"] == "ETHNICITY"
    assert merged_payload["primary_dataset"] == "SD1"
    assert merged_payload["max_hops"] == 4


def test_get_keys_any_domain_queries_all_uses_ties(client, monkeypatch):
    seen = {}
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())

    def fake_validate_domain_label(label, driver=None):
        raise AssertionError(f"ANY DOMAIN should not be validated as a label: {label}")

    def fake_get_query(query, driver=None, params=None):
        seen["query"] = query
        seen["params"] = params
        return [
            {"Key": "lang == english"},
            {"Key": "period == early && phase == a"},
            {"Key": "lang == spanish"},
        ]

    monkeypatch.setattr(merge_routes, "validate_domain_label", fake_validate_domain_label)
    monkeypatch.setattr(merge_routes, "getQuery", fake_get_query)

    response = client.post(
        "/getKeys",
        json={
            "names": "SD1",
            "database": "ArchaMap",
            "subdomain": "ANY DOMAIN",
        },
    )

    assert response.status_code == 200
    assert "MATCH (d:DATASET {CMID: $datasetID})-[r:USES]->()" in seen["query"]
    assert seen["params"] == {"datasetID": "SD1"}
    assert response.get_json()["keysByDataset"]["SD1"] == ["lang", "period", "phase"]


def test_get_keys_accepts_common_dataset_separators(client, monkeypatch):
    seen = []
    monkeypatch.setattr(merge_routes, "getDriver", lambda _database: object())

    def fake_get_query(query, driver=None, params=None):
        seen.append(params["datasetID"])
        return [{"Key": "lang == english"}]

    monkeypatch.setattr(merge_routes, "getQuery", fake_get_query)

    response = client.post(
        "/getKeys",
        json={
            "names": "SD1 AD2\nSD3\tAD4;SD5|AD6",
            "database": "ArchaMap",
            "subdomain": "ANY DOMAIN",
        },
    )

    assert response.status_code == 200
    assert seen == ["SD1", "AD2", "SD3", "AD4", "SD5", "AD6"]


def test_dataset_choice_parser_accepts_non_breaking_spaces():
    raw = "SD468368 SD785\u00a0\u00a0\u00a0\u00a0 SD787\u00a0\u00a0"

    assert merge_routes.parse_dataset_choices(raw) == ["SD468368", "SD785", "SD787"]


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
