import pytest

import CM.ownership as ownership
import CM.upload as upload
import CMroutes.admin_routes as admin_routes
import CMroutes.upload_routes as upload_routes


def test_owner_helper_allows_admin_without_query(monkeypatch):
    monkeypatch.setattr(
        ownership,
        "getQuery",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("admin should not query ownership")),
    )

    assert ownership.assert_owned_nodes("ArchaMap", ["AM1"], {"userid": "1", "role": "admin"})


def test_owner_helper_allows_owned_node(monkeypatch):
    monkeypatch.setattr(ownership, "getDriver", lambda database: object())
    monkeypatch.setattr(
        ownership,
        "getQuery",
        lambda **kwargs: [{"cmid": "AM1", "targetCount": 1, "ownedCount": 1}],
    )

    assert ownership.assert_owned_nodes("ArchaMap", ["AM1"], {"userid": "7", "role": "user"})


def test_owner_helper_rejects_unowned_or_unmarked_node(monkeypatch):
    monkeypatch.setattr(ownership, "getDriver", lambda database: object())
    monkeypatch.setattr(
        ownership,
        "getQuery",
        lambda **kwargs: [{"cmid": "AM1", "targetCount": 1, "ownedCount": 0}],
    )

    with pytest.raises(ownership.OwnershipError, match="not authorized"):
        ownership.assert_owned_nodes("ArchaMap", ["AM1"], {"userid": "7", "role": "user"})


def test_admin_edit_allows_regular_user_for_owned_node_property(client, monkeypatch):
    captured = {}

    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "assert_owned_nodes",
        lambda database, cmids, claims: captured.update({"database": database, "cmids": cmids, "claims": claims}) or True,
    )
    monkeypatch.setattr(admin_routes, "add_edit_delete_Node", lambda database, user, input: "done")

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "add/edit/delete node property",
            "input": {"s1_1": "edit", "s1_2": "AM1", "s1_3": "New", "s1_7": "CMName"},
        },
    )

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "done"
    assert captured["cmids"] == ["AM1"]
    assert captured["claims"]["userid"] == "7"


def test_admin_edit_rejects_regular_user_for_global_admin_function(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "mergeNodes",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("mergeNodes should not run")),
    )

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "merge nodes",
            "input": {"s1_2": "AM1", "s1_3": "AM2"},
        },
    )

    assert response.status_code == 403
    assert "not authorized" in response.get_data(as_text=True).lower()


def test_upload_replacement_queue_carries_actor_and_checks_scope(client, monkeypatch):
    seen = {}

    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "validate_upload_ownership_scope",
        lambda database, upload_option, rows, claims: seen.update({
            "database": database,
            "upload_option": upload_option,
            "claims": claims,
        }) or True,
    )
    monkeypatch.setattr(upload_routes, "_start_upload_task", lambda **kwargs: seen.update(kwargs) or "task-owned")

    response = client.post(
        "/uploadInputNodes",
        json={
            "database": "ArchaMap",
            "so": "standard",
            "ao": "update_replace",
            "df": [{"CMID": "AM1", "datasetID": "AD1", "Key": "Type == A", "NewKey": "Type == B"}],
            "formData": {"datasetID": "AD1", "cmNameColumn": "Name", "categoryNamesColumn": "Name", "cmidColumn": "CMID", "keyColumn": "Key"},
            "optionalProperties": ["NewKey"],
            "user": "7",
        },
    )

    assert response.status_code == 202
    assert seen["upload_option"] == "update_replace"
    assert seen["claims"] == {"userid": "7", "role": "user"}
    assert seen["job_args"]["actorClaims"] == {"userid": "7", "role": "user"}
    assert seen["job_args"]["contributionId"].startswith("contribution_")


def test_upload_replacement_queue_rejects_unowned_targets(client, monkeypatch):
    monkeypatch.setattr(upload_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        upload_routes,
        "validate_upload_ownership_scope",
        lambda *args, **kwargs: (_ for _ in ()).throw(ownership.OwnershipError("User is not authorized to edit unowned USES relationship: AD1 / AM1 / Type == A")),
    )
    monkeypatch.setattr(
        upload_routes,
        "_start_upload_task",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("upload should not be queued")),
    )

    response = client.post(
        "/uploadInputNodes",
        json={
            "database": "ArchaMap",
            "so": "standard",
            "ao": "update_replace",
            "df": [{"CMID": "AM1", "datasetID": "AD1", "Key": "Type == A", "NewKey": "Type == B"}],
            "formData": {"datasetID": "AD1", "cmNameColumn": "Name", "categoryNamesColumn": "Name", "cmidColumn": "CMID", "keyColumn": "Key"},
            "optionalProperties": ["NewKey"],
            "user": "7",
        },
    )

    assert response.status_code == 403
    assert "unowned uses relationship" in response.get_data(as_text=True).lower()


def test_worker_rechecks_upload_ownership_before_mutating(monkeypatch):
    monkeypatch.setattr(upload, "updateLog", lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, "getDriver", lambda database: object())
    monkeypatch.setattr(
        upload,
        "getQuery",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no mutation/query should run after failed ownership check")),
    )
    monkeypatch.setattr(
        upload,
        "validate_upload_ownership_scope",
        lambda *args, **kwargs: (_ for _ in ()).throw(ownership.OwnershipError("User is not authorized to edit unowned USES relationship")),
    )

    with pytest.raises(ownership.OwnershipError):
        upload.input_Nodes_Uses(
            dataset=[{"CMID": "AM1", "datasetID": "AD1", "Key": "Type == A", "Name": "A"}],
            database="ArchaMap",
            uploadOption="update_replace",
            optionalProperties=["Name"],
            user="7",
            actorClaims={"userid": "7", "role": "user"},
        )
