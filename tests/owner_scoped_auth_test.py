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


def test_admin_edit_rejects_regular_user_node_log_property(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "add_edit_delete_Node",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("node edit should not run")),
    )

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "add/edit/delete node property",
            "input": {"s1_1": "edit", "s1_2": "AM1", "s1_3": "tamper", "s1_7": "log"},
        },
    )

    assert response.status_code == 403
    assert "log properties" in response.get_data(as_text=True).lower()


def test_admin_edit_rejects_regular_user_uses_log_property(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "add_edit_delete_USES",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("uses edit should not run")),
    )

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "add/edit/delete USES property",
            "input": {"s1_1": "edit", "s1_2": "AM1", "s1_3": "tamper", "s1_8": "logID"},
        },
    )

    assert response.status_code == 403
    assert "this uses property" in response.get_data(as_text=True).lower()


def test_admin_edit_rejects_regular_user_uses_ownership_metadata_property(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "add_edit_delete_USES",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("uses edit should not run")),
    )

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "add/edit/delete USES property",
            "input": {"s1_1": "edit", "s1_2": "AM1", "s1_3": "8", "s1_8": "ownerUserId"},
        },
    )

    assert response.status_code == 403
    assert "this uses property" in response.get_data(as_text=True).lower()


def test_admin_edit_allows_regular_user_for_owned_uses_key_property(client, monkeypatch):
    seen = {}
    selected_relation = [
        {"CMID": "AM1", "CMName": "Node"},
        {"id": "rel-owned", "Key": "Type == A"},
        {"CMID": "AD1", "CMName": "Dataset"},
    ]

    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "assert_owned_uses_by_relids",
        lambda database, relids, claims: seen.update({"database": database, "relids": relids, "claims": claims}) or True,
    )
    monkeypatch.setattr(admin_routes, "add_edit_delete_USES", lambda database, user, input: "done")

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "add/edit/delete USES property",
            "input": {
                "s1_1": "edit",
                "s1_2": "AM1",
                "s1_3": "Type == B",
                "s1_4": [selected_relation],
                "s1_7": "1",
                "s1_8": "Key",
            },
        },
    )

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "done"
    assert seen["relids"] == ["rel-owned"]


def test_admin_edit_allows_regular_user_merge_for_owned_isolated_discard_node(client, monkeypatch):
    seen = {}

    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "assert_owner_scoped_node_removal_allowed",
        lambda database, cmid, claims: seen.update({"database": database, "cmid": cmid, "claims": claims}) or True,
    )
    monkeypatch.setattr(admin_routes, "mergeNodes", lambda keep, discard, user, database: f"merged {discard} into {keep}")

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "merge nodes",
            "input": {"s1_2": "AM_KEEP", "s1_3": "AM_DISCARD"},
        },
    )

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "merged AM_DISCARD into AM_KEEP"
    assert seen["cmid"] == "AM_DISCARD"
    assert seen["claims"]["userid"] == "7"


def test_admin_edit_allows_regular_user_delete_for_owned_isolated_node(client, monkeypatch):
    seen = {}

    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(
        admin_routes,
        "assert_owner_scoped_node_removal_allowed",
        lambda database, cmid, claims: seen.update({"database": database, "cmid": cmid, "claims": claims}) or True,
    )
    monkeypatch.setattr(admin_routes, "deleteNode", lambda database, user, input: "deleted")

    response = client.post(
        "/admin/edit",
        headers={"Authorization": "Bearer test-token"},
        json={
            "database": "ArchaMap",
            "fun": "delete node",
            "input": {"s1_2": "AM_DELETE"},
        },
    )

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "deleted"
    assert seen["cmid"] == "AM_DELETE"


def test_owner_helper_rejects_node_removal_with_unowned_uses_ties(monkeypatch):
    monkeypatch.setattr(ownership, "getDriver", lambda database: object())

    def fake_get_query(**kwargs):
        query = kwargs.get("query", "")
        if "count(n) AS targetCount" in query:
            return [{"cmid": "AM1", "targetCount": 1, "ownedCount": 1}]
        if "unownedIncidentUses" in query:
            return [{"cmid": "AM1", "incidentUses": 2, "unownedIncidentUses": 1}]
        raise AssertionError("reference query should not run after unowned incident USES")

    monkeypatch.setattr(ownership, "getQuery", fake_get_query)

    with pytest.raises(ownership.OwnershipError, match="USES ties not owned"):
        ownership.assert_owner_scoped_node_removal_allowed("ArchaMap", "AM1", {"userid": "7", "role": "user"})


def test_owner_helper_rejects_node_removal_when_cmid_referenced_elsewhere(monkeypatch):
    monkeypatch.setattr(ownership, "getDriver", lambda database: object())

    def fake_get_query(**kwargs):
        query = kwargs.get("query", "")
        if "count(n) AS targetCount" in query:
            return [{"cmid": "AM1", "targetCount": 1, "ownedCount": 1}]
        if "unownedIncidentUses" in query:
            return [{"cmid": "AM1", "incidentUses": 1, "unownedIncidentUses": 0}]
        if "keys(r)" in query:
            return [{"relID": "rel-other", "Key": "Type == A"}]
        raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(ownership, "getQuery", fake_get_query)

    with pytest.raises(ownership.OwnershipError, match="referenced in other USES ties"):
        ownership.assert_owner_scoped_node_removal_allowed("ArchaMap", "AM1", {"userid": "7", "role": "user"})


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
