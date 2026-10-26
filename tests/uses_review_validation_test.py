from unittest.mock import Mock

import pytest
import CMroutes.admin_routes as routes


def payload():
    return {
        "s1_1": "edit", "s1_2": "AM1", "s1_3": "Ware == Plain Brown Ware",
        "s1_7": 1, "s1_8": "Key",
        "s1_4": [[{"CMID": "AM1"}, {"id": "rel-1", "Key": "Type == Plain Brown Ware"}, {"CMID": "AD187"}]],
    }


@pytest.mark.parametrize("role", ["user", "admin"])
@pytest.mark.parametrize("patch", [
    {"s1_7": "", "s1_8": ""},  # Archived #28/#29: queued despite no selection.
    {"s1_8": ""},  # Archived #26/#27: selected tie but blank property.
    {"s1_7": "", "s1_4": [], "s1_2": "AD116"},  # Archived #15.
    {"s1_7": 0}, {"s1_7": -1}, {"s1_7": 2}, {"s1_7": 1.5}, {"s1_7": True},
    {"s1_2": "AM2"}, {"s1_3": "Ware = Plain Brown Ware"},
])
def test_incomplete_uses_edits_never_reach_ownership_or_queue(client, monkeypatch, role, patch):
    data = payload()
    data.update(patch)
    monkeypatch.setattr(routes, "verify_request_auth", lambda **kw: {"userid": "7", "role": role})
    authorize, create, execute = Mock(), Mock(), Mock()
    monkeypatch.setattr(routes, "_authorize_admin_edit_function", authorize)
    monkeypatch.setattr(routes, "_create_change_review", create)
    monkeypatch.setattr(routes, "_execute_admin_edit", execute)
    response = client.post("/admin/edit", headers={"Authorization": "Bearer test"}, json={
        "database": "archamap", "fun": "add/edit/delete USES property", "input": data,
    })
    assert response.status_code == 400
    authorize.assert_not_called()
    create.assert_not_called()
    execute.assert_not_called()


@pytest.mark.parametrize("live", [[], [{"properties": {"Key": "changed"}}]])
def test_missing_or_changed_live_tie_cannot_be_submitted(client, monkeypatch, live):
    monkeypatch.setattr(routes, "verify_request_auth", lambda **kw: {"userid": "7", "role": "user"})
    monkeypatch.setattr(routes, "getDriver", lambda db: object())
    query = Mock(return_value=live)
    monkeypatch.setattr(routes, "getQuery", query)
    create = Mock()
    monkeypatch.setattr(routes, "_create_change_review", create)
    response = client.post("/admin/edit", headers={"Authorization": "Bearer test"}, json={
        "database": "archamap", "fun": "add/edit/delete USES property", "input": payload(),
    })
    assert response.status_code == 400
    assert query.call_args.kwargs["params"] == {"datasetID": "AD187", "CMID": "AM1", "relID": "rel-1"}
    create.assert_not_called()


def test_valid_unowned_edit_still_enters_approval_queue(client, monkeypatch):
    monkeypatch.setattr(routes, "verify_request_auth", lambda **kw: {"userid": "7", "role": "user"})
    monkeypatch.setattr(routes, "getDriver", lambda db: object())
    monkeypatch.setattr(routes, "getQuery", lambda **kw: [{"properties": payload()["s1_4"][0][1]}])
    monkeypatch.setattr(routes, "_authorize_admin_edit_function", Mock(side_effect=routes.OwnershipError("not owner")))
    create = Mock(return_value={"requestId": "new-review"})
    monkeypatch.setattr(routes, "_create_change_review", create)
    response = client.post("/admin/edit", headers={"Authorization": "Bearer test"}, json={
        "database": "archamap", "fun": "add/edit/delete USES property", "input": payload(),
    })
    assert response.status_code == 202
    assert response.get_json()["submittedForReview"] is True
    create.assert_called_once()


def test_direct_queue_creation_rejects_invalid_target_before_persistence(monkeypatch):
    query = Mock()
    monkeypatch.setattr(routes, "getQuery", query)
    with pytest.raises(routes.InvalidUsesEdit):
        routes._create_change_review("archamap", "add/edit/delete USES property", {}, [], "", {"userid": "7"}, "not owner")
    query.assert_not_called()


def test_invalid_legacy_review_cannot_be_claimed_for_approval(client, monkeypatch):
    monkeypatch.setattr(routes, "verify_request_auth", lambda **kw: {"userid": "1", "role": "admin"})
    monkeypatch.setattr(routes, "_load_change_review", lambda rid: {
        "status": "pending", "database": "archamap", "action": "add/edit/delete USES property", "input": {},
    })
    query = Mock()
    monkeypatch.setattr(routes, "getQuery", query)
    response = client.post("/admin/change-reviews/legacy/decision", json={"decision": "approve"})
    assert response.status_code == 400
    query.assert_not_called()
