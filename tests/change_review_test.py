import CMroutes.admin_routes as admin_routes
import pytest


def _review(status="pending", notify=True):
    return {
        "requestId": "change-123",
        "database": "archamap",
        "action": "add/edit/delete node property",
        "targetCmid": "AM1",
        "submittedBy": "7",
        "submittedAt": "2026-08-26T12:00:00Z",
        "status": status,
        "authorizationReason": "User is not authorized",
        "input": {"s1_1": "edit", "s1_2": "AM1", "s1_3": "New", "s1_7": "CMName"},
        "tabledata": [],
        "datasetID": "",
        "notifyRequester": notify,
    }


def test_create_change_review_persists_safe_payload_and_notifies_admins(monkeypatch):
    captured = {}

    def fake_get_query(**kwargs):
        if "RETURN u.database AS database" in kwargs["query"]:
            return [{"database": "archamap"}]
        captured.update(kwargs)
        params = kwargs["params"]
        return [{
            **params,
            "submitterName": "user-seven",
            "status": "pending",
            "notifyRequester": False,
        }]

    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "getQuery", fake_get_query)
    monkeypatch.setattr(
        admin_routes,
        "_notify_admins_of_change_review",
        lambda review: {"recipients": 2, "result": "sent"},
    )

    review = admin_routes._create_change_review(
        database="ArchaMap",
        action="add/edit/delete node property",
        input_payload={"s1_2": "AM1", "_actorClaims": {"userid": "7"}},
        tabledata=None,
        dataset_id=None,
        claims={"userid": "7", "role": "user"},
        reason="not owner",
    )

    assert review["requestId"].startswith("change_")
    assert review["database"] == "archamap"
    assert review["adminNotification"]["recipients"] == 2
    assert "_actorClaims" not in captured["params"]["inputJson"]
    assert captured["params"]["targetCmid"] == "AM1"


def test_admin_notification_recipients_default_on_and_opt_out_per_database(monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "getQuery", lambda **kwargs: [
        {"email": "rbischoff@asu.edu", "database": "sociomap|archamap", "socioPref": None, "archaPref": None},
        {"email": "dhruschk@asu.edu", "database": "sociomap|archamap", "socioPref": None, "archaPref": False},
        {"email": "explicit@example.org", "database": "sociomap", "socioPref": True, "archaPref": None},
        {"email": "default-off@example.org", "database": "sociomap|archamap", "socioPref": None, "archaPref": None},
    ])

    assert admin_routes._change_review_admin_recipients("archamap") == ["rbischoff@asu.edu"]
    assert admin_routes._change_review_admin_recipients("sociomap") == [
        "rbischoff@asu.edu",
        "dhruschk@asu.edu",
        "explicit@example.org",
    ]


def test_review_email_defaults_only_to_robert_and_dan(monkeypatch):
    monkeypatch.delenv("CATMAPPER_CHANGE_REVIEW_DEFAULT_RECIPIENTS", raising=False)

    assert admin_routes._change_review_email_preference(
        {"email": "rbischoff@asu.edu", "socioPref": None}, "sociomap"
    ) is True
    assert admin_routes._change_review_email_preference(
        {"email": "dhruschk@asu.edu", "socioPref": None}, "sociomap"
    ) is True
    assert admin_routes._change_review_email_preference(
        {"email": "other@example.org", "socioPref": None}, "sociomap"
    ) is False


def test_change_review_rejects_database_outside_user_access(monkeypatch):
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "getQuery", lambda **kwargs: [{"database": "sociomap"}])

    with pytest.raises(admin_routes.OwnershipError, match="this database"):
        admin_routes._create_change_review(
            database="archamap",
            action="delete node",
            input_payload={"s1_2": "AM1"},
            tabledata=[],
            dataset_id="",
            claims={"userid": "7", "role": "user"},
            reason="not owner",
        )


def test_requester_can_opt_in_to_approval_email(client, monkeypatch):
    seen = {}
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "7", "role": "user"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(
        admin_routes,
        "getQuery",
        lambda **kwargs: seen.update(kwargs["params"]) or [{"requestId": "change-123"}],
    )

    response = client.patch(
        "/admin/change-reviews/change-123/notification",
        headers={"Authorization": "Bearer token"},
        json={"notifyRequester": True},
    )

    assert response.status_code == 200
    assert response.get_json()["notifyRequester"] is True
    assert seen == {"requestId": "change-123", "userid": "7", "notifyRequester": True}


def test_admin_approval_applies_stored_change_and_marks_it_approved(client, monkeypatch):
    loaded = {"count": 0}
    executed = {}

    def fake_load(request_id):
        loaded["count"] += 1
        return _review(status="pending" if loaded["count"] == 1 else "approved")

    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "1", "role": "admin"})
    monkeypatch.setattr(admin_routes, "_load_change_review", fake_load)
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "getQuery", lambda **kwargs: [{"requestId": "change-123"}])
    monkeypatch.setattr(
        admin_routes,
        "_execute_admin_edit",
        lambda database, action, acting_user, input_payload, data: executed.update({
            "database": database,
            "action": action,
            "actingUser": acting_user,
            "claims": input_payload["_actorClaims"],
        }) or "done",
    )
    monkeypatch.setattr(admin_routes, "_send_change_review_approval_email", lambda review: "sent")

    response = client.post(
        "/admin/change-reviews/change-123/decision",
        headers={"Authorization": "Bearer admin-token"},
        json={"decision": "approve"},
    )

    assert response.status_code == 200
    assert response.get_json()["message"] == "Change approved and applied."
    assert executed == {
        "database": "archamap",
        "action": "add/edit/delete node property",
        "actingUser": "1",
        "claims": {"userid": "1", "role": "admin"},
    }


def test_admin_can_reject_without_applying_change(client, monkeypatch):
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "1", "role": "admin"})
    monkeypatch.setattr(admin_routes, "_load_change_review", lambda request_id: _review(status="pending"))
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "getQuery", lambda **kwargs: [{"requestId": "change-123"}])
    monkeypatch.setattr(
        admin_routes,
        "_execute_admin_edit",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("rejected change must not run")),
    )

    response = client.post(
        "/admin/change-reviews/change-123/decision",
        headers={"Authorization": "Bearer admin-token"},
        json={"decision": "reject", "note": "Insufficient evidence"},
    )

    assert response.status_code == 200
    assert response.get_json()["message"] == "Change rejected."


def test_approval_email_uses_requesters_address_on_file(monkeypatch):
    sent = {}
    monkeypatch.setenv("CATMAPPER_CHANGE_REVIEW_EMAIL_ENABLED", "1")
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())
    monkeypatch.setattr(admin_routes, "getQuery", lambda **kwargs: [{"email": "requester@example.org"}])
    monkeypatch.setattr(admin_routes, "get_default_sender", lambda: "noreply@example.org")
    monkeypatch.setattr(admin_routes, "sendEmail", lambda **kwargs: sent.update(kwargs) or "sent")

    result = admin_routes._send_change_review_approval_email(_review(notify=True))

    assert result == "sent"
    assert sent["recipients"] == ["requester@example.org"]
    assert "change-123" in sent["body"]


def test_review_email_delivery_is_paused_by_default(monkeypatch):
    monkeypatch.delenv("CATMAPPER_CHANGE_REVIEW_EMAIL_ENABLED", raising=False)
    monkeypatch.setattr(
        admin_routes,
        "sendEmail",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("email must remain paused")),
    )

    assert admin_routes._notify_admins_of_change_review(_review()) == {
        "recipients": 0,
        "result": "Change-review email delivery is paused",
    }
    assert admin_routes._send_change_review_approval_email(_review(notify=True)) is None


def test_change_review_preferences_are_independent_by_database(client, monkeypatch):
    captured = {}
    monkeypatch.setattr(admin_routes, "verify_request_auth", lambda **kwargs: {"userid": "1", "role": "admin"})
    monkeypatch.setattr(admin_routes, "getDriver", lambda database: object())

    def fake_get_query(**kwargs):
        captured.update(kwargs)
        return [{"email": "other@example.org", "socioPref": False, "archaPref": True}]

    monkeypatch.setattr(admin_routes, "getQuery", fake_get_query)
    response = client.patch(
        "/admin/change-review-preferences",
        headers={"Authorization": "Bearer admin-token"},
        json={"database": "sociomap", "enabled": False},
    )

    assert response.status_code == 200
    assert response.get_json() == {"sociomap": False, "archamap": True, "deliveryEnabled": False}
    assert "changeReviewEmailSocioMap" in captured["query"]
    assert captured["params"]["enabled"] is False
