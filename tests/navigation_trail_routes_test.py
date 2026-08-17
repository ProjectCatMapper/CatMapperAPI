from unittest.mock import Mock

import CMroutes.navigation_trail_routes as navigation_trail_routes


SESSION_ID = "00000000-0000-4000-8000-000000000001"


def test_navigation_event_stores_full_url_and_timestamps(client, monkeypatch):
    store = Mock()
    monkeypatch.setattr(navigation_trail_routes, "store_navigation_event", store)

    response = client.post(
        "/api/navigation-trail/events",
        json={
            "sessionId": SESSION_ID,
            "url": "https://catmapper.org/SocioMap/explore?dataset=example&view=table",
            "occurredAt": "2026-08-17T18:42:45.000Z",
        },
    )

    assert response.status_code == 201
    event = store.call_args.args[0]
    assert event["sessionId"] == SESSION_ID
    assert event["url"] == "https://catmapper.org/SocioMap/explore?dataset=example&view=table"
    assert event["occurredAt"] == "2026-08-17T18:42:45.000Z"
    assert event["recordedAt"]


def test_navigation_event_validates_session_id_and_url(client, monkeypatch):
    store = Mock()
    monkeypatch.setattr(navigation_trail_routes, "store_navigation_event", store)

    invalid_session = client.post(
        "/api/navigation-trail/events",
        json={"sessionId": "not-a-session", "url": "https://catmapper.org/", "occurredAt": "now"},
    )
    missing_url = client.post(
        "/api/navigation-trail/events",
        json={"sessionId": SESSION_ID, "url": "", "occurredAt": "now"},
    )
    external_url = client.post(
        "/api/navigation-trail/events",
        json={"sessionId": SESSION_ID, "url": "https://example.org/?not=internal", "occurredAt": "now"},
    )

    assert invalid_session.status_code == 400
    assert missing_url.status_code == 400
    assert external_url.status_code == 400
    store.assert_not_called()


def test_withdrawing_consent_deletes_the_session_trail(client, monkeypatch):
    delete = Mock()
    monkeypatch.setattr(navigation_trail_routes, "delete_navigation_trail", delete)

    response = client.delete(f"/api/navigation-trail/{SESSION_ID}")

    assert response.status_code == 204
    delete.assert_called_once_with(SESSION_ID)
