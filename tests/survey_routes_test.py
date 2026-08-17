from unittest.mock import Mock

import CMroutes.survey_routes as survey_routes


def test_survey_response_stores_only_expected_fields(client, monkeypatch):
    stored = []
    sent = Mock(return_value="Email sent successfully")
    monkeypatch.setattr(survey_routes, "_rate_limit_exceeded", lambda: False)
    monkeypatch.setattr(survey_routes, "store_survey_response", stored.append)
    monkeypatch.setattr(survey_routes, "_send_survey_notification", sent)

    response = client.post(
        "/api/survey-responses",
        json={
            "campaignId": "user-purpose-2026-08",
            "choice": "other",
            "otherText": "Researching classifications",
            "url": "/SocioMap/explore",
            "ip": "192.0.2.42",
        },
        environ_base={"REMOTE_ADDR": "192.0.2.42"},
    )

    assert response.status_code == 201
    assert set(stored[0]) == {
        "responseId",
        "campaignId",
        "choice",
        "otherText",
        "submittedAt",
    }
    assert stored[0]["otherText"] == "Researching classifications"
    assert "192.0.2.42" not in str(stored[0])
    assert "/SocioMap/explore" not in str(stored[0])
    sent.assert_called_once_with(stored[0])


def test_survey_notification_emails_response_details(monkeypatch):
    send = Mock(return_value="Email sent successfully")
    monkeypatch.setattr(survey_routes, "sendEmail", send)
    monkeypatch.setattr(survey_routes, "get_default_sender", lambda: "noreply@catmapper.org")
    response = {
        "responseId": "response-123",
        "campaignId": "launch-week",
        "choice": "other",
        "otherText": "Researching classifications",
        "submittedAt": "2026-08-17T18:42:45+00:00",
    }

    survey_routes._send_survey_notification(response)

    send.assert_called_once_with(
        mail=survey_routes.mail,
        subject="New CatMapper survey response",
        recipients=["admin@catmapper.org"],
        sender="noreply@catmapper.org",
        body=(
            "A CatMapper survey response was submitted.\n\n"
            "Response ID: response-123\n"
            "Campaign: launch-week\n"
            "Answer: other\n"
            "Submitted at: 2026-08-17T18:42:45+00:00\n"
            "Further comments: Researching classifications\n"
        ),
    )


def test_survey_response_enforces_1000_character_limit(client, monkeypatch):
    store = Mock()
    monkeypatch.setattr(survey_routes, "_rate_limit_exceeded", lambda: False)
    monkeypatch.setattr(survey_routes, "store_survey_response", store)
    monkeypatch.setattr(survey_routes, "_send_survey_notification", Mock())

    accepted = client.post(
        "/api/survey-responses",
        json={"campaignId": "campaign", "choice": "other", "otherText": "x" * 1000},
    )
    rejected = client.post(
        "/api/survey-responses",
        json={"campaignId": "campaign", "choice": "other", "otherText": "x" * 1001},
    )

    assert accepted.status_code == 201
    assert rejected.status_code == 400
    assert "1000 characters or fewer" in rejected.get_json()["error"]
    assert store.call_count == 1


def test_survey_response_validates_choice_and_required_comment(client, monkeypatch):
    store = Mock()
    monkeypatch.setattr(survey_routes, "_rate_limit_exceeded", lambda: False)
    monkeypatch.setattr(survey_routes, "store_survey_response", store)

    invalid_choice = client.post(
        "/api/survey-responses",
        json={"campaignId": "campaign", "choice": "tracking"},
    )
    missing_other = client.post(
        "/api/survey-responses",
        json={"campaignId": "campaign", "choice": "other"},
    )

    assert invalid_choice.status_code == 400
    assert missing_other.status_code == 400
    store.assert_not_called()


def test_survey_response_reports_storage_outage(client, monkeypatch):
    monkeypatch.setattr(survey_routes, "_rate_limit_exceeded", lambda: False)
    monkeypatch.setattr(
        survey_routes,
        "store_survey_response",
        Mock(side_effect=survey_routes.SurveyStoreUnavailable()),
    )

    response = client.post(
        "/api/survey-responses",
        json={"campaignId": "campaign", "choice": "gis"},
    )

    assert response.status_code == 503
