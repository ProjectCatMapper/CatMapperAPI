import hashlib
import hmac
import os
import re
import secrets
import uuid
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from CM.email import get_default_sender, sendEmail

from .extensions import mail
from .survey_store import (
    SurveyStoreUnavailable,
    deployment_environment,
    get_redis_connection,
    store_survey_response,
)


survey_bp = Blueprint("survey", __name__)

ALLOWED_CHOICES = {"information", "data_tools", "gis", "cats", "other"}
CAMPAIGN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,80}$")
MAX_OTHER_TEXT_LENGTH = 1000
MAX_REQUEST_BYTES = 4096
RATE_LIMIT_REQUESTS = 10
RATE_LIMIT_SECONDS = 60 * 60
SURVEY_NOTIFICATION_RECIPIENT = "admin@catmapper.org"
_RATE_LIMIT_SECRET = os.getenv("CATMAPPER_SURVEY_RATE_LIMIT_SECRET", "").encode() or secrets.token_bytes(32)


def _rate_limit_key():
    remote_address = request.remote_addr or "unknown"
    digest = hmac.new(
        _RATE_LIMIT_SECRET,
        remote_address.encode("utf-8", errors="ignore"),
        hashlib.sha256,
    ).hexdigest()
    environment = deployment_environment()
    return f"catmapper:survey-rate:{environment}:{digest}"


def _rate_limit_exceeded():
    connection = get_redis_connection()
    if connection is None:
        return False
    key = _rate_limit_key()
    try:
        count = connection.incr(key)
        if count == 1:
            connection.expire(key, RATE_LIMIT_SECONDS)
        return count > RATE_LIMIT_REQUESTS
    except Exception:
        return False


def _error(message, status):
    return jsonify({"error": message}), status


def _send_survey_notification(response):
    body = (
        "A CatMapper survey response was submitted.\n\n"
        f"Response ID: {response['responseId']}\n"
        f"Campaign: {response['campaignId']}\n"
        f"Answer: {response['choice']}\n"
        f"Submitted at: {response['submittedAt']}\n"
        f"Further comments: {response.get('otherText') or '(none)'}\n"
    )
    return sendEmail(
        mail=mail,
        subject="New CatMapper survey response",
        recipients=[SURVEY_NOTIFICATION_RECIPIENT],
        body=body,
        sender=get_default_sender(),
    )


@survey_bp.route("/api/survey-responses", methods=["POST"])
def create_survey_response():
    if request.content_length and request.content_length > MAX_REQUEST_BYTES:
        return _error("Survey response is too large", 413)
    if _rate_limit_exceeded():
        return _error("Too many survey submissions. Please try again later.", 429)

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return _error("A JSON object is required", 400)

    campaign_id = str(payload.get("campaignId", "")).strip()
    choice = str(payload.get("choice", "")).strip()
    other_text = str(payload.get("otherText", "")).strip()

    if not CAMPAIGN_ID_PATTERN.fullmatch(campaign_id):
        return _error("Invalid campaignId", 400)
    if choice not in ALLOWED_CHOICES:
        return _error("Invalid survey choice", 400)
    if len(other_text) > MAX_OTHER_TEXT_LENGTH:
        return _error(
            f"otherText must be {MAX_OTHER_TEXT_LENGTH} characters or fewer",
            400,
        )
    if choice == "other" and not other_text:
        return _error("otherText is required when choice is other", 400)
    if choice != "other":
        other_text = ""

    response = {
        "responseId": str(uuid.uuid4()),
        "campaignId": campaign_id,
        "choice": choice,
        "otherText": other_text,
        "submittedAt": datetime.now(timezone.utc).isoformat(),
    }
    try:
        store_survey_response(response)
    except SurveyStoreUnavailable:
        return _error("Survey response storage is temporarily unavailable", 503)

    _send_survey_notification(response)

    return jsonify({"responseId": response["responseId"]}), 201
