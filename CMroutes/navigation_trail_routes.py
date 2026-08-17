import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from flask import Blueprint, jsonify, request

from .navigation_trail_store import (
    NavigationTrailStoreUnavailable,
    delete_navigation_trail,
    store_navigation_event,
)


navigation_trail_bp = Blueprint("navigation_trail", __name__)

SESSION_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
MAX_URL_LENGTH = 8192


def _error(message, status):
    return jsonify({"error": message}), status


def _valid_session_id(session_id):
    return bool(SESSION_ID_PATTERN.fullmatch(session_id))


def _is_internal_url(url):
    parsed = urlparse(url)
    configured_host = urlparse(os.getenv("CATMAPPER_FRONTEND_URL", "")).netloc.lower()
    allowed_hosts = {"catmapper.org", "dev.catmapper.org"}
    if configured_host:
        allowed_hosts.add(configured_host)
    return parsed.scheme in {"http", "https"} and parsed.netloc.lower() in allowed_hosts


@navigation_trail_bp.route("/api/navigation-trail/events", methods=["POST"])
def create_navigation_event():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return _error("A JSON object is required", 400)

    session_id = str(payload.get("sessionId", "")).strip()
    url = str(payload.get("url", "")).strip()
    occurred_at = str(payload.get("occurredAt", "")).strip()
    if not _valid_session_id(session_id):
        return _error("Invalid sessionId", 400)
    if not url or len(url) > MAX_URL_LENGTH:
        return _error(f"url must be between 1 and {MAX_URL_LENGTH} characters", 400)
    if not _is_internal_url(url):
        return _error("url must be a CatMapper URL", 400)
    if not occurred_at:
        return _error("occurredAt is required", 400)

    event = {
        "sessionId": session_id,
        "url": url,
        "occurredAt": occurred_at,
        "recordedAt": datetime.now(timezone.utc).isoformat(),
    }
    try:
        store_navigation_event(event)
    except NavigationTrailStoreUnavailable:
        return _error("Navigation trail storage is temporarily unavailable", 503)
    return jsonify({"status": "recorded"}), 201


@navigation_trail_bp.route("/api/navigation-trail/<session_id>", methods=["DELETE"])
def remove_navigation_trail(session_id):
    if not _valid_session_id(session_id):
        return _error("Invalid sessionId", 400)
    try:
        delete_navigation_trail(session_id)
    except NavigationTrailStoreUnavailable:
        return _error("Navigation trail storage is temporarily unavailable", 503)
    return "", 204
