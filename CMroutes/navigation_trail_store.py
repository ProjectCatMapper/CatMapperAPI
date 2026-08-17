import os
import time

from .survey_store import deployment_environment
from .task_store import get_redis_connection


DEFAULT_RETENTION_DAYS = 30
DEFAULT_MAX_EVENTS = 500


class NavigationTrailStoreUnavailable(RuntimeError):
    pass


def _positive_int_from_env(name, default):
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _stream_key(session_id):
    return f"catmapper:navigation-trail:{deployment_environment()}:{session_id}"


def store_navigation_event(event):
    connection = get_redis_connection()
    if connection is None:
        raise NavigationTrailStoreUnavailable("Navigation trail storage is unavailable")

    retention_days = _positive_int_from_env(
        "CATMAPPER_NAVIGATION_TRAIL_RETENTION_DAYS", DEFAULT_RETENTION_DAYS
    )
    retention_seconds = retention_days * 24 * 60 * 60
    max_events = _positive_int_from_env(
        "CATMAPPER_NAVIGATION_TRAIL_MAX_EVENTS", DEFAULT_MAX_EVENTS
    )
    try:
        pipeline = connection.pipeline()
        pipeline.xadd(
            _stream_key(event["sessionId"]),
            {
                "url": event["url"],
                "occurredAt": event["occurredAt"],
                "recordedAt": event["recordedAt"],
            },
            maxlen=max_events,
            approximate=True,
        )
        pipeline.expire(_stream_key(event["sessionId"]), retention_seconds)
        pipeline.execute()
    except Exception as exc:
        raise NavigationTrailStoreUnavailable("Navigation trail storage is unavailable") from exc


def delete_navigation_trail(session_id):
    connection = get_redis_connection()
    if connection is None:
        raise NavigationTrailStoreUnavailable("Navigation trail storage is unavailable")
    try:
        connection.delete(_stream_key(session_id))
    except Exception as exc:
        raise NavigationTrailStoreUnavailable("Navigation trail storage is unavailable") from exc
