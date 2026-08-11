import os
import time

from .task_store import get_redis_connection


DEFAULT_RETENTION_DAYS = 180
DEFAULT_MAX_RESPONSES = 10000


class SurveyStoreUnavailable(RuntimeError):
    pass


def deployment_environment():
    configured = os.getenv("CATMAPPER_DEPLOYMENT_ENV", "").strip().lower()
    if configured:
        return configured
    frontend_url = os.getenv("CATMAPPER_FRONTEND_URL", "").lower()
    return "dev" if "dev.catmapper.org" in frontend_url else "production"


def _stream_key():
    return f"catmapper:survey-responses:{deployment_environment()}"


def _positive_int_from_env(name, default):
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def store_survey_response(response):
    connection = get_redis_connection()
    if connection is None:
        raise SurveyStoreUnavailable("Survey response storage is unavailable")

    retention_days = _positive_int_from_env(
        "CATMAPPER_SURVEY_RETENTION_DAYS", DEFAULT_RETENTION_DAYS
    )
    retention_seconds = retention_days * 24 * 60 * 60
    max_responses = _positive_int_from_env(
        "CATMAPPER_SURVEY_MAX_RESPONSES", DEFAULT_MAX_RESPONSES
    )
    cutoff_ms = int((time.time() - retention_seconds) * 1000)
    key = _stream_key()
    fields = {
        "responseId": response["responseId"],
        "campaignId": response["campaignId"],
        "choice": response["choice"],
        "otherText": response.get("otherText", ""),
        "submittedAt": response["submittedAt"],
    }

    try:
        pipeline = connection.pipeline()
        pipeline.xtrim(key, minid=f"{cutoff_ms}-0", approximate=False)
        pipeline.xadd(key, fields, maxlen=max_responses, approximate=True)
        pipeline.expire(key, retention_seconds)
        pipeline.execute()
    except Exception as exc:
        raise SurveyStoreUnavailable("Survey response storage is unavailable") from exc
