import threading
import uuid

import pandas as pd

from CM import (
    input_Nodes_Uses,
    waitingUSES,
    set_upload_log_listener,
    clear_upload_log_listener,
    set_query_cancel_checker,
    clear_query_cancel_checker,
    QueryCancelledError,
)

from .task_queue import enqueue_waiting_uses_task, is_rq_enabled
from .task_store import get_task_store
from .upload_error_utils import extract_upload_error_details
from CM.geojson_upload import (
    GeoJSONUploadError,
    apply_geojson_upload,
    delete_preflight_token,
)


class UploadCancelledError(Exception):
    pass


_DEPRECATED_UPLOAD_JOB_KEYS = {"contributionId"}


def _normalize_upload_job_args(job_args):
    """Remove fields persisted by older upload producers."""
    return {
        key: value
        for key, value in job_args.items()
        if key not in _DEPRECATED_UPLOAD_JOB_KEYS
    }


def _humanize_upload_error(err):
    if isinstance(err, KeyError):
        missing = str(err).strip().strip("'\"")
        if missing:
            return (
                f"Missing required column or mapping: '{missing}'. "
                "Check your selected CMID/CMName/Name/Key column mappings and uploaded headers."
            )
        return "Missing required column or mapping in upload data."
    return str(err)


def _task_user(task):
    if not isinstance(task, dict):
        return None
    return task.get("user")


def _run_waiting_uses_inline(waiting_task_id, database):
    thread = threading.Thread(
        target=run_waiting_uses_task,
        args=(waiting_task_id, database),
        daemon=True,
        name=f"waitingUSES-{waiting_task_id[:8]}",
    )
    thread.start()


def run_upload_task(task_id):
    store = get_task_store()
    task = store.get_upload_task(task_id, cursor=0)
    if task is None:
        return
    if str(task.get("status", "")).lower() in {"completed", "failed", "canceled"}:
        store.delete_upload_job_payload(task_id)
        return

    user = _task_user(task)
    database = task.get("database")
    job_args = store.get_upload_job_payload(task_id)
    if not isinstance(job_args, dict):
        store.fail_upload_task(task_id, "Upload job payload is missing.")
        return
    job_args = _normalize_upload_job_args(job_args)

    def _raise_if_cancelled():
        if store.is_upload_cancel_requested(task_id):
            raise QueryCancelledError("Upload cancelled by user request.")

    def _upload_log_listener(message):
        store.append_upload_event(task_id, message)
        normalized_message = str(message).strip().lower()
        if normalized_message.endswith("end of batch"):
            store.increment_upload_batch(task_id)
        _raise_if_cancelled()

    if store.is_upload_cancel_requested(task_id):
        store.cancel_upload_task(task_id, "Upload cancelled before starting.")
        store.delete_upload_job_payload(task_id)
        return
    store.mark_upload_running(task_id)
    set_upload_log_listener(_upload_log_listener)
    set_query_cancel_checker(_raise_if_cancelled)
    try:
        _raise_if_cancelled()
        response, desired_order = input_Nodes_Uses(**job_args)
        if not isinstance(response, pd.DataFrame):
            raise RuntimeError("Upload did not return a table result.")

        n = len(response)
        response_dict = response.to_dict(orient="records")
        waiting_task_id = store.create_waiting_task(
            user=user,
            database=database,
            upload_task_id=task_id,
        )
        store.complete_upload_task(
            task_id=task_id,
            message=f"Upload completed for {n} row(s)",
            result_file=response_dict,
            result_order=desired_order,
            waiting_task_id=waiting_task_id,
        )

        if is_rq_enabled():
            enqueue_waiting_uses_task(waiting_task_id, database)
        else:
            _run_waiting_uses_inline(waiting_task_id, database)
    except (UploadCancelledError, QueryCancelledError) as err:
        store.cancel_upload_task(task_id, str(err))
    except Exception as err:
        message = _humanize_upload_error(err)
        details = extract_upload_error_details(message)
        store.fail_upload_task(task_id, message, error_details=details)
    finally:
        clear_upload_log_listener()
        clear_query_cancel_checker()
        store.delete_upload_job_payload(task_id)


def run_waiting_uses_task(waiting_task_id, database=None):
    store = get_task_store()
    task = store.get_waiting_task(waiting_task_id)
    if task is None:
        return

    database = database or task.get("database")
    store.mark_waiting_running(waiting_task_id)
    try:
        result = waitingUSES(database)
        if isinstance(result, tuple) and len(result) == 2 and result[1] == 500:
            raise RuntimeError(str(result[0]))
        store.complete_waiting_task(waiting_task_id, str(result))
    except Exception as err:
        store.fail_waiting_task(waiting_task_id, str(err))


def run_geojson_upload_task(task_id):
    """Apply one staged polygon file using the shared upload task store."""
    store = get_task_store()
    task = store.get_upload_task(task_id, cursor=0)
    if task is None:
        return
    payload = store.get_upload_job_payload(task_id)
    if not isinstance(payload, dict) or payload.get("kind") != "geojson_polygon":
        store.fail_upload_task(task_id, "Polygon upload job payload is missing.")
        return
    token = payload.get("token")

    def _cancelled():
        return store.is_upload_cancel_requested(task_id)

    if _cancelled():
        store.cancel_upload_task(task_id, "Polygon upload cancelled before starting.")
        store.delete_upload_job_payload(task_id)
        delete_preflight_token(token)
        return

    store.mark_upload_running(task_id)
    upload_id = f"geojson_{uuid.uuid4().hex}"
    try:
        result = apply_geojson_upload(
            payload["path"],
            payload["database"],
            payload["actorClaims"],
            expected_digest=payload["expectedDigest"],
            replace_existing=bool(payload.get("replaceExisting")),
            upload_id=upload_id,
            cancelled=_cancelled,
            log=lambda message: store.append_upload_event(task_id, message),
        )
        store.complete_upload_task(
            task_id,
            f"Polygon upload completed for {result['featureCount']} feature(s).",
            [result],
            ["featureCount", "geometryNodes", "usesLinks", "uploadID"],
        )
    except GeoJSONUploadError as err:
        if _cancelled() and "cancel" in str(err).lower():
            store.cancel_upload_task(task_id, str(err))
        else:
            store.fail_upload_task(task_id, str(err), error_details=err.details)
    except Exception as err:
        store.fail_upload_task(task_id, str(err))
    finally:
        store.delete_upload_job_payload(task_id)
        delete_preflight_token(token)
