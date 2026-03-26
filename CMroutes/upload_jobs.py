import threading

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


class UploadCancelledError(Exception):
    pass


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
        message = str(err)
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
