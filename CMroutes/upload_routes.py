from flask import Blueprint, request, jsonify
import pandas as pd
import json
import os
import threading
import math
from datetime import datetime, timezone

from CM import unlist
from .auth_utils import verify_request_auth, classify_auth_error_status
from .task_store import get_task_store, DEFAULT_UPLOAD_BATCH_SIZE
from .task_queue import enqueue_upload_task, is_rq_enabled
from .upload_jobs import run_upload_task
from .upload_error_utils import extract_upload_error_details

try:
    from rq.command import send_stop_job_command
except Exception:  # pragma: no cover
    send_stop_job_command = None

upload_bp = Blueprint('upload', __name__)
API_HELP_URL = "https://help.catmapper.org/API.html"

# Compatibility placeholders for tests/tools that introspect module attributes.
UPLOAD_TASKS = {}
UPLOAD_TASKS_LOCK = threading.Lock()
WAITING_USES_TASKS = {}
WAITING_USES_TASKS_LOCK = threading.Lock()

UPLOAD_BATCH_SIZE = DEFAULT_UPLOAD_BATCH_SIZE


def _cursor_from_payload(data):
    try:
        return max(int(data.get("cursor", 0) or 0), 0)
    except (TypeError, ValueError):
        return 0


def _request_json_payload():
    data = request.get_json(silent=True)
    if data is None:
        raw = request.get_data(as_text=True)
        data = json.loads(raw) if raw else {}
    if not isinstance(data, dict):
        raise Exception("Invalid payload")
    return data


def _build_auth_error_response(error_message, fallback_status=500):
    error_message = str(error_message or "").strip()
    status_code = classify_auth_error_status(error_message) or fallback_status
    if status_code in (401, 403):
        clear_error = (
            f"Not authorized: {error_message or 'missing or invalid credentials'}. "
            f"Provide a valid Bearer token or X-API-Key. See {API_HELP_URL}"
        )
        return jsonify(
            {
                "error": clear_error,
                "detail": error_message,
                "help": API_HELP_URL,
                "code": "NOT_AUTHORIZED",
            }
        ), status_code
    return None, status_code


def _request_acting_user(data):
    credentials = unlist(data.get("cred"))
    claims = verify_request_auth(credentials=credentials, req=request)
    acting_user = claims.get("userid") or "unknown"

    requested_user = data.get("user")
    if requested_user is not None and str(requested_user).strip():
        if str(requested_user).strip() != str(acting_user):
            raise Exception("User does not match authenticated API key/token owner")

    return acting_user


def _coerce_property_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if v not in (None, "")]
    if isinstance(value, tuple):
        return [v for v in value if v not in (None, "")]
    return [value] if value not in (None, "") else []


def _resolve_optional_properties(data):
    warnings = []
    optional_properties = _coerce_property_list(data.get("optionalProperties"))
    if optional_properties:
        return optional_properties, warnings

    all_context = _coerce_property_list(data.get("allContext"))
    if all_context and "allContext" in data and "optionalProperties" not in data:
        warnings.append(
            "`allContext` is deprecated for upload property selection; use `optionalProperties`."
        )
    return all_context, warnings


def _validate_simple_key_values(df, key_column):
    key_columns = key_column if isinstance(key_column, list) else [key_column]
    key_columns = [str(col).strip() for col in key_columns if str(col).strip()]
    if not key_columns:
        raise Exception("Simple upload requires at least one key column.")

    for col in key_columns:
        if col not in df.columns:
            raise Exception(f"Simple upload requires key column '{col}' in payload rows.")
        key_series = df[col].fillna("").astype(str)
        bad_rows = key_series[key_series.str.contains(r"==", regex=True)].index.tolist()
        if bad_rows:
            bad_rows = [i + 1 for i in bad_rows]
            raise Exception(
                f"Simple upload expects raw key values without '=='. Rows {bad_rows} in key column '{col}' include preformatted keys; use so='standard'."
            )


def _resolve_simple_key_columns(form_data):
    key_columns = form_data.get("keyColumns", [])
    if not isinstance(key_columns, list):
        key_columns = [key_columns] if key_columns else []
    key_columns = [str(col).strip() for col in key_columns if str(col).strip()]

    # Backward compatibility: support legacy single keyColumn payload.
    legacy_key = str(form_data.get("keyColumn", "")).strip()
    if legacy_key:
        key_columns.append(legacy_key)

    deduped = []
    seen = set()
    for col in key_columns:
        if col in seen:
            continue
        seen.add(col)
        deduped.append(col)
    return deduped


def _compose_simple_key(df, key_columns):
    if not key_columns:
        raise Exception("Simple upload requires at least one key column.")

    for col in key_columns:
        if col not in df.columns:
            raise Exception(f"Simple upload requires key column '{col}' in payload rows.")

    def _expr(row):
        parts = []
        for col in key_columns:
            raw = row.get(col)
            if pd.isna(raw):
                continue
            text = str(raw).strip()
            if not text:
                continue
            parts.append(f"{col} == {text}")
        return " && ".join(parts)

    df["Key"] = df.apply(_expr, axis=1)
    blank_rows = df.index[df["Key"].fillna("").astype(str).str.strip().eq("")].tolist()
    if blank_rows:
        rows_1_based = [i + 1 for i in blank_rows]
        raise Exception(
            f"Simple upload requires at least one non-empty key value per row across selected key columns. "
            f"Rows {rows_1_based} have no key values."
        )


def _prepare_upload_job(data, acting_user):
    df = data.get("df")
    database = unlist(data.get("database"))
    formData = unlist(data.get("formData"))
    if not isinstance(formData, dict):
        raise Exception("Invalid formData")

    label = formData.get("subdomain") or formData.get("domain")
    label_upper = str(label).upper() if label is not None else ""
    if label_upper == "ANY DOMAIN":
        label = "CATEGORY"
    if label_upper == "AREA":
        label = "DISTRICT"

    datasetID = formData["datasetID"]
    CMName = formData["cmNameColumn"]
    Name = formData["categoryNamesColumn"]
    altNamesColumns = formData.get("alternateCategoryNamesColumns", [])
    if not isinstance(altNamesColumns, list):
        altNamesColumns = [altNamesColumns] if altNamesColumns else []
    altNamesColumns = [col for col in altNamesColumns if col]
    altNames = formData.get("alternateCategoryNamesColumn", "")
    CMID = formData["cmidColumn"]
    key_columns = _resolve_simple_key_columns(formData)

    optionalProperties, warnings = _resolve_optional_properties(data)
    addoptions = data.get("addoptions") or {}
    addDistrict = bool(addoptions.get("district"))
    addRecordYear = bool(addoptions.get("recordyear"))
    mergingType = data.get("mergingType")
    so = str(data.get("so") or "standard").strip().lower()
    upload_option = str(data.get("ao") or "").strip()

    if so not in {"standard", "simple"}:
        raise Exception("`so` must be either 'standard' or 'simple'.")

    if so == "standard":
        dataset_payload = df
        total_rows = len(pd.DataFrame(df))
        job_args = {
            "dataset": dataset_payload,
            "database": database,
            "uploadOption": upload_option,
            "formatKey": False,
            "optionalProperties": optionalProperties,
            "user": acting_user,
            "addDistrict": addDistrict,
            "addRecordYear": addRecordYear,
            "mergingType": mergingType,
            "geocode": False,
            "batchSize": UPLOAD_BATCH_SIZE,
            "ignoreIfSame": bool(data.get("ignore_if_same", False)),
        }
        return job_args, total_rows, database, warnings

    if not label:
        raise Exception("Must specify a domain")

    df = pd.DataFrame(df)
    df['label'] = label
    df['datasetID'] = datasetID

    if Name not in df.columns:
        df['Name'] = df[CMName]
        Name = "Name"

    if CMID not in df.columns:
        df['CMID'] = ""
        CMID = "CMID"

    if altNamesColumns:
        existing_alt_cols = [col for col in altNamesColumns if col in df.columns]
        if existing_alt_cols:
            def _combine_alt_names(row):
                values = []
                for col in existing_alt_cols:
                    raw = row.get(col)
                    if pd.isna(raw):
                        continue
                    text = str(raw).strip()
                    if text:
                        values.append(text)
                return ";".join(values)

            df["altNames"] = df.apply(_combine_alt_names, axis=1)
    elif altNames and altNames in df.columns:
        df.rename(columns={altNames: "altNames"}, inplace=True)

    _validate_simple_key_values(df, key_columns)
    _compose_simple_key(df, key_columns)
    df.rename(columns={CMName: "CMName", CMID: "CMID", Name: "Name"}, inplace=True)
    dataset_payload = df.to_dict(orient='records')

    job_args = {
        "dataset": dataset_payload,
        "database": database,
        "uploadOption": "add_uses",
        "formatKey": False,
        "optionalProperties": optionalProperties,
        "user": acting_user,
        "addDistrict": False,
        "addRecordYear": False,
        "geocode": False,
        "batchSize": UPLOAD_BATCH_SIZE,
        "ignoreIfSame": bool(data.get("ignore_if_same", False)),
    }
    return job_args, len(df), database, warnings


def _start_upload_task(job_args, user, database, total_rows):
    store = get_task_store()
    task_id = store.create_upload_task(
        user=user,
        database=database,
        total_rows=total_rows,
        batch_size=UPLOAD_BATCH_SIZE,
    )
    store.set_upload_job_payload(task_id, job_args)

    try:
        if is_rq_enabled():
            job = enqueue_upload_task(task_id)
            if job is not None and getattr(job, "id", None):
                store.set_upload_rq_job_id(task_id, job.id)
        else:
            thread = threading.Thread(
                target=run_upload_task,
                args=(task_id,),
                daemon=True,
                name=f"upload-{task_id[:8]}",
            )
            thread.start()
    except Exception as err:
        store.fail_upload_task(task_id, str(err))
        raise

    return task_id


def _get_upload_task(task_id, cursor=0):
    store = get_task_store()
    return store.get_upload_task(task_id, cursor=cursor)


def _get_waiting_uses_task(task_id):
    store = get_task_store()
    return store.get_waiting_task(task_id)


def _request_upload_cancel(task_id):
    store = get_task_store()
    return store.request_upload_cancel(task_id)


def _send_cancel_to_rq(task_id, task):
    if not is_rq_enabled():
        return False, "rq-disabled"

    store = get_task_store()
    connection = getattr(store, "redis", None)
    if connection is None:
        return False, "no-redis-connection"

    rq_job_id = store.get_upload_rq_job_id(task_id)
    if not rq_job_id:
        return False, "missing-rq-job-id"

    status = str(task.get("status") or "").strip().lower()
    queue_name = os.getenv("CATMAPPER_UPLOAD_QUEUE", "catmapper-upload")
    rq_job_key = f"rq:job:{rq_job_id}"
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        if status == "queued":
            connection.lrem(f"rq:queue:{queue_name}", 0, rq_job_id)
            connection.zrem(f"rq:wip:{queue_name}", rq_job_id)
            connection.hset(
                rq_job_key,
                mapping={
                    "status": "canceled",
                    "ended_at": now_iso,
                },
            )
            return True, "removed-queued-job"

        if status == "running":
            if send_stop_job_command is None:
                return False, "rq-stop-not-available"
            send_stop_job_command(connection, rq_job_id)
            return True, "sent-stop-signal"
    except Exception as err:
        return False, str(err)

    return False, "no-op"


def _cancel_upload_task(task_id, task):
    store = get_task_store()
    _request_upload_cancel(task_id)

    status = str(task.get("status") or "").strip().lower()
    stopped, action = _send_cancel_to_rq(task_id, task)

    if status == "queued":
        store.cancel_upload_task(task_id, "Upload cancelled before starting.")
        store.delete_upload_job_payload(task_id)
        if stopped:
            store.append_upload_event(task_id, "Queued job removed from queue.")
        return

    if status == "running" and stopped and action == "sent-stop-signal":
        store.append_upload_event(task_id, "Stop signal sent to worker.")


@upload_bp.route("/uploadWaitingUSESStatus", methods=["POST"])
def upload_waiting_uses_status():
    try:
        data = _request_json_payload()
        credentials = unlist(data.get("cred"))
        claims = verify_request_auth(credentials=credentials, req=request)
        acting_user = claims.get("userid") or "unknown"

        requested_user = unlist(data.get("user"))
        if requested_user and str(requested_user).strip() != str(acting_user):
            raise Exception("User does not match authenticated API key/token owner")

        task_id = unlist(data.get("taskId"))
        if not task_id:
            raise Exception("taskId not specified")

        task = _get_waiting_uses_task(str(task_id))
        if task is None:
            return jsonify({"error": "Task not found"}), 404
        if str(task.get("user")) != str(acting_user):
            raise Exception("User does not match authenticated API key/token owner")

        return jsonify(task)
    except Exception as e:
        error_message = str(e)
        auth_response, status_code = _build_auth_error_response(error_message, fallback_status=500)
        if auth_response is not None:
            return auth_response, status_code
        return jsonify({"error": error_message}), status_code


@upload_bp.route("/uploadInputNodesStatus", methods=["POST"])
def upload_input_nodes_status():
    try:
        data = _request_json_payload()
        acting_user = _request_acting_user(data)

        task_id = unlist(data.get("taskId"))
        if not task_id:
            raise Exception("taskId not specified")

        cursor = _cursor_from_payload(data)
        task = _get_upload_task(str(task_id), cursor=cursor)
        if task is None:
            return jsonify({"error": "Task not found"}), 404
        if str(task.get("user")) != str(acting_user):
            raise Exception("User does not match authenticated API key/token owner")

        return jsonify(task)
    except Exception as e:
        error_message = str(e)
        auth_response, status_code = _build_auth_error_response(error_message, fallback_status=500)
        if auth_response is not None:
            return auth_response, status_code
        return jsonify({"error": error_message}), status_code


@upload_bp.route("/uploadInputNodesCancel", methods=["POST"])
def upload_input_nodes_cancel():
    try:
        data = _request_json_payload()
        acting_user = _request_acting_user(data)

        task_id = unlist(data.get("taskId"))
        if not task_id:
            raise Exception("taskId not specified")

        cursor = _cursor_from_payload(data)
        task = _get_upload_task(str(task_id), cursor=cursor)
        if task is None:
            return jsonify({"error": "Task not found"}), 404
        if str(task.get("user")) != str(acting_user):
            raise Exception("User does not match authenticated API key/token owner")

        _cancel_upload_task(str(task_id), task)
        task = _get_upload_task(str(task_id), cursor=cursor)
        return jsonify(task), 202
    except Exception as e:
        error_message = str(e)
        auth_response, status_code = _build_auth_error_response(error_message, fallback_status=500)
        if auth_response is not None:
            return auth_response, status_code
        return jsonify({"error": error_message}), status_code


@upload_bp.route("/uploadInputNodes", methods=['GET', 'POST'])
def upload_API():
    acting_user = "unknown"
    try:
        data = _request_json_payload()
        acting_user = _request_acting_user(data)

        job_args, total_rows, database, warnings = _prepare_upload_job(data, acting_user)
        task_id = _start_upload_task(
            job_args=job_args,
            user=acting_user,
            database=database,
            total_rows=total_rows,
        )

        total_batches = math.ceil(total_rows / UPLOAD_BATCH_SIZE) if total_rows > 0 else 0
        response = {
            "taskId": task_id,
            "status": "queued",
            "progress": {
                "batchSize": UPLOAD_BATCH_SIZE,
                "totalRows": total_rows,
                "totalBatches": total_batches,
                "completedBatches": 0,
                "percent": 0,
            },
            "queue": "rq" if is_rq_enabled() else "thread",
        }
        if warnings:
            response["warnings"] = warnings
        return jsonify(response), 202

    except Exception as e:
        error_message = str(e)
        log_file = f'log/{acting_user}uploadProgress.txt'
        full_log = []
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                full_log = file.readlines()
        else:
            full_log.append("Log file not found.")

        auth_response, status_code = _build_auth_error_response(error_message, fallback_status=500)
        if auth_response is not None:
            auth_payload = auth_response.get_json() or {}
            auth_payload["full_log"] = full_log
            return jsonify(auth_payload), status_code

        response_data = {
            "error": f"Upload error - {error_message}",
            "full_log": full_log,
            "error_details": extract_upload_error_details(error_message),
        }
        return jsonify(response_data), status_code
