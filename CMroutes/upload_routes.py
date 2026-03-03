from flask import Blueprint, request, jsonify
import pandas as pd
import json
import os
import threading
import math

from CM import unlist
from .auth_utils import verify_request_auth, classify_auth_error_status
from .task_store import get_task_store, DEFAULT_UPLOAD_BATCH_SIZE
from .task_queue import enqueue_upload_task, is_rq_enabled
from .upload_jobs import run_upload_task

upload_bp = Blueprint('upload', __name__)

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


def _request_acting_user(data):
    credentials = unlist(data.get("cred"))
    claims = verify_request_auth(credentials=credentials, req=request)
    acting_user = claims.get("userid") or "unknown"

    requested_user = data.get("user")
    if requested_user is not None and str(requested_user).strip():
        if str(requested_user).strip() != str(acting_user):
            raise Exception("User does not match authenticated API key/token owner")

    return acting_user


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
    Key = formData["keyColumn"]

    optionalProperties = data.get("allContext") or []
    addoptions = data.get("addoptions") or {}
    addDistrict = bool(addoptions.get("district"))
    addRecordYear = bool(addoptions.get("recordyear"))
    mergingType = data.get("mergingType")

    if data.get("so") == "standard":
        uploadOption = data.get("ao")
        dataset_payload = df
        total_rows = len(pd.DataFrame(df))
        job_args = {
            "dataset": dataset_payload,
            "database": database,
            "uploadOption": uploadOption,
            "formatKey": False,
            "optionalProperties": optionalProperties,
            "user": acting_user,
            "addDistrict": addDistrict,
            "addRecordYear": addRecordYear,
            "mergingType": mergingType,
            "geocode": False,
            "batchSize": UPLOAD_BATCH_SIZE,
        }
        return job_args, total_rows, database

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

    df.rename(columns={CMName: "CMName", CMID: "CMID", Name: "Name", Key: "Key"}, inplace=True)
    dataset_payload = df.to_dict(orient='records')

    job_args = {
        "dataset": dataset_payload,
        "database": database,
        "uploadOption": "add_uses",
        "formatKey": True,
        "optionalProperties": optionalProperties,
        "user": acting_user,
        "addDistrict": False,
        "addRecordYear": False,
        "geocode": False,
        "batchSize": UPLOAD_BATCH_SIZE,
    }
    return job_args, len(df), database


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
            enqueue_upload_task(task_id)
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
        status_code = classify_auth_error_status(error_message) or 500
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
        status_code = classify_auth_error_status(error_message) or 500
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

        _request_upload_cancel(str(task_id))
        task = _get_upload_task(str(task_id), cursor=cursor)
        return jsonify(task), 202
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 500
        return jsonify({"error": error_message}), status_code


@upload_bp.route("/uploadInputNodes", methods=['GET', 'POST'])
def upload_API():
    acting_user = "unknown"
    try:
        data = _request_json_payload()
        acting_user = _request_acting_user(data)

        job_args, total_rows, database = _prepare_upload_job(data, acting_user)
        task_id = _start_upload_task(
            job_args=job_args,
            user=acting_user,
            database=database,
            total_rows=total_rows,
        )

        total_batches = math.ceil(total_rows / UPLOAD_BATCH_SIZE) if total_rows > 0 else 0
        return jsonify(
            {
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
        ), 202

    except Exception as e:
        error_message = str(e)
        log_file = f'log/{acting_user}uploadProgress.txt'
        full_log = []
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                full_log = file.readlines()
        else:
            full_log.append("Log file not found.")

        response_data = {
            "error": f"Upload error - {error_message}",
            "full_log": full_log
        }

        status_code = classify_auth_error_status(error_message) or 500
        return jsonify(response_data), status_code
