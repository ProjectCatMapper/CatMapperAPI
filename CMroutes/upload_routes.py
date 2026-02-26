from flask import Blueprint, request, jsonify
import pandas as pd
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from CM import input_Nodes_Uses, unlist, waitingUSES
from .auth_utils import verify_request_auth, classify_auth_error_status

upload_bp = Blueprint('upload', __name__)
WAITING_USES_TASKS = {}
WAITING_USES_TASKS_LOCK = threading.Lock()
WAITING_USES_TASK_RETENTION_SECONDS = int(
    os.getenv("CATMAPPER_WAITING_USES_TASK_RETENTION_SECONDS", "86400")
)


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _prune_waiting_uses_tasks(now_ts=None):
    now_ts = now_ts if now_ts is not None else time.time()
    stale_task_ids = []

    for task_id, task in WAITING_USES_TASKS.items():
        finished_at_ts = task.get("finishedAtTs")
        if finished_at_ts is None:
            continue
        if now_ts - finished_at_ts > WAITING_USES_TASK_RETENTION_SECONDS:
            stale_task_ids.append(task_id)

    for task_id in stale_task_ids:
        WAITING_USES_TASKS.pop(task_id, None)


def _start_waiting_uses_task(database, user):
    task_id = uuid.uuid4().hex
    created_at = _utc_now_iso()
    created_ts = time.time()

    with WAITING_USES_TASKS_LOCK:
        _prune_waiting_uses_tasks(now_ts=created_ts)
        WAITING_USES_TASKS[task_id] = {
            "taskId": task_id,
            "status": "queued",
            "user": str(user),
            "database": str(database),
            "createdAt": created_at,
            "startedAt": None,
            "finishedAt": None,
            "message": None,
            "error": None,
            "finishedAtTs": None,
        }

    def _run_waiting_uses():
        started_at = _utc_now_iso()
        with WAITING_USES_TASKS_LOCK:
            task = WAITING_USES_TASKS.get(task_id)
            if task is None:
                return
            task["status"] = "running"
            task["startedAt"] = started_at

        try:
            result = waitingUSES(database)
            if isinstance(result, tuple) and len(result) == 2 and result[1] == 500:
                raise RuntimeError(str(result[0]))

            finished_at = _utc_now_iso()
            finished_ts = time.time()
            with WAITING_USES_TASKS_LOCK:
                task = WAITING_USES_TASKS.get(task_id)
                if task is None:
                    return
                task["status"] = "completed"
                task["finishedAt"] = finished_at
                task["finishedAtTs"] = finished_ts
                task["message"] = str(result)
                task["error"] = None
        except Exception as err:
            finished_at = _utc_now_iso()
            finished_ts = time.time()
            with WAITING_USES_TASKS_LOCK:
                task = WAITING_USES_TASKS.get(task_id)
                if task is None:
                    return
                task["status"] = "failed"
                task["finishedAt"] = finished_at
                task["finishedAtTs"] = finished_ts
                task["message"] = None
                task["error"] = str(err)

    thread = threading.Thread(
        target=_run_waiting_uses,
        daemon=True,
        name=f"waitingUSES-{task_id[:8]}",
    )
    thread.start()
    return task_id


def _get_waiting_uses_task(task_id):
    with WAITING_USES_TASKS_LOCK:
        _prune_waiting_uses_tasks()
        task = WAITING_USES_TASKS.get(task_id)
        if task is None:
            return None
        response_task = dict(task)
        response_task.pop("finishedAtTs", None)
        return response_task


@upload_bp.route("/uploadWaitingUSESStatus", methods=["POST"])
def upload_waiting_uses_status():
    try:
        data = request.get_json(silent=True)
        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise Exception("Invalid payload")

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

@upload_bp.route("/uploadInputNodes", methods=['GET', 'POST'])
def upload_API():
    acting_user = "unknown"
    try:
        data = request.get_json(silent=True)
        if data is None:
            raw = request.get_data(as_text=True)
            data = json.loads(raw) if raw else {}
        if not isinstance(data, dict):
            raise Exception("Invalid payload")

        credentials = unlist(data.get("cred"))
        claims = verify_request_auth(credentials=credentials, req=request)
        acting_user = claims.get("userid") or "unknown"
        requested_user = data.get("user")
        if requested_user is not None and str(requested_user).strip():
            if str(requested_user).strip() != str(acting_user):
                raise Exception("User does not match authenticated API key/token owner")

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

            dfpd = pd.DataFrame(df)
            required = ["CMName", "Name", "CMID",
                        "label", "altNames", "Key", "datasetID"]
            key_cols = {}
            for key in required:
                if key in dfpd.columns.to_list():
                    key_cols[key] = key
                else:
                    key_cols[key] = None

            response, desired_order = input_Nodes_Uses(
                dataset=df,
                database=database,
                uploadOption=uploadOption,
                formatKey=False,
                optionalProperties=optionalProperties,
                user=acting_user,
                addDistrict=addDistrict,
                addRecordYear=addRecordYear,
                mergingType=mergingType,
                geocode=False,
                batchSize=1000)
        else:

            if not label:
                raise Exception("Must specify a domain")
            df = pd.DataFrame(df)
            df['label'] = label
            df['datasetID'] = datasetID
            if not Name in df.columns:
                df['Name'] = df[CMName]
                Name = "Name"
            if not CMID in df.columns:
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
            df = df.to_dict(orient='records')
            # return {"Name":Name, "CMID":CMID,"altNames":altNames,"Key":Key,"user":user,"overwriteProperties":overwriteProperties,"updateProperties":updateProperties,"addDistrict":addDistrict,"addRecordYear":addRecordYear}
            response, desired_order = input_Nodes_Uses(
                dataset=df,
                database=database,
                uploadOption="add_uses",
                formatKey=True,
                optionalProperties=optionalProperties,
                user=acting_user,
                addDistrict=False,
                addRecordYear=False,
                geocode=False,
                batchSize=1000)

        if isinstance(response, pd.DataFrame):
            n = len(response)
            response_dict = response.to_dict(orient='records')
            waiting_uses_task_id = _start_waiting_uses_task(
                database=database,
                user=acting_user,
            )
            return jsonify(
                {
                    "message": f"Upload completed for {n} row(s)",
                    "file": response_dict,
                    "order": desired_order,
                    "waitingUsesTask": waiting_uses_task_id,
                    "waitingUsesStatus": "queued",
                }
            )
        # else:
        #     return "Error!! Check your file."

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
