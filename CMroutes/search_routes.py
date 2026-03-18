from flask import request, Blueprint, jsonify
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone

from CM import translate, unlist, search
from .task_store import get_redis_connection

search_bp = Blueprint('search', __name__)

_TRANSLATE_TASKS = {}
_TRANSLATE_TASKS_LOCK = threading.Lock()
_TRANSLATE_TASK_RETENTION_SECONDS = int(os.getenv("CATMAPPER_TRANSLATE_TASK_RETENTION_SECONDS", "7200"))
_NLP_PARSE_LOG_WRITE_LOCK = threading.Lock()
_DEFAULT_NLP_PARSE_LOG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "log", "nlp_parse_requests")
)


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _translate_task_key(task_id):
    return f"cm:translate:task:{task_id}"


def _sanitize_log_value(value, depth=0):
    if depth > 6:
        return "<max_depth>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:10000]
    if isinstance(value, list):
        return [_sanitize_log_value(item, depth + 1) for item in value[:200]]
    if isinstance(value, dict):
        clipped = {}
        for idx, (key, item) in enumerate(value.items()):
            if idx >= 200:
                break
            clipped[str(key)[:120]] = _sanitize_log_value(item, depth + 1)
        return clipped
    return str(value)[:2000]


def _safe_log_database_name(raw_database):
    value = str(raw_database or "").strip().lower()
    if re.fullmatch(r"[a-z0-9_-]{1,40}", value):
        return value
    return "unknown"


def _parse_contexts_query_args(req):
    values = []

    for raw in req.args.getlist("contexts"):
        if raw is None:
            continue
        values.extend(str(raw).split(","))

    raw_context = req.args.get("context")
    if raw_context is not None:
        values.append(raw_context)

    cleaned = [str(value).strip() for value in values if str(value).strip()]
    # preserve order while deduplicating
    return list(dict.fromkeys(cleaned))


def _load_translate_task(task_id):
    connection = get_redis_connection()
    if connection is not None:
        raw = connection.get(_translate_task_key(task_id))
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            return json.loads(raw)
        except Exception:
            return None

    with _TRANSLATE_TASKS_LOCK:
        task = _TRANSLATE_TASKS.get(task_id)
        return dict(task) if isinstance(task, dict) else None


def _save_translate_task(task):
    task_id = str(task.get("taskId") or "").strip()
    if not task_id:
        return

    connection = get_redis_connection()
    if connection is not None:
        key = _translate_task_key(task_id)
        connection.set(key, json.dumps(task))
        connection.expire(key, _TRANSLATE_TASK_RETENTION_SECONDS)
        return

    with _TRANSLATE_TASKS_LOCK:
        _TRANSLATE_TASKS[task_id] = dict(task)


def _update_translate_task(task_id, **updates):
    task = _load_translate_task(task_id)
    if task is None:
        return None
    task.update(updates)
    _save_translate_task(task)
    return task


def _translate_task_response(task):
    if not isinstance(task, dict):
        return None
    result = dict(task)
    result.pop("startedAtTs", None)
    return result


def _parse_translate_payload(payload):
    database = unlist(payload.get("database"))
    property_name = unlist(payload.get("property"))
    if property_name == "CatMapper ID (CMID)":
        property_name = "CMID"

    return {
        "database": database,
        "property": property_name,
        "domain": unlist(payload.get("domain")),
        "key": unlist(payload.get("key")),
        "term": unlist(payload.get("term")),
        "country": unlist(payload.get("country")),
        "context": unlist(payload.get("context")),
        "dataset": unlist(payload.get("dataset")),
        "yearStart": unlist(payload.get("yearStart")),
        "yearEnd": unlist(payload.get("yearEnd")),
        "query": unlist(payload.get("query")),
        "table": payload.get("table"),
        "countsamename": payload.get("countsamename"),
        "uniqueRows": payload.get("uniqueRows"),
    }


def _run_translate_task(task_id, translate_kwargs, batch_size):
    started_at_ts = time.time()
    _update_translate_task(
        task_id,
        startedAt=_utc_now_iso(),
        startedAtTs=started_at_ts,
        status="processing",
        message="Processing input...",
        percent=10,
        stage="preprocessing",
    )

    latest_progress = {"processedRows": 0, "totalRows": 0}

    def progress_callback(percent, message, processedRows=0, totalRows=0):
        latest_progress["processedRows"] = int(processedRows or 0)
        latest_progress["totalRows"] = int(totalRows or 0)
        _update_translate_task(
            task_id,
            status="processing",
            percent=max(0, min(100, int(percent))),
            message=str(message),
            stage="processing",
            processedRows=latest_progress["processedRows"],
            totalRows=latest_progress["totalRows"],
            elapsedSeconds=round(time.time() - started_at_ts, 1),
        )

    def cancel_checker():
        task = _load_translate_task(task_id)
        if not isinstance(task, dict):
            return False
        return bool(task.get("cancelRequested"))

    try:
        translate_result = translate(
            **translate_kwargs,
            progress_callback=progress_callback,
            batch_size=batch_size,
            cancel_checker=cancel_checker,
        )

        warnings = []
        if isinstance(translate_result, tuple):
            if len(translate_result) == 3:
                dataframe, desired_order, warnings = translate_result
            elif len(translate_result) == 2:
                dataframe, desired_order = translate_result
            else:
                raise Exception("translate returned unexpected tuple shape")
        else:
            raise Exception("translate returned unexpected response type")

        data_dict = dataframe.to_dict(orient="records")
        total_rows = latest_progress["totalRows"] or latest_progress["processedRows"]
        _update_translate_task(
            task_id,
            status="completed",
            percent=100,
            message="Translation completed.",
            stage="completed",
            file=data_dict,
            order=desired_order,
            warnings=warnings,
            error="",
            processedRows=total_rows,
            totalRows=total_rows,
            elapsedSeconds=round(time.time() - started_at_ts, 1),
            finishedAt=_utc_now_iso(),
        )
    except Exception as err:
        error_message = str(err)
        is_canceled = "cancelled" in error_message.lower() or "canceled" in error_message.lower()
        _update_translate_task(
            task_id,
            status="canceled" if is_canceled else "failed",
            message="Translation canceled." if is_canceled else "",
            error="" if is_canceled else error_message,
            stage="completed",
            percent=100 if is_canceled else 0,
            elapsedSeconds=round(time.time() - started_at_ts, 1),
            finishedAt=_utc_now_iso(),
        )
    
@search_bp.route('/search', methods=['GET'])
def getSearch():
    """Search endpoint for explore page
    This endpoint is used for database searches of a single or empty term.
    ---
    parameters:
        - name: database
          in: query
          type: string
          enum: ['SocioMap','ArchaMap']
          required: true
          description: Name of the CatMapper database to search
        - name: term
          in: query
          type: string
          required: false
          description: Search term
        - name: property
          in: query
          type: string
          required: false
          enum: ['Name','CMID','Key']
          description: Property to search by
        - name: domain
          in: query
          type: string
          required: false
          enum: ['DISTRICT','ETHNICITY','STONE']
          default: CATEGORY
          description: Domain containing the category
        - name: yearStart
          in: query
          type: integer
          required: false
          description: Earliest year the category existed or data was collected from (will return a result if category year range intersects with year range)
        - name: yearEnd
          in: query
          type: integer
          required: false
          description: Latest year the category existed or data was collected from
        - name: country
          in: query
          type: string
          required: false
          description: CMID of ADM0 node with DISTRICT_OF tie
        - name: context
          in: query
          type: string
          required: false
          description: CMID of parent node in network
        - name: contexts
          in: query
          type: string
          required: false
          description: Comma-separated list of context CMIDs (or repeated query parameter). Supports multi-context filtering.
        - name: contextMode
          in: query
          type: string
          enum: ['all','any']
          required: false
          default: all
          description: Multi-context mode. `all` requires ties to every context CMID. `any` requires at least one.
        - name: limit
          in: query
          type: string
          required: false
          default: 10000
          description: Number of results to limit search to
        - name: query
          in: query
          type: string
          enum: ['true','false']
          required: false
          description: Whether to return results or cypher query
    response:
        200:
            description: JSON of search results unless query is true, then a JSON with the cypher query is returned.
            schema:
                type: object
                properties:
                    CMID:
                        type: string
                        example: SM1
                    CMName:
                        type: string
                        example: Afghanistan
                    country:
                        type: array
                        items:
                            type: string
                        example: ["United States of America"]
                    domain:
                        type: array
                        items:
                            type: string
                        example: ["DISTRICT","FEATURE"]
                    matching:
                        type: string
                        example: Afghanistan
                    matchingDistance:
                        type: integer
                        example: 1
        500:
            description: JSON of error
            schema:
            type: string
    """
    try:
        database = request.args.get('database')
        term = request.args.get('term')
        property = request.args.get('property')
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        domain = request.args.get('domain')
        yearStart = request.args.get('yearStart')
        yearEnd = request.args.get('yearEnd')
        context = request.args.get('context')
        contexts = _parse_contexts_query_args(request)
        context_mode = request.args.get('contextMode') or request.args.get('context_mode') or "all"
        dataset = request.args.get('dataset')
        country = request.args.get('country')
        query = request.args.get('query')

        result = search(
            database,
            term,
            property,
            domain,
            yearStart,
            yearEnd,
            context,
            country,
            query,
            dataset,
            contexts=contexts,
            context_mode=context_mode)
        
        return jsonify(result)

    except Exception as e:
        return str(e), 500    

@search_bp.route('/translate', methods=['POST'])
def getTranslate2():
    try:
        payload = request.get_data()
        payload = json.loads(payload)
        translate_kwargs = _parse_translate_payload(payload)

        translate_result = translate(**translate_kwargs)

        warnings = []
        if isinstance(translate_result, tuple):
            if len(translate_result) == 3:
                data, desired_order, warnings = translate_result
            elif len(translate_result) == 2:
                data, desired_order = translate_result
            else:
                raise Exception("translate returned unexpected tuple shape")
        else:
            raise Exception("translate returned unexpected response type")

        data_dict = data.to_dict(orient='records')

        print(data_dict)

        return jsonify({"file": data_dict, "order": desired_order, "warnings": warnings})

    except Exception as e:
        return str(e), 500


@search_bp.route('/translate/start', methods=['POST'])
def start_translate_task():
    try:
        payload = request.get_data()
        payload = json.loads(payload)
        translate_kwargs = _parse_translate_payload(payload)
        batch_size_raw = payload.get("batchSize", 500)

        try:
            batch_size = int(batch_size_raw)
        except Exception:
            batch_size = 500
        batch_size = max(1, batch_size)

        task_id = uuid.uuid4().hex
        created_at = _utc_now_iso()
        task = {
            "taskId": task_id,
            "status": "processing",
            "stage": "preprocessing",
            "percent": 10,
            "message": "Processing input...",
            "error": "",
            "database": translate_kwargs.get("database"),
            "batchSize": batch_size,
            "processedRows": 0,
            "totalRows": 0,
            "elapsedSeconds": 0,
            "createdAt": created_at,
            "startedAt": created_at,
            "startedAtTs": time.time(),
            "finishedAt": None,
            "cancelRequested": False,
            "file": None,
            "order": None,
            "warnings": [],
        }
        _save_translate_task(task)

        thread = threading.Thread(
            target=_run_translate_task,
            args=(task_id, translate_kwargs, batch_size),
            daemon=True,
            name=f"translate-{task_id[:8]}",
        )
        thread.start()
        return jsonify(_translate_task_response(task))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@search_bp.route('/translate/status', methods=['POST'])
def get_translate_task_status():
    try:
        payload = request.get_data()
        payload = json.loads(payload)
        task_id = unlist(payload.get("taskId"))
        if not task_id:
            raise Exception("taskId not specified")

        task = _load_translate_task(str(task_id))
        if task is None:
            return jsonify({"error": "Task not found"}), 404

        response_task = _translate_task_response(task)
        if str(response_task.get("status", "")).lower() == "processing":
            started_at_ts = float(task.get("startedAtTs") or 0)
            if started_at_ts > 0:
                response_task["elapsedSeconds"] = round(time.time() - started_at_ts, 1)
        return jsonify(response_task)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@search_bp.route('/translate/cancel', methods=['POST'])
def cancel_translate_task():
    try:
        payload = request.get_data()
        payload = json.loads(payload)
        task_id = unlist(payload.get("taskId"))
        if not task_id:
            raise Exception("taskId not specified")

        task = _load_translate_task(str(task_id))
        if task is None:
            return jsonify({"error": "Task not found"}), 404

        status = str(task.get("status") or "").lower()
        if status in {"completed", "failed", "canceled"}:
            return jsonify(_translate_task_response(task))

        updated = _update_translate_task(
            str(task_id),
            cancelRequested=True,
            message="Cancel requested. Waiting for current batch to finish.",
        )
        return jsonify(_translate_task_response(updated))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@search_bp.route('/nlp/parse-log', methods=['POST'])
def save_nlp_parse_log():
    try:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "JSON object payload is required."}), 400

        database = _safe_log_database_name(payload.get("database"))
        now_utc = datetime.now(timezone.utc)
        log_dir = os.getenv("CATMAPPER_NLP_PARSE_LOG_DIR", _DEFAULT_NLP_PARSE_LOG_DIR)
        os.makedirs(log_dir, exist_ok=True)

        file_name = f"{database}_{now_utc.strftime('%Y-%m-%d')}.jsonl"
        target_file = os.path.join(log_dir, file_name)

        sanitized_payload = _sanitize_log_value(payload)
        record = {
            "server_received_at": now_utc.isoformat(),
            "database": database,
            "entry": sanitized_payload
        }
        line = json.dumps(record, ensure_ascii=False)

        with _NLP_PARSE_LOG_WRITE_LOCK:
            with open(target_file, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")

        return jsonify({"status": "ok", "path": target_file}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
