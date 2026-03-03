import json
import math
import os
import threading
import time
import uuid
from datetime import datetime, timezone

try:
    import redis
except Exception:  # pragma: no cover
    redis = None


DEFAULT_UPLOAD_BATCH_SIZE = int(os.getenv("CATMAPPER_UPLOAD_BATCH_SIZE", "500"))
DEFAULT_TASK_RETENTION_SECONDS = int(
    os.getenv("CATMAPPER_UPLOAD_TASK_RETENTION_SECONDS", "86400")
)

_TASK_STORE_LOCK = threading.Lock()
_TASK_STORE_SINGLETON = None


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _compute_percent(completed, total):
    if total <= 0:
        return 0
    return int(min(100, max(0, round((completed / total) * 100))))


def _new_upload_task(task_id, user, database, total_rows, batch_size):
    total_rows = _safe_int(total_rows, 0)
    batch_size = max(1, _safe_int(batch_size, DEFAULT_UPLOAD_BATCH_SIZE))
    total_batches = math.ceil(total_rows / batch_size) if total_rows > 0 else 0
    return {
        "taskId": task_id,
        "status": "queued",
        "user": str(user),
        "database": str(database),
        "createdAt": _utc_now_iso(),
        "startedAt": None,
        "finishedAt": None,
        "message": None,
        "error": None,
        "finishedAtTs": None,
        "batchSize": batch_size,
        "totalRows": total_rows,
        "totalBatches": total_batches,
        "completedBatches": 0,
        "percent": 0,
        "events": ["Task queued. Waiting for available worker."],
        "cancelRequested": False,
        "resultFile": None,
        "resultOrder": None,
        "waitingUsesTask": None,
        "waitingUsesStatus": None,
        "jobPayload": None,
        "rqJobId": None,
    }


def _new_waiting_task(task_id, user, database, upload_task_id=None):
    return {
        "taskId": task_id,
        "status": "queued",
        "user": str(user),
        "database": str(database),
        "createdAt": _utc_now_iso(),
        "startedAt": None,
        "finishedAt": None,
        "message": None,
        "error": None,
        "finishedAtTs": None,
        "uploadTaskId": upload_task_id,
    }


def _serialize_upload_task(task, cursor=0):
    response_task = dict(task)
    response_task.pop("finishedAtTs", None)
    response_task.pop("jobPayload", None)
    response_task.pop("rqJobId", None)

    all_events = response_task.pop("events", [])
    cursor = min(max(_safe_int(cursor, 0), 0), len(all_events))
    response_task["events"] = all_events[cursor:]
    response_task["nextCursor"] = len(all_events)

    response_task["progress"] = {
        "batchSize": response_task.pop("batchSize", DEFAULT_UPLOAD_BATCH_SIZE),
        "totalRows": response_task.pop("totalRows", 0),
        "totalBatches": response_task.pop("totalBatches", 0),
        "completedBatches": response_task.pop("completedBatches", 0),
        "percent": response_task.pop("percent", 0),
    }

    response_task["file"] = response_task.pop("resultFile", None)
    response_task["order"] = response_task.pop("resultOrder", None)
    return response_task


class InMemoryTaskStore:
    def __init__(self, retention_seconds=DEFAULT_TASK_RETENTION_SECONDS):
        self.retention_seconds = retention_seconds
        self.lock = threading.RLock()
        self.upload_tasks = {}
        self.waiting_tasks = {}

    def _prune(self):
        now_ts = time.time()
        stale_upload = []
        stale_waiting = []
        for task_id, task in self.upload_tasks.items():
            finished_at_ts = task.get("finishedAtTs")
            if finished_at_ts is not None and now_ts - finished_at_ts > self.retention_seconds:
                stale_upload.append(task_id)
        for task_id, task in self.waiting_tasks.items():
            finished_at_ts = task.get("finishedAtTs")
            if finished_at_ts is not None and now_ts - finished_at_ts > self.retention_seconds:
                stale_waiting.append(task_id)
        for task_id in stale_upload:
            self.upload_tasks.pop(task_id, None)
        for task_id in stale_waiting:
            self.waiting_tasks.pop(task_id, None)

    def create_upload_task(self, user, database, total_rows, batch_size):
        task_id = uuid.uuid4().hex
        task = _new_upload_task(task_id, user, database, total_rows, batch_size)
        with self.lock:
            self._prune()
            self.upload_tasks[task_id] = task
        return task_id

    def set_upload_job_payload(self, task_id, payload):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["jobPayload"] = payload

    def get_upload_job_payload(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return None
            return task.get("jobPayload")

    def delete_upload_job_payload(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["jobPayload"] = None

    def set_upload_rq_job_id(self, task_id, job_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["rqJobId"] = str(job_id) if job_id else None

    def get_upload_rq_job_id(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return None
            return task.get("rqJobId")

    def mark_upload_running(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["status"] = "running"
            task["startedAt"] = _utc_now_iso()

    def append_upload_event(self, task_id, message):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["events"].append(str(message))

    def increment_upload_batch(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["completedBatches"] = min(
                _safe_int(task.get("completedBatches", 0)) + 1,
                _safe_int(task.get("totalBatches", 0)),
            )
            task["percent"] = _compute_percent(
                _safe_int(task.get("completedBatches", 0)),
                _safe_int(task.get("totalBatches", 0)),
            )

    def is_upload_cancel_requested(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return False
            return bool(task.get("cancelRequested"))

    def request_upload_cancel(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return False
            if task.get("status") in {"completed", "failed", "canceled"}:
                return True
            task["cancelRequested"] = True
            task["events"].append("Cancellation requested by user.")
            return True

    def complete_upload_task(self, task_id, message, result_file, result_order, waiting_task_id=None):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["status"] = "completed"
            task["finishedAt"] = _utc_now_iso()
            task["finishedAtTs"] = time.time()
            task["message"] = str(message)
            task["error"] = None
            task["resultFile"] = result_file
            task["resultOrder"] = result_order
            task["waitingUsesTask"] = waiting_task_id
            task["waitingUsesStatus"] = "queued" if waiting_task_id else None
            task["completedBatches"] = _safe_int(task.get("totalBatches", 0))
            task["percent"] = 100 if _safe_int(task.get("totalBatches", 0)) > 0 else 0

    def cancel_upload_task(self, task_id, message):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["status"] = "canceled"
            task["finishedAt"] = _utc_now_iso()
            task["finishedAtTs"] = time.time()
            task["message"] = str(message)
            task["error"] = None

    def fail_upload_task(self, task_id, error_message):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return
            task["status"] = "failed"
            task["finishedAt"] = _utc_now_iso()
            task["finishedAtTs"] = time.time()
            task["message"] = None
            task["error"] = str(error_message)
            task["events"].append(str(error_message))

    def set_upload_waiting_status(self, upload_task_id, waiting_status):
        with self.lock:
            task = self.upload_tasks.get(upload_task_id)
            if task is None:
                return
            task["waitingUsesStatus"] = str(waiting_status)

    def get_upload_task(self, task_id, cursor=0):
        with self.lock:
            self._prune()
            task = self.upload_tasks.get(task_id)
            if task is None:
                return None
            return _serialize_upload_task(task, cursor=cursor)

    def get_upload_user(self, task_id):
        with self.lock:
            task = self.upload_tasks.get(task_id)
            if task is None:
                return None
            return task.get("user")

    def create_waiting_task(self, user, database, upload_task_id=None):
        task_id = uuid.uuid4().hex
        task = _new_waiting_task(task_id, user, database, upload_task_id=upload_task_id)
        with self.lock:
            self._prune()
            self.waiting_tasks[task_id] = task
            if upload_task_id:
                upload_task = self.upload_tasks.get(upload_task_id)
                if upload_task:
                    upload_task["waitingUsesTask"] = task_id
                    upload_task["waitingUsesStatus"] = "queued"
        return task_id

    def mark_waiting_running(self, waiting_task_id):
        with self.lock:
            task = self.waiting_tasks.get(waiting_task_id)
            if task is None:
                return
            task["status"] = "running"
            task["startedAt"] = _utc_now_iso()
            upload_task_id = task.get("uploadTaskId")
            if upload_task_id:
                self.set_upload_waiting_status(upload_task_id, "running")

    def complete_waiting_task(self, waiting_task_id, message):
        with self.lock:
            task = self.waiting_tasks.get(waiting_task_id)
            if task is None:
                return
            task["status"] = "completed"
            task["finishedAt"] = _utc_now_iso()
            task["finishedAtTs"] = time.time()
            task["message"] = str(message)
            task["error"] = None
            upload_task_id = task.get("uploadTaskId")
            if upload_task_id:
                self.set_upload_waiting_status(upload_task_id, "completed")

    def fail_waiting_task(self, waiting_task_id, error_message):
        with self.lock:
            task = self.waiting_tasks.get(waiting_task_id)
            if task is None:
                return
            task["status"] = "failed"
            task["finishedAt"] = _utc_now_iso()
            task["finishedAtTs"] = time.time()
            task["message"] = None
            task["error"] = str(error_message)
            upload_task_id = task.get("uploadTaskId")
            if upload_task_id:
                self.set_upload_waiting_status(upload_task_id, "failed")

    def get_waiting_task(self, waiting_task_id):
        with self.lock:
            self._prune()
            task = self.waiting_tasks.get(waiting_task_id)
            if task is None:
                return None
            response_task = dict(task)
            response_task.pop("finishedAtTs", None)
            response_task.pop("uploadTaskId", None)
            return response_task


class RedisTaskStore:
    def __init__(self, redis_client, retention_seconds=DEFAULT_TASK_RETENTION_SECONDS):
        self.redis = redis_client
        self.retention_seconds = retention_seconds

    def _upload_task_key(self, task_id):
        return f"cm:upload:task:{task_id}"

    def _upload_events_key(self, task_id):
        return f"cm:upload:events:{task_id}"

    def _upload_job_key(self, task_id):
        return f"cm:upload:job:{task_id}"

    def _rq_job_key(self, job_id):
        return f"rq:job:{job_id}"

    def _find_rq_job_id_for_task(self, task_id):
        needle = f"run_upload_task('{task_id}')"
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor=cursor, match="rq:job:*", count=200)
            for raw_key in keys:
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                description = self.redis.hget(key, "description")
                if isinstance(description, bytes):
                    description = description.decode()
                if description and needle in str(description):
                    return key.split("rq:job:", 1)[-1]
            if cursor == 0:
                break
        return None

    def _waiting_task_key(self, task_id):
        return f"cm:waiting_uses:task:{task_id}"

    def _decode_hash(self, raw):
        if not raw:
            return None
        decoded = {}
        for key, value in raw.items():
            decoded_key = key.decode() if isinstance(key, bytes) else str(key)
            if isinstance(value, bytes):
                try:
                    decoded_value = value.decode()
                except UnicodeDecodeError:
                    decoded_value = value
            else:
                decoded_value = value
            decoded[decoded_key] = decoded_value
        return decoded

    def _decode_list(self, raw_values):
        values = []
        for value in raw_values:
            if isinstance(value, bytes):
                values.append(value.decode())
            else:
                values.append(str(value))
        return values

    def _expire_upload_keys(self, task_id):
        self.redis.expire(self._upload_task_key(task_id), self.retention_seconds)
        self.redis.expire(self._upload_events_key(task_id), self.retention_seconds)
        self.redis.expire(self._upload_job_key(task_id), self.retention_seconds)

    def _expire_waiting_key(self, waiting_task_id):
        self.redis.expire(self._waiting_task_key(waiting_task_id), self.retention_seconds)

    def create_upload_task(self, user, database, total_rows, batch_size):
        task_id = uuid.uuid4().hex
        task = _new_upload_task(task_id, user, database, total_rows, batch_size)
        task_mapping = {
            "taskId": task["taskId"],
            "status": task["status"],
            "user": task["user"],
            "database": task["database"],
            "createdAt": task["createdAt"],
            "startedAt": "",
            "finishedAt": "",
            "message": "",
            "error": "",
            "batchSize": str(task["batchSize"]),
            "totalRows": str(task["totalRows"]),
            "totalBatches": str(task["totalBatches"]),
            "completedBatches": "0",
            "percent": "0",
            "cancelRequested": "0",
            "waitingUsesTask": "",
            "waitingUsesStatus": "",
            "resultFile": "",
            "resultOrder": "",
            "rqJobId": "",
        }
        self.redis.hset(self._upload_task_key(task_id), mapping=task_mapping)
        self.redis.rpush(
            self._upload_events_key(task_id),
            "Task queued. Waiting for available worker.",
        )
        self._expire_upload_keys(task_id)
        return task_id

    def set_upload_job_payload(self, task_id, payload):
        self.redis.set(self._upload_job_key(task_id), json.dumps(payload))
        self._expire_upload_keys(task_id)

    def get_upload_job_payload(self, task_id):
        raw = self.redis.get(self._upload_job_key(task_id))
        if raw is None:
            return None
        text = raw.decode() if isinstance(raw, bytes) else str(raw)
        try:
            return json.loads(text)
        except Exception:
            return None

    def delete_upload_job_payload(self, task_id):
        self.redis.delete(self._upload_job_key(task_id))

    def set_upload_rq_job_id(self, task_id, job_id):
        self.redis.hset(
            self._upload_task_key(task_id),
            mapping={"rqJobId": str(job_id) if job_id else ""},
        )
        self._expire_upload_keys(task_id)

    def get_upload_rq_job_id(self, task_id):
        value = self.redis.hget(self._upload_task_key(task_id), "rqJobId")
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode()
        value = str(value).strip()
        return value or None

    def mark_upload_running(self, task_id):
        self.redis.hset(
            self._upload_task_key(task_id),
            mapping={"status": "running", "startedAt": _utc_now_iso()},
        )
        self._expire_upload_keys(task_id)

    def append_upload_event(self, task_id, message):
        self.redis.rpush(self._upload_events_key(task_id), str(message))
        self._expire_upload_keys(task_id)

    def increment_upload_batch(self, task_id):
        task_key = self._upload_task_key(task_id)
        completed = self.redis.hincrby(task_key, "completedBatches", 1)
        total = _safe_int(self.redis.hget(task_key, "totalBatches"), 0)
        if completed > total:
            completed = total
            self.redis.hset(task_key, "completedBatches", total)
        percent = _compute_percent(completed, total)
        self.redis.hset(task_key, "percent", percent)
        self._expire_upload_keys(task_id)

    def is_upload_cancel_requested(self, task_id):
        value = self.redis.hget(self._upload_task_key(task_id), "cancelRequested")
        if value is None:
            return False
        if isinstance(value, bytes):
            value = value.decode()
        return str(value).strip() == "1"

    def request_upload_cancel(self, task_id):
        task_key = self._upload_task_key(task_id)
        status = self.redis.hget(task_key, "status")
        if status is None:
            return False
        if isinstance(status, bytes):
            status = status.decode()
        if status not in {"completed", "failed", "canceled"}:
            self.redis.hset(task_key, "cancelRequested", "1")
            self.append_upload_event(task_id, "Cancellation requested by user.")
        self._expire_upload_keys(task_id)
        return True

    def complete_upload_task(self, task_id, message, result_file, result_order, waiting_task_id=None):
        task_key = self._upload_task_key(task_id)
        total_batches = _safe_int(self.redis.hget(task_key, "totalBatches"), 0)
        mapping = {
            "status": "completed",
            "finishedAt": _utc_now_iso(),
            "message": str(message),
            "error": "",
            "resultFile": json.dumps(result_file or []),
            "resultOrder": json.dumps(result_order or []),
            "waitingUsesTask": waiting_task_id or "",
            "waitingUsesStatus": "queued" if waiting_task_id else "",
            "completedBatches": str(total_batches),
            "percent": "100" if total_batches > 0 else "0",
        }
        self.redis.hset(task_key, mapping=mapping)
        self._expire_upload_keys(task_id)

    def cancel_upload_task(self, task_id, message):
        self.redis.hset(
            self._upload_task_key(task_id),
            mapping={
                "status": "canceled",
                "finishedAt": _utc_now_iso(),
                "message": str(message),
                "error": "",
            },
        )
        self._expire_upload_keys(task_id)

    def fail_upload_task(self, task_id, error_message):
        self.redis.hset(
            self._upload_task_key(task_id),
            mapping={
                "status": "failed",
                "finishedAt": _utc_now_iso(),
                "message": "",
                "error": str(error_message),
            },
        )
        self.append_upload_event(task_id, str(error_message))
        self._expire_upload_keys(task_id)

    def set_upload_waiting_status(self, upload_task_id, waiting_status):
        self.redis.hset(
            self._upload_task_key(upload_task_id),
            mapping={"waitingUsesStatus": str(waiting_status)},
        )
        self._expire_upload_keys(upload_task_id)

    def _reconcile_upload_task_with_rq(self, task_id, task_raw):
        status = str(task_raw.get("status") or "").strip().lower()
        if status in {"completed", "failed", "canceled"}:
            return task_raw

        rq_job_id = str(task_raw.get("rqJobId") or "").strip()
        if not rq_job_id:
            discovered_job_id = self._find_rq_job_id_for_task(task_id)
            if not discovered_job_id:
                return task_raw
            rq_job_id = discovered_job_id
            self.set_upload_rq_job_id(task_id, rq_job_id)
            task_raw["rqJobId"] = rq_job_id

        rq_raw = self._decode_hash(self.redis.hgetall(self._rq_job_key(rq_job_id)))
        task_key = self._upload_task_key(task_id)
        cancel_requested = str(task_raw.get("cancelRequested", "0")).strip() == "1"

        if rq_raw is None:
            if cancel_requested:
                self.cancel_upload_task(task_id, "Upload cancelled by user request.")
            else:
                self.fail_upload_task(
                    task_id,
                    "Upload job missing from queue backend. Worker may have stopped unexpectedly.",
                )
            return self._decode_hash(self.redis.hgetall(task_key))

        rq_status = str(rq_raw.get("status") or "").strip().lower()
        if rq_status == "queued" and status == "running":
            self.redis.hset(task_key, mapping={"status": "queued", "startedAt": ""})
        elif rq_status == "started" and status == "queued":
            started_at = str(rq_raw.get("started_at") or _utc_now_iso())
            self.redis.hset(task_key, mapping={"status": "running", "startedAt": started_at})
        elif rq_status == "failed" and status not in {"completed", "failed", "canceled"}:
            if cancel_requested:
                self.cancel_upload_task(task_id, "Upload cancelled by user request.")
            else:
                self.fail_upload_task(task_id, "Upload job failed in queue backend.")

        return self._decode_hash(self.redis.hgetall(task_key))

    def get_upload_task(self, task_id, cursor=0):
        task_raw = self._decode_hash(self.redis.hgetall(self._upload_task_key(task_id)))
        if task_raw is None:
            return None

        task_raw = self._reconcile_upload_task_with_rq(task_id, task_raw)
        if task_raw is None:
            return None

        total_events = self.redis.llen(self._upload_events_key(task_id))
        cursor = min(max(_safe_int(cursor, 0), 0), total_events)
        events = self._decode_list(
            self.redis.lrange(self._upload_events_key(task_id), cursor, -1)
        )

        result_file = []
        result_order = []
        if task_raw.get("resultFile"):
            try:
                result_file = json.loads(task_raw["resultFile"])
            except Exception:
                result_file = []
        if task_raw.get("resultOrder"):
            try:
                result_order = json.loads(task_raw["resultOrder"])
            except Exception:
                result_order = []

        return {
            "taskId": task_raw.get("taskId"),
            "status": task_raw.get("status"),
            "user": task_raw.get("user"),
            "database": task_raw.get("database"),
            "createdAt": task_raw.get("createdAt"),
            "startedAt": task_raw.get("startedAt") or None,
            "finishedAt": task_raw.get("finishedAt") or None,
            "message": task_raw.get("message") or None,
            "error": task_raw.get("error") or None,
            "events": events,
            "nextCursor": total_events,
            "progress": {
                "batchSize": _safe_int(task_raw.get("batchSize"), DEFAULT_UPLOAD_BATCH_SIZE),
                "totalRows": _safe_int(task_raw.get("totalRows"), 0),
                "totalBatches": _safe_int(task_raw.get("totalBatches"), 0),
                "completedBatches": _safe_int(task_raw.get("completedBatches"), 0),
                "percent": _safe_int(task_raw.get("percent"), 0),
            },
            "file": result_file,
            "order": result_order,
            "cancelRequested": str(task_raw.get("cancelRequested", "0")) == "1",
            "waitingUsesTask": task_raw.get("waitingUsesTask") or None,
            "waitingUsesStatus": task_raw.get("waitingUsesStatus") or None,
        }

    def get_upload_user(self, task_id):
        user = self.redis.hget(self._upload_task_key(task_id), "user")
        if user is None:
            return None
        if isinstance(user, bytes):
            return user.decode()
        return str(user)

    def create_waiting_task(self, user, database, upload_task_id=None):
        waiting_task_id = uuid.uuid4().hex
        task = _new_waiting_task(
            waiting_task_id,
            user=user,
            database=database,
            upload_task_id=upload_task_id,
        )
        mapping = {
            "taskId": task["taskId"],
            "status": task["status"],
            "user": task["user"],
            "database": task["database"],
            "createdAt": task["createdAt"],
            "startedAt": "",
            "finishedAt": "",
            "message": "",
            "error": "",
            "uploadTaskId": upload_task_id or "",
        }
        self.redis.hset(self._waiting_task_key(waiting_task_id), mapping=mapping)
        self._expire_waiting_key(waiting_task_id)
        if upload_task_id:
            self.redis.hset(
                self._upload_task_key(upload_task_id),
                mapping={
                    "waitingUsesTask": waiting_task_id,
                    "waitingUsesStatus": "queued",
                },
            )
            self._expire_upload_keys(upload_task_id)
        return waiting_task_id

    def mark_waiting_running(self, waiting_task_id):
        key = self._waiting_task_key(waiting_task_id)
        self.redis.hset(key, mapping={"status": "running", "startedAt": _utc_now_iso()})
        upload_task_id = self.redis.hget(key, "uploadTaskId")
        if upload_task_id:
            if isinstance(upload_task_id, bytes):
                upload_task_id = upload_task_id.decode()
            self.set_upload_waiting_status(upload_task_id, "running")
        self._expire_waiting_key(waiting_task_id)

    def complete_waiting_task(self, waiting_task_id, message):
        key = self._waiting_task_key(waiting_task_id)
        self.redis.hset(
            key,
            mapping={
                "status": "completed",
                "finishedAt": _utc_now_iso(),
                "message": str(message),
                "error": "",
            },
        )
        upload_task_id = self.redis.hget(key, "uploadTaskId")
        if upload_task_id:
            if isinstance(upload_task_id, bytes):
                upload_task_id = upload_task_id.decode()
            self.set_upload_waiting_status(upload_task_id, "completed")
        self._expire_waiting_key(waiting_task_id)

    def fail_waiting_task(self, waiting_task_id, error_message):
        key = self._waiting_task_key(waiting_task_id)
        self.redis.hset(
            key,
            mapping={
                "status": "failed",
                "finishedAt": _utc_now_iso(),
                "message": "",
                "error": str(error_message),
            },
        )
        upload_task_id = self.redis.hget(key, "uploadTaskId")
        if upload_task_id:
            if isinstance(upload_task_id, bytes):
                upload_task_id = upload_task_id.decode()
            self.set_upload_waiting_status(upload_task_id, "failed")
        self._expire_waiting_key(waiting_task_id)

    def get_waiting_task(self, waiting_task_id):
        task_raw = self._decode_hash(self.redis.hgetall(self._waiting_task_key(waiting_task_id)))
        if task_raw is None:
            return None
        return {
            "taskId": task_raw.get("taskId"),
            "status": task_raw.get("status"),
            "user": task_raw.get("user"),
            "database": task_raw.get("database"),
            "createdAt": task_raw.get("createdAt"),
            "startedAt": task_raw.get("startedAt") or None,
            "finishedAt": task_raw.get("finishedAt") or None,
            "message": task_raw.get("message") or None,
            "error": task_raw.get("error") or None,
        }


def _build_redis_store():
    if redis is None:
        return None
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        client = redis.from_url(redis_url, socket_timeout=1, socket_connect_timeout=1)
        client.ping()
        return RedisTaskStore(client)
    except Exception:
        return None


def get_task_store():
    global _TASK_STORE_SINGLETON
    with _TASK_STORE_LOCK:
        if _TASK_STORE_SINGLETON is not None:
            return _TASK_STORE_SINGLETON
        redis_store = _build_redis_store()
        if redis_store is not None:
            _TASK_STORE_SINGLETON = redis_store
        else:
            _TASK_STORE_SINGLETON = InMemoryTaskStore()
        return _TASK_STORE_SINGLETON


def get_redis_connection():
    store = get_task_store()
    if isinstance(store, RedisTaskStore):
        return store.redis
    return None
