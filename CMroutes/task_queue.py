import os

from .task_store import get_redis_connection

try:
    from rq import Queue
except Exception:  # pragma: no cover
    Queue = None


def _env_enabled(name, default="1"):
    value = os.getenv(name, default)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def is_rq_enabled():
    if not _env_enabled("CATMAPPER_USE_RQ", "1"):
        return False
    if Queue is None:
        return False
    return get_redis_connection() is not None


def _enqueue(function_path, *args):
    connection = get_redis_connection()
    if Queue is None or connection is None:
        raise RuntimeError("RQ queue is not available. Check redis/rq setup.")

    queue_name = os.getenv("CATMAPPER_UPLOAD_QUEUE", "catmapper-upload")
    failure_ttl = int(os.getenv("CATMAPPER_RQ_FAILURE_TTL", "86400"))
    queue = Queue(name=queue_name, connection=connection)
    return queue.enqueue(
        function_path,
        *args,
        job_timeout=-1,
        result_ttl=0,
        failure_ttl=failure_ttl,
    )


def enqueue_upload_task(task_id):
    return _enqueue("CMroutes.upload_jobs.run_upload_task", task_id)


def enqueue_waiting_uses_task(waiting_task_id, database):
    return _enqueue("CMroutes.upload_jobs.run_waiting_uses_task", waiting_task_id, database)
