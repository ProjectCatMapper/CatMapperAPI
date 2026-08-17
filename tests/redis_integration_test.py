import pytest

from CMroutes import task_queue
from CMroutes.task_store import RedisTaskStore


@pytest.mark.redis
def test_redis_task_store_and_rq_queue_lifecycle(redis_client, monkeypatch):
    store = RedisTaskStore(redis_client, retention_seconds=120)
    task_id = store.create_upload_task(
        user="pytest-user",
        database="ArchaMap",
        total_rows=3,
        batch_size=2,
    )

    store.set_upload_job_payload(task_id, {"rows": [{"CMID": "AM1"}]})
    assert store.get_upload_job_payload(task_id) == {"rows": [{"CMID": "AM1"}]}

    monkeypatch.setattr(task_queue, "get_redis_connection", lambda: redis_client)
    monkeypatch.setenv("CATMAPPER_UPLOAD_QUEUE", "pytest-upload")
    job = task_queue.enqueue_upload_task(task_id)
    store.set_upload_rq_job_id(task_id, job.id)

    store.mark_upload_running(task_id)
    store.increment_upload_batch(task_id)
    store.append_upload_event(task_id, "Processed first batch.")
    waiting_id = store.create_waiting_task(
        user="pytest-user",
        database="ArchaMap",
        upload_task_id=task_id,
    )
    store.mark_waiting_running(waiting_id)
    store.complete_waiting_task(waiting_id, "USES processing complete.")
    store.complete_upload_task(
        task_id,
        "Upload complete.",
        result_file=[{"CMID": "AM1"}],
        result_order=["CMID"],
        waiting_task_id=waiting_id,
    )

    status = store.get_upload_task(task_id)
    assert status["status"] == "completed"
    assert status["progress"] == {
        "batchSize": 2,
        "totalRows": 3,
        "totalBatches": 2,
        "completedBatches": 2,
        "percent": 100,
    }
    assert status["file"] == [{"CMID": "AM1"}]
    assert status["waitingUsesTask"] == waiting_id
    assert store.get_waiting_task(waiting_id)["status"] == "completed"
    assert redis_client.ttl(store._upload_task_key(task_id)) > 0
    assert redis_client.exists(f"rq:job:{job.id}") == 1
