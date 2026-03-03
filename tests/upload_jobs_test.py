import pandas as pd

import CMroutes.upload_jobs as upload_jobs


class FakeUploadStore:
    def __init__(self):
        self.events = []
        self.increment_calls = 0
        self.marked_running = False
        self.completed = False

    def get_upload_task(self, task_id, cursor=0):
        return {
            "taskId": task_id,
            "status": "queued",
            "user": "api-user",
            "database": "ArchaMap",
        }

    def get_upload_job_payload(self, task_id):
        return {
            "dataset": [],
            "database": "ArchaMap",
            "uploadOption": "add_uses",
            "formatKey": True,
            "optionalProperties": [],
            "user": "api-user",
            "addDistrict": False,
            "addRecordYear": False,
            "geocode": False,
            "batchSize": 500,
        }

    def is_upload_cancel_requested(self, task_id):
        return False

    def mark_upload_running(self, task_id):
        self.marked_running = True

    def append_upload_event(self, task_id, message):
        self.events.append(message)

    def increment_upload_batch(self, task_id):
        self.increment_calls += 1

    def create_waiting_task(self, user, database, upload_task_id):
        return "waiting-1"

    def complete_upload_task(self, **kwargs):
        self.completed = True

    def cancel_upload_task(self, task_id, message):
        raise AssertionError("cancel_upload_task should not be called")

    def fail_upload_task(self, task_id, message):
        raise AssertionError(f"fail_upload_task should not be called: {message}")

    def delete_upload_job_payload(self, task_id):
        return None


def test_run_upload_task_increments_batch_for_timed_end_of_batch_log(monkeypatch):
    store = FakeUploadStore()
    listener_ref = {}

    monkeypatch.setattr(upload_jobs, "get_task_store", lambda: store)
    monkeypatch.setattr(upload_jobs, "set_upload_log_listener", lambda cb: listener_ref.setdefault("cb", cb))
    monkeypatch.setattr(upload_jobs, "clear_upload_log_listener", lambda: None)
    monkeypatch.setattr(upload_jobs, "set_query_cancel_checker", lambda checker: None)
    monkeypatch.setattr(upload_jobs, "clear_query_cancel_checker", lambda: None)
    monkeypatch.setattr(upload_jobs, "is_rq_enabled", lambda: False)
    monkeypatch.setattr(upload_jobs, "_run_waiting_uses_inline", lambda waiting_task_id, database: None)

    def fake_input_nodes_uses(**kwargs):
        listener_ref["cb"]("[+0.20s | 0.20s] End of batch")
        return pd.DataFrame([{"CMID": "SM1"}]), ["CMID"]

    monkeypatch.setattr(upload_jobs, "input_Nodes_Uses", fake_input_nodes_uses)

    upload_jobs.run_upload_task("task-123")

    assert store.marked_running is True
    assert store.increment_calls == 1
    assert store.completed is True
