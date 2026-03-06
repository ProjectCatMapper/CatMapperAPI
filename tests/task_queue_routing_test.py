import CMroutes.task_queue as task_queue


class DummyQueue:
    instances = []

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection
        self.enqueued = None
        DummyQueue.instances.append(self)

    def enqueue(self, function_path, *args, **kwargs):
        self.enqueued = {
            "function_path": function_path,
            "args": args,
            "kwargs": kwargs,
        }
        return self.enqueued


def _setup_queue(monkeypatch):
    DummyQueue.instances = []
    monkeypatch.setattr(task_queue, "Queue", DummyQueue)
    monkeypatch.setattr(task_queue, "get_redis_connection", lambda: object())


def test_enqueue_upload_task_uses_upload_queue_by_default(monkeypatch):
    _setup_queue(monkeypatch)
    monkeypatch.delenv("CATMAPPER_UPLOAD_QUEUE", raising=False)

    task_queue.enqueue_upload_task("task-1")

    assert len(DummyQueue.instances) == 1
    instance = DummyQueue.instances[0]
    assert instance.name == "catmapper-upload"
    assert instance.enqueued["function_path"] == "CMroutes.upload_jobs.run_upload_task"
    assert instance.enqueued["args"] == ("task-1",)


def test_enqueue_waiting_uses_task_uses_dedicated_queue_by_default(monkeypatch):
    _setup_queue(monkeypatch)
    monkeypatch.delenv("CATMAPPER_WAITING_USES_QUEUE", raising=False)

    task_queue.enqueue_waiting_uses_task("wait-1", "archamap")

    assert len(DummyQueue.instances) == 1
    instance = DummyQueue.instances[0]
    assert instance.name == "catmapper-waiting-uses"
    assert instance.enqueued["function_path"] == "CMroutes.upload_jobs.run_waiting_uses_task"
    assert instance.enqueued["args"] == ("wait-1", "archamap")


def test_enqueue_waiting_uses_task_honors_queue_override(monkeypatch):
    _setup_queue(monkeypatch)
    monkeypatch.setenv("CATMAPPER_WAITING_USES_QUEUE", "custom-waiting")

    task_queue.enqueue_waiting_uses_task("wait-2", "sociomap")

    assert len(DummyQueue.instances) == 1
    instance = DummyQueue.instances[0]
    assert instance.name == "custom-waiting"
