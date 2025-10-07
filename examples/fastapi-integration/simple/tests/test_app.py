import pytest
from fastapi.testclient import TestClient
from sheppy import MemoryBackend, Queue, Worker
from main import app, get_queue


@pytest.fixture
def backend():
    return MemoryBackend()


@pytest.fixture
def queue(backend):
    return Queue(backend, "pytest")


@pytest.fixture
def worker(backend):
    w = Worker("pytest", backend)
    # speed up tests (temporary solution)
    w._blocking_timeout = 0.01
    w._scheduler_polling_interval = 0.01
    w._cron_polling_interval = 0.01
    return w


def test_fastapi_send_email_route(queue, worker):

    app.dependency_overrides[get_queue] = lambda: queue

    with TestClient(app) as client:
        # run worker process (temporary solution)
        client.portal.start_task_soon(
            lambda: worker.work(max_tasks=1, register_signal_handlers=False)
        )

        # Define email data
        email_data = {
            "to": "test@example.com",
            "subject": "Welcome Email",
            "body": "Hello, pytest!"
        }

        response = client.post("/send-email", json=email_data)

        assert response.status_code == 200
        assert response.json() == {"ok": True}
