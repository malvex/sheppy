import pytest
from fastapi.testclient import TestClient
from sheppy import MemoryBackend, Queue, Worker
from main import app, get_queue


@pytest.fixture
def queue():
    return Queue(MemoryBackend(), "pytest")


def test_fastapi_send_email_route(queue):
    app.dependency_overrides[get_queue] = lambda: queue

    with TestClient(app) as client:
        # Define email data
        email_data = {
            "to": "test@example.com",
            "subject": "Welcome Email",
            "body": "Hello, pytest!"
        }

        response = client.post("/send-email", json=email_data)

        assert response.status_code == 200
        assert response.json() == {"ok": True}
