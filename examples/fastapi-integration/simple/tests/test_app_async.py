import asyncio
import pytest
from httpx import ASGITransport, AsyncClient
from sheppy import MemoryBackend, Queue, Worker
from main import app, get_queue


@pytest.fixture
def queue():
    return Queue(MemoryBackend(), "pytest")


async def test_fastapi_send_email_route(queue):
    # override queue dependency to inject PytestBackend
    app.dependency_overrides[get_queue] = lambda: queue

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        # Define email data
        email_data = {
            "to": "test@example.com",
            "subject": "Welcome Email",
            "body": "Hello, pytest!"
        }

        response = await client.post("/send-email", json=email_data)

        assert response.status_code == 200
        assert response.json() == {"ok": True}
