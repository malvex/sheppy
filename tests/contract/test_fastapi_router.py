from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from sheppy import Queue, Task, Worker
from sheppy.backend import Backend
from tests.dependencies import failing_task

try:
    from fastapi import FastAPI  # type: ignore[import-not-found,no-redef,unused-ignore]
    from httpx import (  # type: ignore[import-not-found,no-redef,unused-ignore]
        ASGITransport,
        AsyncClient,
    )

    from sheppy.fastapi import create_router  # type: ignore[no-redef,unused-ignore]

    FASTAPI_INSTALLED = True
except ImportError:
    FASTAPI_INSTALLED = False

pytestmark = pytest.mark.skipif(not FASTAPI_INSTALLED, reason="FastAPI is not installed")


@pytest.fixture
async def client(backend: Backend):

    app = FastAPI()
    app.include_router(create_router(backend, prefix="/sheppy"))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


class TestQueueStats:
    async def test_list_queues_with_tasks(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        await queue.add(task_add_fn(1, 2))
        await queue.add(task_add_fn(3, 4))

        resp = await client.get("/sheppy/queues")
        assert resp.status_code == 200
        data = resp.json()
        assert "pytest" in data
        assert data["pytest"] == 2

    async def test_get_queue_size_empty(self, client: "AsyncClient"):
        resp = await client.get("/sheppy/pytest/size")
        assert resp.status_code == 200
        assert resp.json() == 0

    async def test_get_queue_size_with_tasks(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        await queue.add(task_add_fn(1, 2))
        await queue.add(task_add_fn(3, 4))

        resp = await client.get("/sheppy/pytest/size")
        assert resp.status_code == 200
        assert resp.json() == 2


class TestTaskListing:
    async def test_list_tasks_empty(self, client: "AsyncClient"):
        resp = await client.get("/sheppy/pytest/tasks")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_list_tasks(self, client: "AsyncClient", queue: Queue, worker: Worker, task_add_fn: Callable[[int, int], Task]):
        t = task_add_fn(1, 2)
        await queue.add(t)
        await worker.work(1)

        resp = await client.get("/sheppy/pytest/tasks")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["id"] == str(t.id)
        assert data[0]["status"] == "completed"
        assert data[0]["result"] == 3

    async def test_list_pending_tasks_empty(self, client: "AsyncClient"):
        resp = await client.get("/sheppy/pytest/tasks/pending")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_list_pending_tasks(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        t1, t2 = task_add_fn(1, 2), task_add_fn(3, 4)
        await queue.add([t1, t2])

        resp = await client.get("/sheppy/pytest/tasks/pending")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    async def test_list_pending_tasks_with_limit(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        await queue.add([task_add_fn(i, i) for i in range(5)])

        resp = await client.get("/sheppy/pytest/tasks/pending?limit=3")
        assert resp.status_code == 200
        assert len(resp.json()) == 3

    async def test_list_scheduled_tasks_empty(self, client: "AsyncClient"):
        resp = await client.get("/sheppy/pytest/tasks/scheduled")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_list_scheduled_tasks(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        t = task_add_fn(1, 2)
        await queue.schedule(t, at=timedelta(hours=1))

        resp = await client.get("/sheppy/pytest/tasks/scheduled")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["id"] == str(t.id)


class TestTaskById:
    async def test_get_task(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        t = task_add_fn(1, 2)
        await queue.add(t)

        resp = await client.get(f"/sheppy/pytest/tasks/{t.id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == str(t.id)

    async def test_get_task_not_found(self, client: "AsyncClient"):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        resp = await client.get(f"/sheppy/pytest/tasks/{fake_id}")
        assert resp.status_code == 404


class TestRetryTask:
    async def test_retry_failed_task(self, client: "AsyncClient", queue: Queue, worker: Worker, task_fail_once_fn: Callable[[], Task]):
        t = task_fail_once_fn()

        await queue.add(t)
        await worker.work(1)

        resp = await client.post(f"/sheppy/pytest/tasks/{t.id}/retry")
        assert resp.status_code == 200
        assert resp.json() == f"Task {t.id} queued for retry"
        assert await queue.size() == 1

    async def test_retry_with_delay(self, client: "AsyncClient", queue: Queue, worker: Worker):
        t = failing_task()
        await queue.add(t)
        await worker.work(1)

        resp = await client.post(f"/sheppy/pytest/tasks/{t.id}/retry", json={"delay_seconds": 60})
        assert resp.status_code == 200
        assert resp.json() == f"Task {t.id} queued for retry"
        assert await queue.size() == 0  # scheduled, not pending
        assert len(await queue.get_scheduled()) == 1

    async def test_retry_completed_task_requires_force(
        self, client: "AsyncClient", queue: Queue, worker: Worker, task_add_fn: Callable[[int, int], Task]
    ):
        t = task_add_fn(1, 2)
        await queue.add(t)
        await worker.work(1)

        resp = await client.post(f"/sheppy/pytest/tasks/{t.id}/retry")
        assert resp.status_code == 400

        resp = await client.post(f"/sheppy/pytest/tasks/{t.id}/retry", json={"force": True})
        assert resp.status_code == 200
        assert resp.json() == f"Task {t.id} queued for retry"

    async def test_retry_not_found(self, client: "AsyncClient"):
        fake_id = UUID("00000000-0000-0000-0000-000000000000")
        resp = await client.post(f"/sheppy/pytest/tasks/{fake_id}/retry")
        assert resp.status_code == 400


class TestAddTask:
    async def test_add_task(self, client: "AsyncClient", queue: Queue):
        resp = await client.post(
            "/sheppy/pytest/tasks",
            json={"func": "tests.conftest:async_task_add", "args": [5, 3]},
        )
        assert resp.status_code == 200
        assert await queue.size() == 1

        data = resp.json()
        assert data["spec"]["func"] == "tests.conftest:async_task_add"
        assert data["spec"]["args"] == [5, 3]

    async def test_add_task_with_kwargs(self, client: "AsyncClient", queue: Queue):
        resp = await client.post(
            "/sheppy/pytest/tasks",
            json={
                "func": "tests.conftest:async_task_add",
                "kwargs": {"x": 10, "y": 20},
            },
        )
        assert resp.status_code == 200
        assert await queue.size() == 1
        assert resp.json()["spec"]["kwargs"] == {"x": 10, "y": 20}

    async def test_add_task_invalid_func(self, client: "AsyncClient"):
        resp = await client.post("/sheppy/pytest/tasks", json={"func": "nonexistent:func"})
        assert resp.status_code == 400
        assert "Could not import" in resp.json()["detail"]

    async def test_add_task_func_not_a_task(self, client: "AsyncClient"):
        resp = await client.post("/sheppy/pytest/tasks", json={"func": "os:getcwd"})
        assert resp.status_code == 400
        assert "did not return a Task" in resp.json()["detail"]


class TestScheduleTask:
    async def test_schedule_with_delay(self, client: "AsyncClient", queue: Queue):
        resp = await client.post(
            "/sheppy/pytest/tasks/schedule",
            json={
                "func": "tests.conftest:async_task_add",
                "args": [1, 2],
                "delay_seconds": 60,
            },
        )
        assert resp.status_code == 200
        assert await queue.size() == 0
        assert len(await queue.get_scheduled()) == 1

    async def test_schedule_at_datetime(self, client: "AsyncClient", queue: Queue):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        resp = await client.post(
            "/sheppy/pytest/tasks/schedule",
            json={
                "func": "tests.conftest:async_task_add",
                "args": [1, 2],
                "at": future.isoformat(),
            },
        )
        assert resp.status_code == 200
        assert len(await queue.get_scheduled()) == 1

    async def test_schedule_requires_delay_or_at(self, client: "AsyncClient"):
        resp = await client.post(
            "/sheppy/pytest/tasks/schedule",
            json={"func": "tests.conftest:async_task_add", "args": [1, 2]},
        )
        assert resp.status_code == 400
        assert "must be provided" in resp.json()["detail"]


class TestClearTasks:
    async def test_clear(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        await queue.add([task_add_fn(i, i) for i in range(5)])
        assert await queue.size() == 5

        resp = await client.post("/sheppy/pytest/tasks/clear")
        assert resp.status_code == 200
        assert resp.json() == 5
        assert await queue.size() == 0


class TestCronJobs:
    async def test_list_crons_empty(self, client: "AsyncClient"):
        resp = await client.get("/sheppy/pytest/crons")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_add_cron(self, client: "AsyncClient", queue: Queue):
        resp = await client.post(
            "/sheppy/pytest/crons",
            json={
                "func": "tests.conftest:async_task_add",
                "args": [1, 2],
                "expression": "*/5 * * * *",
            },
        )
        assert resp.status_code == 200
        assert resp.json() is True

        crons = await queue.get_crons()
        assert len(crons) == 1
        assert crons[0].expression == "*/5 * * * *"

    async def test_delete_cron(self, client: "AsyncClient", queue: Queue, task_add_fn: Callable[[int, int], Task]):
        t = task_add_fn(1, 2)
        await queue.add_cron(t, "*/10 * * * *")
        assert len(await queue.get_crons()) == 1

        resp = await client.request(
            "DELETE",
            "/sheppy/pytest/crons",
            json={
                "func": t.spec.func,
                "args": [1, 2],
                "expression": "*/10 * * * *",
            },
        )
        assert resp.status_code == 200
        assert resp.json() == "Cron job deleted"
        assert len(await queue.get_crons()) == 0

    async def test_delete_cron_not_found(self, client: "AsyncClient"):
        resp = await client.request(
            "DELETE",
            "/sheppy/pytest/crons",
            json={
                "func": "tests.conftest:async_task_add",
                "args": [99, 99],
                "expression": "*/99 * * * *",
            },
        )
        assert resp.status_code == 404

    async def test_add_cron_invalid_func(self, client: "AsyncClient"):
        resp = await client.post(
            "/sheppy/pytest/crons",
            json={"func": "nonexistent:func", "expression": "* * * * *"},
        )
        assert resp.status_code == 400


class TestRouterConfiguration:
    async def test_custom_prefix(self, backend: Backend):
        app = FastAPI()
        app.include_router(create_router(backend, prefix="/custom"))

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/custom/queues")
            assert resp.status_code == 200

    async def test_custom_tags(self, backend: Backend):
        app = FastAPI()
        router = create_router(backend, tags=["MyTasks"])
        app.include_router(router)

        for route in router.routes:
            if hasattr(route, "tags"):
                assert "MyTasks" in route.tags
