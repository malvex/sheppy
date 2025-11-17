from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from sheppy import CURRENT_TASK, Queue, Task, Worker, task
from sheppy.backend import Backend, MemoryBackend, RedisBackend

TEST_QUEUE_NAME = "pytest"


@pytest.fixture
def datetime_now():
    return datetime.now(timezone.utc)


@pytest_asyncio.fixture(params=["memory", "redis"])
async def backend(request: pytest.FixtureRequest) -> AsyncGenerator[Backend, None]:

    if request.param == "memory":

        backend = MemoryBackend()

    elif request.param == "redis":

        backend = RedisBackend(url="redis://localhost:6379/0", consumer_group="pytest")
        await backend.connect()
        await backend._client.flushdb()
        await backend.disconnect()

    else:
        raise NotImplementedError(f"backend {request.param} doesn't exist")

    yield backend


@pytest_asyncio.fixture
async def worker_backend(backend: Backend) -> AsyncGenerator[Backend, None]:
    if isinstance(backend, MemoryBackend):
        # memory backend must be the same instance to access the same task data
        yield backend

    elif isinstance(backend, RedisBackend):

        worker_backend = RedisBackend(url=backend.url, consumer_group=backend.consumer_group)
        yield worker_backend

    else:
        raise NotImplementedError("this should never happen")



@pytest_asyncio.fixture
async def worker(worker_backend: Backend) -> Worker:
    worker = Worker(TEST_QUEUE_NAME, worker_backend)
    worker._blocking_timeout = 0.01
    worker._scheduler_polling_interval = 0.01
    worker._cron_polling_interval = 0.01

    return worker


@pytest_asyncio.fixture
async def queue(backend: Backend) -> Queue:
    return Queue(backend, TEST_QUEUE_NAME)


@task(retry=2, retry_delay=0.1)
async def async_fail_once(current: Task = CURRENT_TASK) -> str:
    if current.retry_count == 0:
        raise Exception("transient error")
    return "ok"


@task(retry=2, retry_delay=0)
def sync_fail_once(current: Task = CURRENT_TASK) -> str:
    if current.retry_count == 0:
        raise Exception("transient error")
    return "ok"


@pytest.fixture(params=["async_task", "sync_task"])
def task_fail_once_fn(request):
    if request.param == "async_task":
        return async_fail_once

    if request.param == "sync_task":
        return sync_fail_once

    raise NotImplementedError
