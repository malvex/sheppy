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
    # is needed to speed up retry logic that is being scheduled (and tests are then waiting for scheduler poll interval).
    # will be fixed once we introduce scheduler notifications instead of polling
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


@task
async def async_task_add(x: int, y: int) -> int:
    return x + y


@task
def sync_task_add(x: int, y: int) -> int:
    return x + y


@pytest.fixture(params=["async_task", "sync_task"])
def task_add_fn(request):
    if request.param == "async_task":
        return async_task_add

    if request.param == "sync_task":
        return sync_task_add

    raise NotImplementedError


@task
async def async_task_chaining(x: int, asynchronous: bool = True) -> Task:
    return async_task_add(x, x*2) if asynchronous else sync_task_add(x, x*2)


@task
def sync_task_chaining(x: int, asynchronous: bool = True) -> Task:
    return async_task_add(x, x*2) if asynchronous else sync_task_add(x, x*2)


@pytest.fixture(params=["async_task", "sync_task"])
def task_chaining_fn(request):
    if request.param == "async_task":
        return async_task_chaining

    if request.param == "sync_task":
        return sync_task_chaining

    raise NotImplementedError


@task
async def async_task_chaining_bulk(x: int, asynchronous: bool = True) -> list[Task]:
    return [
        async_task_add(x, x*2) if asynchronous else sync_task_add(x, x*2),
        async_task_add(x, x+2) if asynchronous else sync_task_add(x, x+2)
    ]


@task
def sync_task_chaining_bulk(x: int, asynchronous: bool = True) -> list[Task]:
    return [
        async_task_add(x, x*2) if asynchronous else sync_task_add(x, x*2),
        async_task_add(x, x+2) if asynchronous else sync_task_add(x, x+2)
    ]


@pytest.fixture(params=["async_task", "sync_task"])
def task_chaining_bulk_fn(request):
    if request.param == "async_task":
        return async_task_chaining_bulk

    if request.param == "sync_task":
        return sync_task_chaining_bulk

    raise NotImplementedError
