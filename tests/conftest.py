from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from sheppy import Queue, Worker
from sheppy.backend import Backend, MemoryBackend, RedisBackend

TEST_QUEUE_NAME = "pytest-queue"


@pytest_asyncio.fixture(params=["memory", "redis"])
async def backend(request: pytest.FixtureRequest) -> AsyncGenerator[Backend, None]:

    if request.param == "memory":

        backend = MemoryBackend()
        await backend.connect()

    elif request.param == "redis":

        backend = RedisBackend(url="redis://localhost:6379/0", consumer_group="group-pytest")
        await backend.connect()
        await backend._client.flushdb()

    else:
        raise NotImplementedError(f"backend {request.param} doesn't exist")

    yield backend


@pytest_asyncio.fixture
async def worker_backend(backend: Backend) -> AsyncGenerator[Backend, None]:
    if isinstance(backend, MemoryBackend):
        # Memory backend must be the same instance to access the same task data
        yield backend

    elif isinstance(backend, RedisBackend):

        worker_backend = RedisBackend(url=backend.url, consumer_group=backend.consumer_group)
        await worker_backend.connect()
        yield worker_backend

    else:
        raise NotImplementedError("this should never happen")



@pytest_asyncio.fixture
async def worker(worker_backend: Backend) -> Worker:
    return Worker(TEST_QUEUE_NAME, worker_backend)


@pytest_asyncio.fixture
async def queue(backend: Backend) -> Queue:
    return Queue(TEST_QUEUE_NAME, backend)
