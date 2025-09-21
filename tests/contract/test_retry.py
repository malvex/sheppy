import asyncio

import pytest

from sheppy import MemoryBackend, Queue, Task, Worker, task


@task(retry=2, retry_delay=2)
async def async_fail_once(self: Task) -> str:
    if self.retry_count == 0:
        raise Exception("transient error")

    return "ok"


@task(retry=2, retry_delay=0)
def sync_fail_once(self: Task) -> str:
    if self.retry_count == 0:
        raise Exception("transient error")

    return "ok"


@pytest.fixture(params=["async_task", "sync_task"])
def fail_once_fn(request):
    if request.param == "async_task":
        return async_fail_once

    if request.param == "sync_task":
        return sync_fail_once

    raise NotImplementedError


async def test_retry(fail_once_fn, queue: Queue, worker: Worker):
    t = fail_once_fn()
    await queue.add(t)

    await worker.work(1)

    t = await queue.get_task(t)
    assert not t.completed
    assert t.error == "transient error"
    assert t.retry_count == 1

    await worker.work(1)

    t = await queue.get_task(t)
    assert t.completed
    assert t.retry_count == 1


async def test_wait_for_result(fail_once_fn, queue: Queue, worker: Worker):

    if isinstance(queue.backend, MemoryBackend):
        pytest.xfail("MemoryBackend is broken for this test")

    t = fail_once_fn()
    await queue.add(t)

    worker._blocking_timeout = 0.01
    worker._scheduler_polling_interval = 0.02

    asyncio.create_task(worker.work(2))

    t = await queue.wait_for_result(t, timeout=10)

    assert not t.completed
    assert t.finished_at is None
    assert t.error == "transient error"
    assert t.retry_count == 1

    t = await queue.wait_for_result(t, timeout=3)
    assert t.completed
    assert t.finished_at is not None
    assert not t.error
    assert t.retry_count == 1
