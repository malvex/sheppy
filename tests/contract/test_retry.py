import asyncio
from collections.abc import Callable

import pytest

from sheppy import MemoryBackend, Queue, Task, Worker
from sheppy.testqueue import assert_is_completed, assert_is_failed


async def test_retry(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker) -> None:
    t = task_fail_once_fn()
    await queue.add(t)

    await worker.work(1)

    t = await queue.get_task(t)
    assert_is_failed(t)
    assert t.error == "transient error"
    assert t.retry_count == 1

    await worker.work(1)

    t = await queue.get_task(t)
    assert_is_completed(t)
    assert t.retry_count == 1


async def test_wait_for(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker):

    if isinstance(queue.backend, MemoryBackend):
        pytest.xfail("MemoryBackend is broken for this test")  # or maybe redis is...

    t = task_fail_once_fn()
    await queue.add(t)

    worker._blocking_timeout = 0.01
    worker._scheduler_polling_interval = 0.02

    asyncio.create_task(worker.work(2))

    t = await queue.wait_for(t, timeout=10)
    assert_is_failed(t)
    assert t.error == "transient error"
    assert t.retry_count == 1

    t = await queue.wait_for(t, timeout=3)
    assert_is_completed(t)
    assert t.retry_count == 1
