import asyncio
from collections.abc import Callable
from datetime import timedelta

import pytest

from sheppy import Queue, Task, Worker
from sheppy.testqueue import assert_is_completed, assert_is_failed
from tests.dependencies import failing_task


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
    t = task_fail_once_fn()
    await queue.add(t)

    asyncio.create_task(worker.work(2))

    recv_task = await queue.wait_for(t, timeout=3)
    assert_is_completed(recv_task)
    assert recv_task.retry_count == 1


async def test_wait_for_race(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker):
    t = task_fail_once_fn()
    await queue.add(t)

    asyncio.create_task(worker.work(2))

    recv_task = await queue.wait_for(t, timeout=3)
    assert_is_completed(recv_task)
    assert recv_task.retry_count == 1

    assert await queue.retry(t.id, force=True) is True
    asyncio.create_task(worker.work(2))

    recv_task2 = await queue.wait_for(t.id, timeout=0.1)  # returns immediately (bug)
    assert recv_task == recv_task2  # bug
    assert recv_task.finished_at == recv_task2.finished_at  # bug

    await asyncio.sleep(0.1)

    recv_task3 = await queue.wait_for(t.id, timeout=0.1)
    # recv_task3 = await queue.get_task(t.id)
    assert recv_task != recv_task3
    assert recv_task.finished_at == recv_task2.finished_at  # expected

    pytest.xfail("BUG: race in wait_for (retry should reset result/completion in backend)")


async def test_wait_for_race_w_at(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker):
    t = task_fail_once_fn()
    await queue.add(t)

    asyncio.create_task(worker.work(2))

    recv_task = await queue.wait_for(t, timeout=3)
    assert_is_completed(recv_task)
    assert recv_task.retry_count == 1

    assert await queue.retry(t.id, at=timedelta(seconds=1), force=True) is True
    assert await queue.size() == 0
    assert len(await queue.get_scheduled()) == 1

    recv_task2 = await queue.wait_for(t.id, timeout=.1)  # bug - should timeouted
    assert recv_task == recv_task2

    await worker.work(1)

    recv_task3 = await queue.wait_for(t.id, timeout=.1)
    assert recv_task != recv_task3
    assert recv_task2 != recv_task3

    pytest.xfail("BUG: race in wait_for (retry should reset result/completion in backend)")


async def test_wait_for_race_no_retriable(queue: Queue, worker: Worker):
    t = failing_task()
    await queue.add(t)

    asyncio.create_task(worker.work(1))

    recv_task = await queue.wait_for(t, timeout=3)
    assert_is_failed(recv_task)
    assert recv_task.retry_count == 0  # non retriable task

    assert await queue.retry(t.id, force=True) is True
    asyncio.create_task(worker.work(1))

    recv_task2 = await queue.wait_for(t.id, timeout=0.1)  # returns immediately (bug)
    assert recv_task == recv_task2  # bug
    assert recv_task.finished_at == recv_task2.finished_at  # bug

    await asyncio.sleep(0.1)

    recv_task3 = await queue.wait_for(t.id, timeout=0.1)
    # recv_task3 = await queue.get_task(t.id)
    assert recv_task != recv_task3
    assert recv_task.finished_at == recv_task2.finished_at  # expected

    pytest.xfail("BUG: race in wait_for (retry should reset result/completion in backend)")
