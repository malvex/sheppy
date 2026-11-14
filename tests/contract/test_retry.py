import asyncio
from collections.abc import Callable
from datetime import timedelta

import pytest

from sheppy import CURRENT_TASK, Queue, Task, Worker, task
from tests.dependencies import (
    assert_is_completed,
    assert_is_failed,
    failing_task,
    simple_async_task,
)


@task(retry=1, retry_delay=0)
async def fail_once_retry_one(current: Task = CURRENT_TASK) -> str:
    if current.retry_count == 0:
        raise Exception("transient error")
    return "ok"


@task(retry=1, retry_delay=0)
async def always_fail_retry_one() -> None:
    raise Exception("permanent error")


@task(retry=2, retry_delay=[60.0, 300.0])
async def always_fail_with_retry_delay_list() -> None:
    raise Exception("permanent error")


async def test_retry_delay_list_uses_first_delay_for_first_retry(queue: Queue, worker: Worker) -> None:
    t = always_fail_with_retry_delay_list()
    await queue.add(t)

    await worker.work(1)

    t = await queue.get_task(t)
    assert t.status == 'retrying'
    assert t.retry_count == 1
    assert t.last_retry_at is not None
    assert t.next_retry_at is not None
    retry_delay = (t.next_retry_at - t.last_retry_at).total_seconds()
    assert retry_delay == pytest.approx(60.0, abs=0.001)


async def test_retry(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker) -> None:
    t = task_fail_once_fn()
    await queue.add(t)

    await worker.work(1)

    t = await queue.get_task(t)
    assert_is_failed(t, status='retrying')
    assert str(t.exception) == "Exception: transient error"
    assert t.retry_count == 1

    await worker.work(1)

    t = await queue.get_task(t)
    assert_is_completed(t)
    assert t.retry_count == 1


async def test_immediate_retry_of_cancelled_task_resets_status(queue: Queue, worker: Worker) -> None:
    worker.enable_scheduler = False
    worker.enable_cron_manager = False

    t = simple_async_task(1, 2)
    await queue.add(t)
    await queue.experimental.cancel(t)

    retried = await queue.retry(t)

    assert retried is not None
    assert retried.status in ('new', 'pending')
    assert retried.finished_at is None

    stored = await queue.get_task(t)
    assert stored is not None
    assert stored.status == retried.status
    assert stored.finished_at is None

    pending = await queue.get_pending()
    assert len(pending) == 1
    assert pending[0].status == retried.status

    await worker.work(oneshot=True)
    processed = await queue.get_task(t)
    assert_is_completed(processed)
    assert processed.result == 3


# regression test for retry off-by-one bug
async def test_retry_one_is_actually_rescheduled(queue: Queue, worker: Worker) -> None:
    t = fail_once_retry_one()
    await queue.add(t)

    await worker.work(1)

    t = await queue.get_task(t)
    assert t.status == 'retrying'
    assert t.retry_count == 1

    await asyncio.wait_for(worker.work(1), timeout=3)

    t = await queue.get_task(t)
    assert_is_completed(t)
    assert t.retry_count == 1
    assert t.result == "ok"

    t2 = always_fail_retry_one()
    await queue.add(t2)

    await asyncio.wait_for(worker.work(2), timeout=5)

    t2 = await queue.get_task(t2)
    assert_is_failed(t2, status='failed')
    assert t2.retry_count == 1

    assert await queue.size() == 0
    assert await queue.get_scheduled() == []


async def test_wait_for(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker):
    t = task_fail_once_fn()
    await queue.add(t)

    asyncio.create_task(worker.work(2))

    recv_task = await queue.wait_for(t, timeout=3)
    assert_is_completed(recv_task)
    assert recv_task.retry_count == 1


@pytest.mark.slow
async def test_wait_for_race(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker):
    t = task_fail_once_fn()
    await queue.add(t)

    worker_task = asyncio.create_task(worker.work(2))

    recv_task = await queue.wait_for(t, timeout=3)
    await worker_task
    assert_is_completed(recv_task)
    assert recv_task.retry_count == 1

    assert await queue.retry(t.id, force=True) is not None

    with pytest.raises(TimeoutError):
        await queue.wait_for(t.id, timeout=0.01)

    worker_task = asyncio.create_task(worker.work(1))

    recv_task2 = await queue.wait_for(t.id, timeout=3)
    await worker_task

    assert recv_task != recv_task2
    assert recv_task.id == recv_task2.id


@pytest.mark.slow
async def test_wait_for_race_w_at(task_fail_once_fn: Callable[[], Task], queue: Queue, worker: Worker):
    t = task_fail_once_fn()
    await queue.add(t)

    asyncio.create_task(worker.work(2))

    recv_task = await queue.wait_for(t, timeout=3)
    assert_is_completed(recv_task)
    assert recv_task.retry_count == 1

    assert await queue.retry(t.id, at=timedelta(seconds=1), force=True) is not None
    assert await queue.size() == 0
    assert len(await queue.get_scheduled()) == 1

    with pytest.raises(TimeoutError):
        await queue.wait_for(t.id, timeout=.01)

    await worker.work(1)

    recv_task2 = await queue.wait_for(t.id, timeout=.1)
    assert recv_task != recv_task2


@pytest.mark.slow
async def test_wait_for_race_no_retriable(queue: Queue, worker: Worker):
    t = failing_task()
    await queue.add(t)

    asyncio.create_task(worker.work(1))

    recv_task = await queue.wait_for(t, timeout=3)
    assert_is_failed(recv_task)
    assert recv_task.retry_count == 0  # non retriable task

    assert await queue.retry(t.id, force=True) is not None

    with pytest.raises(TimeoutError):
        await queue.wait_for(t.id, timeout=0.01)

    asyncio.create_task(worker.work(1))

    recv_task2 = await queue.wait_for(t.id, timeout=1)
    assert recv_task.finished_at != recv_task2.finished_at
