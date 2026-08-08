from datetime import timedelta
from uuid import uuid4

import pytest

from sheppy import Backend, Queue, RedisBackend, Worker
from sheppy.exceptions import TaskCancellationError
from tests.dependencies import (
    assert_is_completed,
    simple_async_task,
    simple_sync_task,
)


@pytest.fixture(params=["async_task", "sync_task"])
def task_fn(request):
    if request.param == "async_task":
        return simple_async_task

    if request.param == "sync_task":
        return simple_sync_task

    raise NotImplementedError


async def test_cancel_pending_task(task_fn, queue: Queue, worker: Worker):
    worker.enable_scheduler = False
    worker.enable_cron_manager = False

    t = task_fn(1, 2)
    await queue.add(t)
    assert await queue.size() == 1

    cancelled = await queue.experimental.cancel(t)

    assert cancelled.status == 'cancelled'
    assert cancelled.finished_at is not None
    assert await queue.size() == 0

    # metadata is kept
    stored = await queue.get_task(t)
    assert stored is not None
    assert stored.status == 'cancelled'
    assert stored.finished_at is not None
    assert stored.result is None

    # the cancelled task must never be executed
    await worker.work(oneshot=True)

    stored = await queue.get_task(t)
    assert stored is not None
    assert stored.status == 'cancelled'
    assert stored.result is None


async def test_cancel_pending_task_by_id(task_fn, queue: Queue):
    t1 = task_fn(1, 2)
    t2 = task_fn(3, 4)
    await queue.add([t1, t2])

    cancelled = await queue.experimental.cancel(t1.id)
    assert cancelled.status == 'cancelled'

    cancelled = await queue.experimental.cancel(str(t2.id))
    assert cancelled.status == 'cancelled'


async def test_cancel_scheduled_task(task_fn, queue: Queue):
    t = task_fn(1, 2)
    await queue.schedule(t, timedelta(minutes=10))
    assert len(await queue.get_scheduled()) == 1

    cancelled = await queue.experimental.cancel(t)

    assert cancelled.status == 'cancelled'
    assert cancelled.finished_at is not None
    assert await queue.get_scheduled() == []
    assert await queue.size() == 0

    stored = await queue.get_task(t)
    assert stored is not None
    assert stored.status == 'cancelled'


async def test_cancel_claimed_task_fails(task_fn, queue: Queue):
    t = task_fn(1, 2)
    await queue.add(t)

    # simulate a worker claiming the task
    claimed = await queue._pop_pending()
    assert len(claimed) == 1

    with pytest.raises(TaskCancellationError):
        await queue.experimental.cancel(t)


async def test_cancel_completed_task_fails(task_fn, queue: Queue, worker: Worker):
    worker.enable_scheduler = False
    worker.enable_cron_manager = False

    t = task_fn(1, 2)
    await queue.add(t)
    await worker.work(1)

    processed = await queue.get_task(t)
    assert_is_completed(processed)

    with pytest.raises(TaskCancellationError):
        await queue.experimental.cancel(t)


async def test_cancel_twice_fails(task_fn, queue: Queue):
    t = task_fn(1, 2)
    await queue.add(t)

    await queue.experimental.cancel(t)

    with pytest.raises(TaskCancellationError):
        await queue.experimental.cancel(t)


async def test_cancel_nonexistent_task_fails(queue: Queue):
    with pytest.raises(TaskCancellationError):
        await queue.experimental.cancel(uuid4())


async def test_wait_for_returns_cancelled_task(task_fn, queue: Queue):
    t = task_fn(1, 2)
    await queue.add(t)

    await queue.experimental.cancel(t)

    finished = await queue.wait_for(t)
    assert finished is not None
    assert finished.status == 'cancelled'


async def test_delete_cancelled_task(task_fn, queue: Queue):
    t = task_fn(1, 2)
    await queue.add(t)
    await queue.experimental.cancel(t)

    assert await queue.experimental.delete(t) is True
    assert await queue.get_task(t) is None

    # already deleted
    assert await queue.experimental.delete(t) is False


async def test_delete_completed_task(task_fn, queue: Queue, worker: Worker):
    worker.enable_scheduler = False
    worker.enable_cron_manager = False

    t = task_fn(1, 2)
    await queue.add(t)
    await worker.work(1)

    assert await queue.experimental.delete(t) is True
    assert await queue.get_task(t) is None


async def test_delete_unfinished_task_fails(task_fn, queue: Queue):
    t = task_fn(1, 2)
    await queue.add(t)

    with pytest.raises(ValueError, match="cancel the task first"):
        await queue.experimental.delete(t)

    # scheduled tasks cannot be deleted either
    t2 = task_fn(1, 2)
    await queue.schedule(t2, timedelta(minutes=10))

    with pytest.raises(ValueError, match="cancel the task first"):
        await queue.experimental.delete(t2)


async def test_delete_nonexistent_task(queue: Queue):
    assert await queue.experimental.delete(uuid4()) is False


#Redis-specific tests for the task_id -> message_id index that cancel() relies on
class TestPendingIndexRedis:

    async def test_index_lifecycle(self, queue: Queue, backend: Backend):
        if not isinstance(backend, RedisBackend):
            pytest.skip("pending index is Redis-specific")

        index_key = f"sheppy:pending_ids:{queue.name}"

        t = simple_async_task(1, 2)
        await queue.add(t)

        # enqueued tasks are indexed
        assert await backend.client.hget(index_key, str(t.id)) is not None

        # claiming the task removes it from the index (making it non-cancellable)
        await queue._pop_pending()
        assert await backend.client.hget(index_key, str(t.id)) is None

    async def test_index_removed_on_cancel(self, queue: Queue, backend: Backend):
        if not isinstance(backend, RedisBackend):
            pytest.skip("pending index is Redis-specific")

        index_key = f"sheppy:pending_ids:{queue.name}"

        t = simple_async_task(1, 2)
        await queue.add(t)
        await queue.experimental.cancel(t)

        assert await backend.client.hget(index_key, str(t.id)) is None

    async def test_cancelled_task_never_executed_even_if_claimed(self, queue: Queue, worker: Worker, backend: Backend):
        if not isinstance(backend, RedisBackend):
            pytest.skip("this race is Redis-specific")

        index_key = f"sheppy:pending_ids:{queue.name}"

        t = simple_async_task(1, 2)
        await queue.add(t)

        # grab the message id, then claim the task (pop removes the index entry)
        message_id = await backend.client.hget(index_key, str(t.id))
        assert message_id is not None
        claimed = await queue._pop_pending()
        assert len(claimed) == 1

        # simulate the cancel landing in the window between the worker's
        # XREADGROUP and the index cleanup (index entry still present)
        await backend.client.hset(index_key, str(t.id), message_id)

        cancelled = await queue.experimental.cancel(t)
        assert cancelled.status == 'cancelled'

        # the worker must skip execution and leave the cancelled state intact
        result = await worker.process_task(claimed[0], queue)
        assert result.status == 'cancelled'
        assert result.result is None

        stored = await queue.get_task(t)
        assert stored is not None
        assert stored.status == 'cancelled'
        assert stored.result is None


async def test_cancelled_task_uses_error_ttl(queue: Queue, backend: Backend):
    if not isinstance(backend, RedisBackend):
        pytest.skip("TTL inspection is Redis-specific")

    t = simple_async_task(1, 2)
    await queue.add(t)
    await queue.experimental.cancel(t)

    ttl = await backend.client.ttl(f"sheppy:tasks:{queue.name}:{t.id}")
    assert ttl > 0
