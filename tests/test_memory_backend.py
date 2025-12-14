from collections.abc import Callable

import pytest

from sheppy import Queue, Task
from sheppy.backend import MemoryBackend
from tests.conftest import async_fail_once, sync_fail_once
from tests.dependencies import (
    async_failing_task,
    failing_task,
    simple_async_task,
    simple_sync_task,
)


@pytest.fixture
async def queue():
    return Queue(MemoryBackend(), "pytest")


class TestMemoryBackend:
    async def test_basic_async_task(self, queue: Queue):
        t = simple_async_task(1, 2)

        success = await queue.add(t)
        assert success is True
        # task is processed immediately, result available
        result = await queue.get_task(t)
        assert result is not None
        assert result.status == 'completed'
        assert result.result == 3

    async def test_basic_sync_task(self, queue: Queue):
        t = simple_sync_task(5, 7)

        await queue.add(t)

        result = await queue.get_task(t)
        assert result is not None
        assert result.result == 12

    async def test_failing_sync_task(self, queue: Queue):
        t = failing_task("intentional failure")

        await queue.add(t)

        result = await queue.get_task(t)
        assert result is not None
        assert result.status == 'failed'
        assert "intentional failure" in result.error

    async def test_failing_async_task(self, queue: Queue):
        t = async_failing_task("async failure")

        await queue.add(t)

        result = await queue.get_task(t)
        assert result is not None
        assert result.status == 'failed'
        assert "async failure" in result.error

    async def test_retry_async_task(self, queue: Queue):
        t = async_fail_once()

        await queue.add(t)

        # task should have retried and succeeded
        result = await queue.get_task(t)
        assert result is not None
        assert result.status == 'completed'
        assert result.result == "ok"
        assert result.retry_count == 1

    async def test_retry_sync_task(self, queue: Queue):
        t = sync_fail_once()

        await queue.add(t)

        result = await queue.get_task(t)
        assert result is not None
        assert result.status == 'completed'
        assert result.result == "ok"
        assert result.retry_count == 1

    async def test_task_chaining(self, queue: Queue, task_chaining_fn: Callable[[], Task]):
        t = task_chaining_fn(5)

        await queue.add(t)

        # original task completed and returned a chained task
        result = await queue.get_task(t)
        assert result is not None
        assert result.status == 'completed'
        assert isinstance(result.result, Task)

        # chained task should also be processed
        chained_task = await queue.get_task(result.result)
        assert chained_task is not None
        assert chained_task.status == 'completed'
        assert chained_task.result == 15

    async def test_bulk_task_chaining(self, queue: Queue, task_chaining_bulk_fn: Callable[[], Task]):
        t = task_chaining_bulk_fn(3)

        await queue.add(t)

        # original task completed and returned chained tasks
        result = await queue.get_task(t)
        assert result is not None
        assert result.status == 'completed'
        assert isinstance(result.result, list)
        assert len(result.result) == 2

        # chained tasks should be processed
        chained1 = await queue.get_task(result.result[0])
        chained2 = await queue.get_task(result.result[1])

        assert chained1 is not None and chained1.status == 'completed'
        assert chained2 is not None and chained2.status == 'completed'
        assert {chained1.result, chained2.result} == {8, 9}

    async def test_multiple_tasks(self, queue: Queue):
        tasks = [simple_async_task(i, i) for i in range(5)]

        results = await queue.add(tasks)
        assert results == [True] * 5

        # all tasks processed
        all_tasks = await queue.get_all_tasks()
        assert len(all_tasks) == 5
        assert all(t.status == 'completed' for t in all_tasks)

    async def test_queue_size_is_zero_after_processing(self, queue: Queue):
        await queue.add(simple_async_task(1, 2))

        # size should be 0 because task was processed immediately
        assert await queue.size() == 0

    async def test_get_pending_is_empty(self, queue: Queue):
        await queue.add(simple_async_task(1, 2))

        # no pending tasks - all processed
        pending = await queue.get_pending()
        assert pending == []
