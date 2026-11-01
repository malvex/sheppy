import os
import time
from collections.abc import Generator
from datetime import timedelta

import pytest

from sheppy.experimental import SyncQueue
from tests.dependencies import assert_is_completed, simple_async_task, simple_sync_task
from tests.utils import WorkerInstanceProcess

IS_CI = os.getenv("CI", False) == "true"

BACKEND_STR = "redis://127.0.0.1:6379/6"
QUEUE = "pytest"


@pytest.fixture
def worker() -> Generator[None, None, WorkerInstanceProcess]:
    worker = WorkerInstanceProcess(QUEUE, BACKEND_STR)
    worker.start()
    yield worker
    worker.graceful_shutdown()


class TestSyncQueue:

    def test_sync_task(self, worker: WorkerInstanceProcess):
        queue = SyncQueue(BACKEND_STR, QUEUE)

        queue.add(t := simple_sync_task(1, 2))
        processed = queue.wait_for(t)

        assert_is_completed(processed)
        assert processed.result == 3

    def test_async_task(self, worker: WorkerInstanceProcess):
        queue = SyncQueue(BACKEND_STR, QUEUE)

        queue.add(t := simple_async_task(3, 4))
        processed = queue.wait_for(t)

        assert_is_completed(processed)
        assert processed.result == 7

    def test_add_returns_task_or_none_for_duplicate(self):
        queue = SyncQueue("memory://", "add-return-contract")
        try:
            task = simple_sync_task(1, 2)
            assert queue.add(task) is task
            assert queue.add(task) is None
        finally:
            queue.close()

    def test_schedule_returns_task_or_none_for_duplicate(self):
        queue = SyncQueue("memory://", "schedule-return-contract")
        try:
            task = simple_sync_task(1, 2)
            assert queue.schedule(task, timedelta(hours=1)) is task
            assert queue.schedule(task, timedelta(hours=2)) is None
        finally:
            queue.close()

    def test_retry_returns_task_or_none_for_missing_task(self):
        queue = SyncQueue("memory://", "retry-return-contract")
        try:
            task = simple_sync_task(1, 2)
            queue.add(task)
            retried_task = queue.retry(task, force=True)
            assert retried_task is not None
            assert retried_task.id == task.id
            assert queue.retry("00000000-0000-0000-0000-000000000000") is None
        finally:
            queue.close()

    def test_add_cron_returns_cron_or_none_for_duplicate(self):
        queue = SyncQueue("memory://", "cron-return-contract")
        try:
            created_cron = queue.add_cron(simple_sync_task(1, 2), "*/5 * * * *")
            assert created_cron is not None
            assert created_cron.expression == "*/5 * * * *"
            assert queue.add_cron(simple_sync_task(1, 2), "*/5 * * * *") is None
        finally:
            queue.close()

    def test_memory_backend(self):
        queue = SyncQueue("memory://", "woof")

        queue.add(t := simple_async_task(4, 8))
        processed = queue.wait_for(t)

        assert_is_completed(processed)
        assert processed.result == 12

    def test_backend_kwargs(self):
        queue = SyncQueue("memory://", backend_kwargs={"ttl": 5, "error_ttl": 60})
        try:
            assert queue._queue.backend.ttl == 5
            assert queue._queue.backend.error_ttl == 60
        finally:
            queue.close()

    def test_performance(self, worker: WorkerInstanceProcess):
        if IS_CI:
            pytest.skip(reason="CI is randomly too slow")

        start = time.perf_counter()

        queue = SyncQueue(BACKEND_STR, QUEUE)
        tasks = [simple_async_task(i, i*2) for i in range(1000)]

        queue.add(tasks)
        processed = queue.wait_for(tasks)

        assert len(processed) == 1000

        for i, t in enumerate(tasks):
            assert processed[t.id].status == "completed"
            assert processed[t.id].result == i + i*2

        end = time.perf_counter()

        assert end - start < (2.0 if IS_CI else 1.0)

    # def test_context_manager(self, worker: WorkerInstanceProcess):

    #     with SyncQueue(BACKEND_STR, QUEUE) as queue:
    #         queue.add(t := simple_async_task(3, 4))
    #         processed = queue.wait_for(t)

    #     assert_is_completed(processed)
    #     assert processed.result == 7
