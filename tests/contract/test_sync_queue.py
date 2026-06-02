import asyncio
import multiprocessing
import time
from collections.abc import Generator

import pytest

from sheppy import SyncQueue, Worker
from sheppy.queue import _create_backend_from_url
from tests.dependencies import assert_is_completed, simple_async_task, simple_sync_task


#! FIXME, temp duplicated
def _start_process_worker(queue: str | list[str], backend_string: str, shutdown_timeout: float | None = None):
    worker = Worker(queue, _create_backend_from_url(backend_string))
    worker.heartbeat_timeout = 0.2
    worker.heartbeat_interval = 0.1
    worker.reclaim_interval = 0.1
    worker._blocking_timeout = 0.001
    worker._scheduler_polling_interval = 0.001
    worker._cron_polling_interval = 0.001

    if shutdown_timeout is not None:
        worker.shutdown_timeout = shutdown_timeout

    return asyncio.run(worker.work())


class WorkerInstanceProcess:
    def __init__(self, queue: str | list[str], backend: str):
        self._queue = queue
        self._backend = backend
        self._process: multiprocessing.Process | None = None

    @property
    def is_alive(self):
        return self._process.is_alive() if self._process else False

    def start(self):
        self._process = multiprocessing.Process(target=_start_process_worker, args=(self._queue, self._backend), daemon=True)
        self._process.start()

    def kill(self):
        self._process.kill()
        self._process.join()

    def graceful_shutdown(self):
        # hack to make this work for python 3.13 and lower
        # I should just check python version but I can't remember how to do it
        # and it doesn't really matter as sigint is handled exactly the same as sigterm
        if hasattr(self._process, "interrupt"):
            self._process.interrupt()
        else:
            self._process.terminate()

    def terminate(self):
        self._process.terminate()


QUEUE = "pytest"
BACKEND_STR = "redis://127.0.0.1:6379/5"

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

    def test_memory_backend(self):
        queue = SyncQueue("memory://", "woof")

        queue.add(t := simple_async_task(4, 8))
        processed = queue.wait_for(t)

        assert_is_completed(processed)
        assert processed.result == 12

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

        assert end - start < 1.0

    # def test_context_manager(self, worker: WorkerInstanceProcess):

    #     with SyncQueue(BACKEND_STR, QUEUE) as queue:
    #         queue.add(t := simple_async_task(3, 4))
    #         processed = queue.wait_for(t)

    #     assert_is_completed(processed)
    #     assert processed.result == 7
