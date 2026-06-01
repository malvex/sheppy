import asyncio
import multiprocessing

import pytest_asyncio

from sheppy import Queue, Worker, task
from sheppy.queue import _create_backend_from_url


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


@task
async def long_running_task(sleep: float):
    await asyncio.sleep(sleep)
    return 67


@task(retry_on_crash=True, retry=3)
async def idempotent_long_running_task(sleep: float):
    await asyncio.sleep(sleep)
    return 67


QUEUE = "pytest"
BACKEND_STR = "redis://127.0.0.1:6379/5"


@pytest_asyncio.fixture(scope="function")
async def queue():
    queue = Queue(BACKEND_STR, QUEUE)
    await queue.clear()
    yield queue
    await queue.clear()


class TestWorkerInstanceProcess:
    async def test_graceful_shutdown(self):
        worker = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker.start()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is True

        worker.graceful_shutdown()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is False
        assert worker._process.exitcode == 0

    # should behave exactly the same as test_graceful_shutdown
    async def test_terminate(self):
        worker = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker.start()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is True

        worker.terminate()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is False
        assert worker._process.exitcode == 0

    async def test_kill(self):
        worker = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker.start()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is True

        worker.kill()
        assert worker.is_alive is False
        assert worker._process.exitcode == -9

    async def test_successful_task(self, queue: Queue):
        await queue.add(t := long_running_task(0.1))
        worker = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker.start()

        await asyncio.sleep(1)
        worker.kill()
        assert worker.is_alive is False

        t = await queue.get_task(t)
        assert t.status == "completed"

    async def test_graceful_shutdown_successful_task(self, queue: Queue):
        await queue.add(t := long_running_task(5))
        worker = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker.start()

        await asyncio.sleep(1)

        worker.graceful_shutdown()

        await asyncio.sleep(2)
        assert worker.is_alive is True

        # should not be done yet
        t = await queue.get_task(t)
        assert t.status == "new"  # is-running

        await asyncio.sleep(3)
        assert worker.is_alive is False

        t = await queue.get_task(t)
        assert t.status == "completed"


    async def test_graceful_shutdown_failed_task(self, queue: Queue):
        await queue.add(t := long_running_task(10))
        worker1 = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker1.start()

        await asyncio.sleep(0.5)
        worker2 = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker2.start()

        await asyncio.sleep(0.5)
        worker1.kill()
        assert worker1.is_alive is False

        await asyncio.sleep(0.5)

        t = await queue.get_task(t)
        assert t.status == "crashed"
        worker2.kill()

    async def test_crashed_task(self, queue: Queue):
        await queue.add(t := long_running_task(10))
        worker1 = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker1.start()

        await asyncio.sleep(0.5)
        worker2 = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker2.start()

        await asyncio.sleep(0.5)
        worker1.kill()
        assert worker1.is_alive is False

        await asyncio.sleep(0.5)

        t = await queue.get_task(t)
        assert t.status == "crashed"
        worker2.kill()

    async def test_crashed_task_retry(self, queue: Queue):
        await queue.add(t := idempotent_long_running_task(3))
        worker1 = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker1.start()

        await asyncio.sleep(0.5)
        worker2 = WorkerInstanceProcess(QUEUE, BACKEND_STR)
        worker2.start()

        await asyncio.sleep(0.5)
        worker1.kill()
        assert worker1.is_alive is False

        await asyncio.sleep(0.5)

        t = await queue.get_task(t)
        assert t.status == "retrying"  # crashed

        await asyncio.sleep(1)

        t = await queue.get_task(t)
        assert t.status == "pending"  # in-progress

        await asyncio.sleep(3)

        t = await queue.get_task(t)
        assert t.status == "completed"

        worker2.kill()

    async def test_kill_many_workers(self):
        workers = [WorkerInstanceProcess(QUEUE, BACKEND_STR) for _ in range(10)]

        [w.start() for w in workers]
        await asyncio.sleep(1)
        [w.kill() for w in workers[0:8]]

        for w in workers[0:8]:
            assert w.is_alive is False

        assert workers[9].is_alive is True
        workers[9].kill()
        assert workers[9].is_alive is False
