import asyncio

from sheppy import Queue, task


@task
async def long_running_task(sleep: float):
    await asyncio.sleep(sleep)
    return 67


@task(retry_on_crash=True, retry=3)
async def idempotent_long_running_task(sleep: float):
    await asyncio.sleep(sleep)
    return 67


class TestWorkerInstanceProcess:
    async def test_graceful_shutdown(self, worker_process_factory):
        worker = worker_process_factory()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is True

        worker.graceful_shutdown()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is False
        assert worker.exitcode == 0

    # should behave exactly the same as test_graceful_shutdown
    async def test_terminate(self, worker_process_factory):
        worker = worker_process_factory()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is True

        worker.terminate()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is False
        assert worker.exitcode == 0

    async def test_kill(self, worker_process_factory):
        worker = worker_process_factory()
        assert worker.is_alive is True

        await asyncio.sleep(1)
        assert worker.is_alive is True

        worker.kill()
        assert worker.is_alive is False
        assert worker.exitcode == -9

    async def test_successful_task(self, queue2: Queue, worker_process_factory):
        await queue2.add(t := long_running_task(0.1))
        worker = worker_process_factory()

        await asyncio.sleep(1)
        worker.kill()
        assert worker.is_alive is False

        t = await queue2.get_task(t)
        assert t.status == "completed"

    async def test_graceful_shutdown_successful_task(self, queue2: Queue, worker_process_factory):
        await queue2.add(t := long_running_task(5))
        worker = worker_process_factory()
        await asyncio.sleep(1)
        worker.graceful_shutdown()

        await asyncio.sleep(2)
        assert worker.is_alive is True

        # should not be done yet
        t = await queue2.get_task(t)
        assert t.status == "new"  # is-running

        await asyncio.sleep(3)
        assert worker.is_alive is False

        t = await queue2.get_task(t)
        assert t.status == "completed"


    async def test_graceful_shutdown_failed_task(self, queue2: Queue, worker_process_factory):
        await queue2.add(t := long_running_task(10))
        worker1 = worker_process_factory()

        await asyncio.sleep(0.5)
        worker2 = worker_process_factory()

        await asyncio.sleep(0.5)
        worker1.kill()
        assert worker1.is_alive is False

        await asyncio.sleep(0.5)

        t = await queue2.get_task(t)
        assert t.status == "crashed"
        worker2.kill()

    async def test_crashed_task(self, queue2: Queue, worker_process_factory):
        await queue2.add(t := long_running_task(10))
        worker1 = worker_process_factory()

        await asyncio.sleep(0.5)
        worker2 = worker_process_factory()

        await asyncio.sleep(0.5)
        worker1.kill()
        assert worker1.is_alive is False

        await asyncio.sleep(0.5)

        t = await queue2.get_task(t)
        assert t.status == "crashed"
        worker2.kill()

    async def test_crashed_task_retry(self, queue2: Queue, worker_process_factory):
        await queue2.add(t := idempotent_long_running_task(3))
        worker1 = worker_process_factory()

        await asyncio.sleep(0.5)
        worker2 = worker_process_factory()

        await asyncio.sleep(0.5)
        worker1.kill()
        assert worker1.is_alive is False

        await asyncio.sleep(0.5)

        t = await queue2.get_task(t)
        assert t.status == "retrying"  # crashed

        await asyncio.sleep(1)

        t = await queue2.get_task(t)
        assert t.status == "pending"  # in-progress

        await asyncio.sleep(3)

        t = await queue2.get_task(t)
        assert t.status == "completed"

        worker2.kill()

    async def test_kill_many_workers(self, worker_process_factory):
        workers = [worker_process_factory() for _ in range(10)]

        await asyncio.sleep(1)
        [w.kill() for w in workers[0:8]]

        for w in workers[0:8]:
            assert w.is_alive is False

        assert workers[9].is_alive is True
        workers[9].kill()
        assert workers[9].is_alive is False
