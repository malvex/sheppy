import asyncio

import pytest

from sheppy import Queue, RedisBackend, Worker, task


@task
def ttl_success_task() -> int:
    return 42


@task
def ttl_fail_task() -> int:
    raise Exception("test exception")


@task(ttl=5, error_ttl=50)
def ttl_success_task_override() -> int:
    return 42


@task(ttl=5, error_ttl=50)
def ttl_fail_task_override() -> int:
    raise Exception("test exception")


@task(ttl=None, error_ttl=None)
def ttl_success_task_override_to_none() -> int:
    return 42


@task(ttl=None, error_ttl=None)
def ttl_fail_task_override_to_none() -> int:
    raise Exception("test exception")


@pytest.fixture
async def redis_backend():
    backend = RedisBackend(url="redis://localhost:6379/0", ttl=1, error_ttl=5)
    await backend.connect()
    await backend._client.flushdb()
    yield backend
    await backend._client.flushdb()
    await backend.disconnect()


@pytest.fixture
async def redis_backend_no_error_ttl():
    backend = RedisBackend(url="redis://localhost:6379/0", ttl=10, error_ttl=None)
    await backend.connect()
    await backend._client.flushdb()
    yield backend
    await backend._client.flushdb()
    await backend.disconnect()


QUEUE_NAME = "pytest_ttl"

@pytest.fixture
async def queue(redis_backend):
    return Queue(redis_backend, QUEUE_NAME)


@pytest.fixture
async def worker(redis_backend):
    worker = Worker(QUEUE_NAME, redis_backend)
    worker._blocking_timeout = 0.001
    worker._scheduler_polling_interval = 0.001
    worker._cron_polling_interval = 0.001
    return worker


class TestRedisTTLWellKindaUnitTestRatherThanIntegrationButItConnectsToRealRedisAndItIsQuickTestSoDealWithIt:
    async def test_completed_task_uses_success_ttl(self, queue: Queue, worker: Worker):
        assert await queue.add(t := ttl_success_task()) is True
        await worker.work(max_tasks=1)

        ttl = await queue.backend._client.ttl(f"sheppy:tasks:{QUEUE_NAME}:{t.id}")
        assert 0 < ttl <= 1

    async def test_failed_task_uses_error_ttl(self, queue: Queue, worker: Worker):
        assert await queue.add(t := ttl_fail_task()) is True
        await worker.work(max_tasks=1)

        ttl = await queue.backend._client.ttl(f"sheppy:tasks:{QUEUE_NAME}:{t.id}")
        assert 2 < ttl <= 5

    async def test_completed_task_uses_success_ttl_override(self, queue: Queue, worker: Worker):
        assert await queue.add(t := ttl_success_task_override()) is True
        await worker.work(max_tasks=1)

        ttl = await queue.backend._client.ttl(f"sheppy:tasks:{QUEUE_NAME}:{t.id}")
        assert 1 < ttl <= 5

    async def test_failed_task_uses_error_ttl_override(self, queue: Queue, worker: Worker):
        assert await queue.add(t := ttl_fail_task_override()) is True
        await worker.work(max_tasks=1)

        ttl = await queue.backend._client.ttl(f"sheppy:tasks:{QUEUE_NAME}:{t.id}")
        assert 10 < ttl <= 50

    async def test_completed_task_uses_success_ttl_override_to_none(self, queue: Queue, worker: Worker):
        assert await queue.add(t := ttl_success_task_override_to_none()) is True
        await worker.work(max_tasks=1)

        ttl = await queue.backend._client.ttl(f"sheppy:tasks:{QUEUE_NAME}:{t.id}")
        assert ttl == -1  # redis returns -1 for keys with no expiration

    async def test_failed_task_uses_error_ttl_override_to_none(self, queue: Queue, worker: Worker):
        assert await queue.add(t := ttl_fail_task_override_to_none()) is True
        await worker.work(max_tasks=1)

        ttl = await queue.backend._client.ttl(f"sheppy:tasks:{QUEUE_NAME}:{t.id}")
        assert ttl == -1

    async def test_error_ttl_none_means_no_expiration(self, redis_backend_no_error_ttl: RedisBackend):
        queue = Queue(redis_backend_no_error_ttl, QUEUE_NAME)
        worker = Worker(QUEUE_NAME, redis_backend_no_error_ttl)
        worker._blocking_timeout = 0.001
        worker._scheduler_polling_interval = 0.001
        worker._cron_polling_interval = 0.001

        assert await queue.add(t := ttl_fail_task()) is True
        await worker.work(max_tasks=1)

        ttl = await queue.backend._client.ttl(f"sheppy:tasks:{QUEUE_NAME}:{t.id}")
        assert ttl == -1


@pytest.mark.slow
class TestActualRealContractTestThatWillBeQuiteSlowUnfortunatelyButHereItIs:
    async def test_this_test_is_really_really_slow(self, queue: Queue, worker: Worker):
        t1 = ttl_success_task()
        t2 = ttl_fail_task()
        t3 = ttl_success_task_override()
        t4 = ttl_fail_task_override()
        t5 = ttl_success_task_override_to_none()
        t6 = ttl_fail_task_override_to_none()

        tasks = [t1, t2, t3, t4, t5, t6]

        await queue.add(tasks)

        await worker.work(len(tasks))

        # should still have all
        tasks1 = await queue.get_task(tasks)
        assert len(tasks1) == 6
        assert t1.id in tasks1
        assert t2.id in tasks1
        assert t3.id in tasks1
        assert t4.id in tasks1
        assert t5.id in tasks1
        assert t6.id in tasks1

        await asyncio.sleep(1.1)

        # t1 should be gone
        tasks2 = await queue.get_task(tasks)
        assert len(tasks2) == 5
        assert t1.id not in tasks2
        assert t2.id in tasks2
        assert t3.id in tasks2
        assert t4.id in tasks2
        assert t5.id in tasks2
        assert t6.id in tasks2

        await asyncio.sleep(4.1)

        # more should be gone after 5s ...
        tasks3 = await queue.get_task(tasks)
        assert len(tasks3) == 3
        assert t1.id not in tasks3
        assert t2.id not in tasks3
        assert t3.id not in tasks3
        assert t4.id in tasks3
        assert t5.id in tasks3
        assert t6.id in tasks3

        # (note: commented out to speed up CI)
        #
        # # now wait infinity seconds to verify that task will still be there without ttl configured
        # await asyncio.sleep(∞)
        # tasks4 = await queue.get_task(tasks)
        # assert len(tasks4) == 3
        # assert t4.id in tasks4
        # assert t5.id in tasks4
        # assert t6.id in tasks4
