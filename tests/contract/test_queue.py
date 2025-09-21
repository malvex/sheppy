import asyncio
from datetime import timedelta
from uuid import UUID

import pytest

from sheppy import Backend, Queue, Worker
from sheppy.testqueue import assert_is_completed, assert_is_new
from tests.dependencies import simple_async_task, simple_sync_task


@pytest.fixture(params=["async_task", "sync_task"])
def task_fn(request):
    if request.param == "async_task":
        return simple_async_task

    if request.param == "sync_task":
        return simple_sync_task

    raise NotImplementedError


async def test_add(task_fn, queue: Queue, worker: Worker):
        worker.enable_scheduler = False
        worker.enable_cron_manager = False

        t = task_fn(1, 2)
        assert_is_new(t)

        await queue.add(t)

        assert await queue.size() == 1
        await worker.work(1)
        assert await queue.size() == 0

        processed = await queue.get_task(t)
        assert_is_completed(processed)
        assert processed.result == 3


@pytest.mark.parametrize("t", [
     None,
     simple_async_task,
     lambda: None,
     42,
     [simple_async_task],
     [5],
     [None],
     [simple_async_task(1, 2), None, simple_async_task(3, 4)]
])
async def test_add_invalid(t, queue: Queue):
     with pytest.raises(AttributeError):
        await queue.add(t)


async def test_get_task(task_fn, queue: Queue, worker: Worker):
        worker.enable_scheduler = False
        worker.enable_cron_manager = False

        t = task_fn(1, 2)
        assert_is_new(t)

        assert await queue.get_task(t) is None
        assert await queue.get_task(t.id) is None
        assert await queue.get_task(str(t.id)) is None

        await queue.add(t)

        assert await queue.get_task(t) == t
        assert await queue.get_task(t.id) == t
        assert await queue.get_task(str(t.id)) == t

        assert await queue.size() == 1
        await worker.work(1)
        assert await queue.size() == 0

        processed = await queue.get_task(t)
        assert_is_completed(processed)
        assert processed.result == 3
        assert await queue.get_task(t.id) == processed
        assert await queue.get_task(str(t.id)) == processed


async def test_get_task_nonexistent(queue: Queue):
        assert await queue.get_task(None) is None
        assert await queue.get_task(UUID('00000000-0000-0000-0000-000000000000')) is None
        assert await queue.get_task('00000000-0000-0000-0000-000000000000') is None


async def test_peek(task_fn, queue: Queue, worker: Worker):
        worker.enable_scheduler = False
        worker.enable_cron_manager = False

        assert await queue.size() == 0
        res = await queue.peek()
        assert len(res) == 0

        await queue.add(t1 := task_fn(1, 2))
        await queue.add(t2 := task_fn(3, 4))
        assert await queue.size() == 2

        # count < queue size + implicit
        res = await queue.peek()
        assert len(res) == 1
        assert res[0] == t1
        assert await queue.size() == 2

        # count < queue size + explicit
        res = await queue.peek(count=1)
        assert len(res) == 1
        assert res[0] == t1
        assert await queue.size() == 2

        # count == queue size
        res = await queue.peek(count=2)
        assert len(res) == 2
        assert res[0] == t1
        assert res[1] == t2
        assert await queue.size() == 2

        # count > queue size
        res = await queue.peek(count=9999)
        assert len(res) == 2
        assert res[0] == t1
        assert res[1] == t2
        assert await queue.size() == 2

        await worker.work(1)

        # peek after task processed
        res = await queue.peek(count=2)
        assert len(res) == 1
        assert res[0] == t2
        assert await queue.size() == 1

        await worker.work(1)

        res = await queue.peek(count=2)
        assert len(res) == 0
        assert await queue.size() == 0


async def test_peek_after_pop(task_fn, queue: Queue, worker: Worker):
        worker.enable_scheduler = False
        worker.enable_cron_manager = False

        await queue.add(t := task_fn(1, 2))
        assert await queue.size() == 1

        res = await queue.peek()
        assert len(res) == 1
        assert res[0] == t
        assert await queue.size() == 1

        await queue._pop()

        if queue.backend.__class__.__name__ == "RedisBackend":
            pytest.xfail("bug(RedisBackend): popped tasks still visible in peek()")

        res = await queue.peek()
        assert len(res) == 0
        # assert res[0] == t
        assert await queue.size() == 0


async def test_peek_invalid(queue: Queue):
    with pytest.raises(ValueError):
        await queue.peek(count=0)

    with pytest.raises(ValueError):
        await queue.peek(count=-1)


async def test_clear(task_fn, queue: Queue, worker: Worker):
        assert await queue.size() == 0

        t1 = task_fn(1, 2)
        t2 = task_fn(3, 4)
        t3 = task_fn(5, 6)

        await queue.add(t1)
        await queue.add(t2)
        await queue.add(t3)
        assert await queue.size() == 3

        await worker.work(1)

        res = await queue._pop()
        assert len(res) == 1
        assert res[0] == t2

        ret = await queue.clear()
        assert ret == 3

        assert await queue.size() == 0
        assert await queue.peek() == []

        ret = await queue.get_all_tasks()
        assert await queue.get_all_tasks() == []


async def test_pop(task_fn, queue: Queue):
        assert await queue.size() == 0
        await queue.add(t1 := task_fn(1, 2))
        await queue.add(t2 := task_fn(3, 4))
        await queue.add(t3 := task_fn(5, 6))
        await queue.add(t4 := task_fn(7, 8))
        await queue.add(t5 := task_fn(9, 10))
        await queue.add(t6 := task_fn(11, 12))
        await queue.add(t7 := task_fn(13, 14))
        await queue.add(t8 := task_fn(15, 16))
        assert await queue.size() == 8

        assert await queue._pop() == [t1]
        assert await queue._pop(timeout=0.01) == [t2]
        assert await queue._pop(limit=2, timeout=0.01) == [t3, t4]
        assert await queue._pop(limit=1) == [t5]
        assert await queue._pop(limit=2) == [t6, t7]
        assert await queue._pop(limit=2) == [t8]

        assert await queue._pop() == []
        assert await queue._pop(limit=2) == []
        assert await queue._pop(timeout=0.01) == []
        assert await queue._pop(limit=2, timeout=0.01) == []


async def test_pop_invalid(queue: Queue):
    with pytest.raises(ValueError):
        assert await queue._pop(0)

    with pytest.raises(ValueError):
        assert await queue._pop(-1)

    with pytest.raises(TypeError):
        assert await queue._pop(None)


async def test_get_all_tasks(task_fn, queue: Queue, worker: Worker):
        assert await queue.get_all_tasks() == []

        tasks = [
            task_fn(1, 2),
            task_fn(3, 4),
            task_fn(5, 6),
            task_fn(7, 8),
            task_fn(9, 10),
            task_fn(11, 12),
            task_fn(13, 14),
            task_fn(15, 16),
        ]
        tasks_order = {t.id: i for i, t in enumerate(tasks)}

        assert await queue.size() == 0
        await queue.add(tasks)
        assert await queue.size() == 8

        all_tasks = await queue.get_all_tasks()
        assert len(all_tasks) == 8

        all_tasks.sort(key=lambda t: tasks_order[t.id])
        assert all_tasks == tasks

        await worker.work(4)
        res = await queue._pop(limit=2)
        assert res == [tasks[4], tasks[5]]

        all_tasks = await queue.get_all_tasks()
        assert len(all_tasks) == 8

        all_tasks.sort(key=lambda t: tasks_order[t.id])
        assert all_tasks[0].id == tasks[0].id
        assert all_tasks[0].completed is True
        assert all_tasks[0] != tasks[0]
        assert all_tasks[1].id == tasks[1].id
        assert all_tasks[2].id == tasks[2].id
        assert all_tasks[3].id == tasks[3].id
        assert all_tasks[4:] == tasks[4:]


async def test_wait_for(task_fn, queue: Queue, worker: Worker):
    await queue.add(t := task_fn(1, 2))

    assert await queue.size() == 1

    asyncio.create_task(worker.work(1))

    processed = await queue.wait_for_result(t, timeout=1)

    assert processed.completed
    assert not processed.error
    assert processed.result == 3


async def test_wait_for_nonexistent(queue: Queue):
    assert await queue.size() == 0

    with pytest.raises(TimeoutError):
        await queue.wait_for_result('00000000-0000-0000-0000-000000000000', timeout=0.01)

    ret = await queue.wait_for_result('00000000-0000-0000-0000-000000000000', timeout=None)
    assert ret is None


class TestBatchOperations:
    async def test_batch_add(self, task_fn, queue: Queue, worker: Worker):
        worker.enable_scheduler = False
        worker.enable_cron_manager = False

        tasks = [task_fn(i, i) for i in range(1000)]

        await queue.add(tasks)

        assert await queue.size() == 1000

        await worker.work(1000)

        processed = []
        for t in tasks:
            processed.append(await queue.get_task(t))

        for i, t in enumerate(processed):
            assert t.completed
            assert not t.error
            assert t.result == 2 * i

    async def test_batch_add_one(self, task_fn, queue: Queue, worker: Worker):
        tasks = [task_fn(1, 1)]

        assert await queue.size() == 0
        success = await queue.add([])
        assert success
        assert await queue.size() == 0

        success = await queue.add(tasks)
        assert success

        assert await queue.size() == 1

        await worker.work(1)

        t = await queue.get_task(tasks[0])

        assert t.completed
        assert not t.error
        assert t.result == 2

    async def test_batch_add_empty(self, queue: Queue):
        await queue.add([])

        assert await queue.size() == 0
        assert await queue.get_all_tasks() == []

    async def test_batch_wait_for(self, task_fn, queue: Queue, worker: Worker):
        tasks = [task_fn(i, i) for i in range(10)]

        await queue.add(tasks)

        assert await queue.size() == 10

        asyncio.create_task(worker.work(10))

        processed = []
        for t in tasks:
            processed.append(await queue.wait_for_result(t, timeout=10))

        for i, t in enumerate(processed):
            assert t.completed
            assert not t.error
            assert t.result == 2 * i


class TestMultipleQueues:
    async def test_multiple_queues(self, task_fn, backend: Backend):
        queue1 = Queue(backend, "queue1")
        queue2 = Queue(backend, "queue2")
        queue3 = Queue(backend, "queue3")

        worker = Worker(["queue1", "queue2", "queue3"], backend)
        worker._blocking_timeout = 0.01
        worker._scheduler_polling_interval = 0.01
        worker._cron_polling_interval = 0.01

        await queue1.add(t1 := task_fn(1, 2))
        await queue2.add(t2 := task_fn(3, 4))
        await queue3.add(t3 := task_fn(5, 6))

        assert await queue1.size() == 1
        assert await queue2.size() == 1
        assert await queue3.size() == 1

        await worker.work(max_tasks=3)

        assert await queue1.size() == 0
        assert await queue2.size() == 0
        assert await queue3.size() == 0

        t1 = await queue1.get_task(t1)
        t2 = await queue2.get_task(t2)
        t3 = await queue3.get_task(t3)

        assert_is_completed(t1)
        assert_is_completed(t2)
        assert_is_completed(t3)

        assert t1.result == 3
        assert t2.result == 7
        assert t3.result == 11


class TestEdgeCases:
    @pytest.mark.skip("BUG")
    async def test_adding_task_twice(self, task_fn, queue: Queue):

        t1 = task_fn(1, 2)
        t2 = task_fn(3, 4)
        t3 = task_fn(5, 6)

        await queue.add(t1)
        await queue.add(t1)  # should fail
        await queue.add([t1])  # should fail
        await queue.schedule(t1, at=timedelta(seconds=10))  # should also fail

        await queue.add([t2, t2])  # should fail
        await queue.add([t3, t1])  # should fail

        assert await queue.size() == 3

    @pytest.mark.skip("BUG")
    async def test_schedule_task_twice(self, task_fn, queue: Queue):

        t1 = task_fn(1, 2)

        await queue.schedule(t1, at=timedelta(seconds=10))
        await queue.schedule(t1, at=timedelta(seconds=10))  # should fail
        await queue.schedule(t1, at=timedelta(hours=2))  # different schedule time should also fail
        await queue.add(t1)  # should also fail

        assert len(await queue.list_scheduled()) == 1
        assert await queue.size() == 0
