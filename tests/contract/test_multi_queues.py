from sheppy import Backend, Queue, Worker
from tests.dependencies import (
    simple_async_task,
)


class TestMultipleQueues:

    async def test_multiple_queues(self, backend: Backend, worker_backend: Backend):
        queue1 = Queue(backend, "queue1")
        queue2 = Queue(backend, "queue2")
        queue3 = Queue(backend, "queue3")

        worker = Worker(["queue1", "queue2", "queue3"], backend)
        worker._blocking_timeout = 0.01

        await queue1.add(t1 := simple_async_task(1, 2))
        await queue2.add(t2 := simple_async_task(3, 4))
        await queue3.add(t3 := simple_async_task(5, 6))

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

        assert t1.completed
        assert t2.completed
        assert t3.completed

        assert t1.result == 3
        assert t2.result == 7
        assert t3.result == 11
