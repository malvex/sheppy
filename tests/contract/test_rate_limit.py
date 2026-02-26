import asyncio
from datetime import datetime

from sheppy import Queue, Worker, task


@task(rate_limit={"max_rate": 2, "rate_period": 1, "strategy": "fixed_window"})
async def rate_limited_fixed_window(created_time: datetime) -> int:
    return int((datetime.now() - created_time).total_seconds()*10)

@task(rate_limit={"max_rate": 2, "rate_period": 1, "strategy": "sliding_window"})
async def rate_limited_sliding_window(created_time: datetime) -> int:
    return int((datetime.now() - created_time).total_seconds()*10)

class TestRateLimitFixedWindow:

    async def test_rate_limit(self, queue: Queue, worker: Worker):
        t1 = rate_limited_fixed_window(datetime.now())
        t2 = rate_limited_fixed_window(datetime.now())
        t3 = rate_limited_fixed_window(datetime.now())

        await queue.add([t1, t2, t3])

        worker_task = asyncio.create_task(worker.work(max_tasks=3))

        processed = await queue.wait_for([t1, t2, t3], timeout=3)

        timer = {}
        for task_id, t in processed.items():
            if t.result not in timer:
                timer[t.result] = []
            timer[t.result].append(task_id)

        assert 0 in timer and len(timer[0]) == 2
        assert 10 in timer and len(timer[10]) == 1

        await worker_task

    async def test_rate_limit_with_delay(self, queue: Queue, worker: Worker):
        worker_task = asyncio.create_task(worker.work(max_tasks=3))

        t1 = rate_limited_fixed_window(datetime.now())
        await queue.add(t1)

        await asyncio.sleep(0.2)
        t2 = rate_limited_fixed_window(datetime.now())
        await queue.add(t2)

        await asyncio.sleep(0.2)
        t3 = rate_limited_fixed_window(datetime.now())
        await queue.add(t3)

        processed = await queue.wait_for([t1, t2, t3], timeout=3)

        timer = {}
        for task_id, t in processed.items():
            if t.result not in timer:
                timer[t.result] = []
            timer[t.result].append(task_id)

        assert 0 in timer and len(timer[0]) == 2
        assert 6 in timer and len(timer[6]) == 1

        await worker_task


class TestRateLimitSlidingWindow:

    async def test_rate_limit(self, queue: Queue, worker: Worker):
        worker_task = asyncio.create_task(worker.work(max_tasks=4))

        t1 = rate_limited_sliding_window(datetime.now())
        t2 = rate_limited_sliding_window(datetime.now())
        t3 = rate_limited_sliding_window(datetime.now())
        t4 = rate_limited_sliding_window(datetime.now())

        await queue.add([t1, t2, t3, t4])


        processed = await queue.wait_for([t1, t2, t3, t4], timeout=10)

        timer = {}
        for task_id, t in processed.items():
            if t.result not in timer:
                timer[t.result] = []
            timer[t.result].append(task_id)

        assert 0 in timer and len(timer[0]) == 2
        # assert 10 in timer and len(timer[10]) == 1
        assert 10 in timer and len(timer[10]) == 2

        await worker_task

    async def test_rate_limit_with_delay(self, queue: Queue, worker: Worker):
        worker_task = asyncio.create_task(worker.work(max_tasks=6))

        t1 = rate_limited_sliding_window(datetime.now())
        await queue.add(t1)

        await asyncio.sleep(0.2)
        t2 = rate_limited_sliding_window(datetime.now())
        t3 = rate_limited_sliding_window(datetime.now())
        t4 = rate_limited_sliding_window(datetime.now())
        t5 = rate_limited_sliding_window(datetime.now())
        t6 = rate_limited_sliding_window(datetime.now())
        await queue.add([t2, t3, t4, t5, t6])


        processed = await queue.wait_for([t1, t2, t3, t4, t5, t6], timeout=5)

        timer = {}
        for task_id, t in processed.items():
            if t.result not in timer:
                timer[t.result] = []
            timer[t.result].append(task_id)

        assert 0 in timer and len(timer[0]) == 2
        assert 8 in timer and len(timer[8]) == 1
        assert 10 in timer and len(timer[10]) == 1
        assert 18 in timer and len(timer[18]) == 1
        assert 20 in timer and len(timer[20]) == 1

        await worker_task
