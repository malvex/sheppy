import asyncio
from time import time

from sheppy import Queue, Worker, task

MAX_RATE = 2
RATE_PERIOD = 1.0
TOLERANCE = 0.15


@task(rate_limit={"max_rate": MAX_RATE, "rate_period": RATE_PERIOD, "strategy": "fixed_window"})
async def rate_limited_fixed_window() -> float:
    return time()

@task(rate_limit={"max_rate": MAX_RATE, "rate_period": RATE_PERIOD, "strategy": "sliding_window"})
async def rate_limited_sliding_window() -> float:
    return time()


def assert_rate_limit_compliance(exec_times: list[float], max_rate: int, rate_period: float, tolerance: float = TOLERANCE):
    window = rate_period - tolerance
    for t in exec_times:
        count = sum(1 for t2 in exec_times if t <= t2 < t + window)
        assert count <= max_rate, f"Rate violated: {count} executions in {window}s window (max_rate={max_rate}, times={exec_times})"


def assert_minimum_delays(exec_times: list[float], max_rate: int, rate_period: float, tolerance: float = TOLERANCE):
    start = exec_times[0]
    for i, t in enumerate(exec_times):
        expected_min = (i // max_rate) * rate_period
        actual_delay = t - start
        assert actual_delay >= expected_min - tolerance, f"Task {i} ran too early: delay={actual_delay:.3f}s, expected at least {expected_min}s"


class TestRateLimitFixedWindow:

    async def test_rate_limit(self, queue: Queue, worker: Worker):
        t1 = rate_limited_fixed_window()
        t2 = rate_limited_fixed_window()
        t3 = rate_limited_fixed_window()

        await queue.add([t1, t2, t3])

        worker_task = asyncio.create_task(worker.work(max_tasks=3))
        processed = await queue.wait_for([t1, t2, t3], timeout=5)
        await worker_task

        exec_times = sorted(t.result for t in processed.values())

        assert_rate_limit_compliance(exec_times, MAX_RATE, RATE_PERIOD)
        assert_minimum_delays(exec_times, MAX_RATE, RATE_PERIOD)

    async def test_rate_limit_with_delay(self, queue: Queue, worker: Worker):
        worker_task = asyncio.create_task(worker.work(max_tasks=3))

        t1 = rate_limited_fixed_window()
        await queue.add(t1)

        await asyncio.sleep(0.2)
        t2 = rate_limited_fixed_window()
        await queue.add(t2)

        await asyncio.sleep(0.2)
        t3 = rate_limited_fixed_window()
        await queue.add(t3)

        processed = await queue.wait_for([t1, t2, t3], timeout=5)
        await worker_task

        exec_times = sorted(t.result for t in processed.values())

        assert_rate_limit_compliance(exec_times, MAX_RATE, RATE_PERIOD)
        assert_minimum_delays(exec_times, MAX_RATE, RATE_PERIOD)


class TestRateLimitSlidingWindow:

    async def test_rate_limit(self, queue: Queue, worker: Worker):
        worker_task = asyncio.create_task(worker.work(max_tasks=4))

        t1 = rate_limited_sliding_window()
        t2 = rate_limited_sliding_window()
        t3 = rate_limited_sliding_window()
        t4 = rate_limited_sliding_window()

        await queue.add([t1, t2, t3, t4])

        processed = await queue.wait_for([t1, t2, t3, t4], timeout=10)
        await worker_task

        exec_times = sorted(t.result for t in processed.values())

        assert_rate_limit_compliance(exec_times, MAX_RATE, RATE_PERIOD)
        assert_minimum_delays(exec_times, MAX_RATE, RATE_PERIOD)

    async def test_rate_limit_with_delay(self, queue: Queue, worker: Worker):
        worker_task = asyncio.create_task(worker.work(max_tasks=6))

        t1 = rate_limited_sliding_window()
        await queue.add(t1)

        await asyncio.sleep(0.2)
        t2 = rate_limited_sliding_window()
        t3 = rate_limited_sliding_window()
        t4 = rate_limited_sliding_window()
        t5 = rate_limited_sliding_window()
        t6 = rate_limited_sliding_window()
        await queue.add([t2, t3, t4, t5, t6])

        processed = await queue.wait_for([t1, t2, t3, t4, t5, t6], timeout=10)
        await worker_task

        exec_times = sorted(t.result for t in processed.values())

        assert_rate_limit_compliance(exec_times, MAX_RATE, RATE_PERIOD)
        assert_minimum_delays(exec_times, MAX_RATE, RATE_PERIOD)
