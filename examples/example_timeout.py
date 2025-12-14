import asyncio

from sheppy import TestQueue, task


@task
async def sleepy_task_no_timeout(how_long: float):
    await asyncio.sleep(how_long)


@task(timeout=1)
async def sleepy_task(how_long: float):
    await asyncio.sleep(how_long)

@task(retry=3, timeout=1)
async def sleepy_task_with_retry(how_long: float):
    await asyncio.sleep(how_long)

@task(retry=3, timeout=1, retry_on_timeout=True)
async def sleepy_task_with_retry_true(how_long: float):
    await asyncio.sleep(how_long)

@task(retry=3, timeout=1, retry_on_timeout=False)
async def sleepy_task_with_retry_false(how_long: float):
    await asyncio.sleep(how_long)


def test_timeout():
    q = TestQueue()

    q.add(sleepy_task(0.5))
    q.add(sleepy_task_no_timeout(2))
    q.add(sleepy_task(2))

    processed1 = q.process_next()
    processed2 = q.process_next()
    processed3 = q.process_next()

    print(processed1)
    print(processed2)
    print(processed3)

    assert processed1.status == 'completed'
    assert processed2.status == 'completed'
    assert processed3.status == 'failed'
    assert processed3.error == "TaskTimeoutError: Task exceeded timeout of 1.0 seconds"


def test_timeout_with_retry():
    q = TestQueue()

    q.add(sleepy_task_with_retry(2))

    processed = q.process_all()
    assert len(processed) == 1
    assert processed[0].retry_count == 0

def test_timeout_with_retry_true():
    q = TestQueue()

    q.add(sleepy_task_with_retry_true(2))

    processed = q.process_all()
    assert len(processed) == 3
    assert processed[0].retry_count == 1
    assert processed[1].retry_count == 2
    assert processed[2].retry_count == 3

def test_timeout_with_retry_false():
    q = TestQueue()

    q.add(sleepy_task_with_retry_false(2))

    processed = q.process_all()
    assert len(processed) == 1
    assert processed[0].retry_count == 0
