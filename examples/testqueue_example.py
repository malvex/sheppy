import asyncio
from sheppy import TestQueue, task

@task
async def multiply(x: int) -> int:
    # this is an async task (but sync tasks are also supported!)
    await asyncio.sleep(1)
    return x * 2

def test_multiply_without_async():
    queue = TestQueue()

    # add async task to queue
    task = multiply(5)
    queue.add(task)  # no await needed!

    # process it synchronously
    task = queue.process_next()

    # check results
    assert task.result == 10
    assert task.status == 'completed'
