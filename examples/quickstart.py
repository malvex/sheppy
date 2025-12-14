import asyncio
from sheppy import Queue, RedisBackend, task


@task
async def add(x: int, y: int) -> int:
    return x + y


backend = RedisBackend("redis://127.0.0.1:6379")
queue = Queue(backend)


async def main():
    # create task instances (returns Task object)
    t = add(1, 2)

    # add task to the queue
    await queue.add(t)
    print(f"Task {t.id} added to the queue.")

    # wait for task to be processed and get the result
    processed = await queue.wait_for(t)

    if processed.status == 'completed':
        print(f"Task {t.id} completed with result: {processed.result}")
    elif processed.error:
        print(f"Task {t.id} failed with error: {processed.error}")
    else:
        # this shouldn't happen because we are waiting for the task to complete
        print(f"Task {t.id} is still pending.")


if __name__ == "__main__":
    asyncio.run(main())
