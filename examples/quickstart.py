import asyncio
from sheppy import Queue, task


@task
async def calculate(x: int, y: int) -> int:
    return x + y


queue = Queue("redis://localhost:6379")


async def main():
    # create task instances (returns Task object)
    t = calculate(1, 2)

    # add task to the queue
    await queue.add(t)
    print(f"Task {t.id} added to the queue.")

    # wait for task to be processed and get the result
    processed = await queue.wait_for(t)

    if processed.status == 'completed':
        print(f"Task {t.id} completed with result: {processed.result}")
    elif processed.exception:
        print(f"Task {t.id} failed with error: {processed.exception}")
    else:
        # this shouldn't happen because we are waiting for the task to complete
        print(f"Task {t.id} is still pending.")


if __name__ == "__main__":
    asyncio.run(main())
