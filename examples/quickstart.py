from sheppy import MemoryBackend, Queue, task
import asyncio

backend = MemoryBackend()
queue = Queue(backend)

@task
async def add(x: int, y: int) -> int:
    return x + y

@task
def divide(x: int, y: int) -> float:  # Task can be sync or async
    return x / y


async def main():
    # create task instances (returns Task object)
    t = add(1, 2)

    # add task to the queue
    await queue.add(t)
    print(f"Task {t.id} added to the queue.")

    # wait for task to be processed and get the result
    processed = await queue.wait_for(t)

    if processed.completed:
        print(f"Task {t.id} completed with result: {processed.result}")
    elif processed.error:
        print(f"Task {t.id} failed with error: {processed.error}")
    else:
        # this should not happen because we are waiting for the task to complete
        print(f"Task {t.id} is still pending.")


if __name__ == "__main__":
    asyncio.run(main())
