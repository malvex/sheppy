import asyncio

from sheppy import Queue, RedisBackend, task


@task
async def calculate(x: int, y: int) -> int:
    return x + y


backend = RedisBackend("redis://127.0.0.1:6379")
queue = Queue("default", backend)


async def main() -> None:
    # create Task
    task = calculate(1, 2)

    # add Task to queue for processing
    await queue.add(task)

    # wait for task to finish
    task = await queue.wait_for_result(task, timeout=300)

    print(f"task result: {task.result}")
    print(f"completed: {task.completed}")
    print(f"error: {task.error}")


if __name__ == "__main__":
    asyncio.run(main())
