import asyncio
from sheppy import RedisBackend, Queue
from tasks import send_email


backend = RedisBackend("redis://localhost:6379")
queue = Queue("email-queue", backend)


async def main():
    # Create Task
    task = send_email("user@example.com")

    # Add Task to queue for processing
    await queue.add(task)

    # wait for task to finish
    task = await queue.wait_for_result(task)

    assert task.result == "sent"
    assert task.completed
    assert not task.error


if __name__ == "__main__":
    asyncio.run(main())
