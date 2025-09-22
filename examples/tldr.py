import asyncio
from datetime import datetime, timedelta
from sheppy import Queue, task, RedisBackend

queue = Queue(RedisBackend("redis://127.0.0.1:6379"))

@task
async def say_hello(to: str) -> str:
    s = f"Hello, {to}!"
    print(s)
    return s

async def main():
    t1 = say_hello("World")
    await queue.add(t1)
    await queue.add(say_hello("Moon"))
    await queue.schedule(say_hello("Patient Person"), at=timedelta(seconds=10))  # runs in 10 seconds from now
    await queue.schedule(say_hello("New Year"), at=datetime.fromisoformat("2026-01-01 00:00:00 +00:00"))

    # await the task completion
    updated_task = await queue.wait_for(t1)

    if updated_task.error:
        print(f"Task failed with error: {updated_task.error}")
    elif updated_task.completed:
        print(f"Task succeed with result: {updated_task.result}")
        assert updated_task.result == "Hello, World!"
    else:
        # Note: this won't happen though because wait_for doesn't return pending tasks
        print("Task is still pending!")

if __name__ == "__main__":
    asyncio.run(main())
