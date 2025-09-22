# Sheppy 🐕

Sheppy is a fast, modern, and easy to use Python task queue that's simple enough to learn in minutes, yet scales to millions of tasks. It is designed to be as simple as possible, with sane defaults, while providing all essential features for working with background tasks.

## Why Sheppy?

- **Blazing Fast**: The fastest Python task queue without compromises
- **Async Native**: Built on asyncio from the ground up - no async backporting, just modern Python
- **Dead Simple**: Just `@task` decorator and `Queue` - that's it
- **Type Safe**: Full Pydantic integration for automatic validation and serialization
- **Multi-backend Support**: Native support for different backends (note: only Redis is implemented at this time)
- **FastAPI Compatible**: Seamless integration with FastAPI's dependency injection

## Installation

```bash
pip install sheppy
# or if you're using uv:
uv add sheppy
```

## TL;DR Quick Start

This is all you need to know:

```python
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
        # note: this won't happen though because wait_for doesn't return pending tasks
        print("Task is still pending!")

if __name__ == "__main__":
    asyncio.run(main())
```

Run it:

```bash
# run the app:
python examples/tldr.py  # nothing will happen because worker isn't running

# in another terminal, you can list queued tasks:
sheppy task list  # (shows 2 pending and 2 scheduled tasks)

# run worker process to process the tasks
sheppy work  # (you should see the tasks to get processed, and the app should finish!)
```

And that's it!

## Tl;Dr multiple queues (including priority queues)

Task can have different priority by simply defining multiple queues with different priority.

```python
backend = RedisBackend("redis://127.0.0.1:6379")
different_backend = RedisBackend("redis://10.10.10.10:6379")

email_queue = Queue(backend, name="email-queue")
high_priority_email_queue = Queue(backend, name="high-priority-email-queue")
data_exports_queue = Queue(different_backend, name="data-exports")

normal_email = send_email(Email(...))
important_email = send_email(Email(...))
data_export = export_data(user_id=1234)

await email_queue.add(normal_email)
await high_priority_email_queue.add(important_email)
await data_exports_queue.add(data_export)
```

And then you run workers like this:

```bash
# the first queue in the arg list will be always processed first
# the second queue in the arg list will be processed only if first queue is empty
sheppy work -q "high-priority-email-queue" -q "email-queue" --redis-url "redis://127.0.0.1:6379"

# in separate terminal, we run worker for different queue with a different backend
sheppy work -q "data-exports" --redis-url "redis://10.10.10.10:6379"
```

## Type Safety with Pydantic

```python
from pydantic import BaseModel
from sheppy import task

class UserData(BaseModel):
    name: str
    email: str
    age: int

class ProcessResult(BaseModel):
    user_id: int
    status: str

@task
async def process_user(data: UserData) -> ProcessResult:
    # automatic validation of inputs and outputs!
    user_id = 42
    return ProcessResult(user_id=user_id, status="active")

# this will validate automatically
data = UserData(name="Alice", email="alice@example.com", age=30)
task = process_user(data)

# you can also provide dict and it will be automatically validated
user_data = {"name": "Bob", "email": "bob@example.com", "age": 30}
task = process_user(user_data)

# input is validated immediately, before the task can even be queued
user_data = {"invalid": "input"}
task = process_user(user_data)  # throws a ValidationError exception!
```

## Easy Testing Without the Async Hassle

```python
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
    assert task.completed
```

No `async def test_`, no `@pytest.mark.asyncio`, no event loop management. Just simple, synchronous tests.

## Requirements

- Python 3.10+
- Redis 8+

## Developing

```bash
git clone https://github.com/malvex/sheppy.git
cd sheppy
uv sync --group dev

pytest -v tests/ --tb=short
mypy src/
ruff check src/
```

## License

This project is licensed under the terms of the MIT license.
