# Getting Started with Sheppy

This guide will walk you through installing Sheppy and creating your first task queue.

## Installation

```bash
pip install sheppy
# or if you're using uv:
uv add sheppy
```

## Your First Task

Let's create a simple task that sends a welcome email to new users.

### Step 1: Define a Task

Create a file called `tasks.py`:

```python
from sheppy import task

@task
async def add(x: int, y: int) -> int:
    """Add two numbers together."""
    return x + y
```

That's it! The `@task` decorator converts your function into a task that can be queued and processed asynchronously.

### Step 2: Create a Queue

Queues store tasks until they are processed. Let's create one:

```python
from sheppy import Queue, RedisBackend

backend = RedisBackend("redis://127.0.0.1:6379")
queue = Queue(backend=backend)
```

Hint: start Redis using docker: `docker run -d --name redis -p 6379:6379 redis:latest`

### Step 3: Add Tasks to the Queue

Now let's add some tasks:

```python
import asyncio
from tasks import add

async def main():
    # create task instances (returns Task object)
    task1 = add(1, 2)
    task2 = add(5, 5)

    # add them to the queue
    await queue.add(task1)
    await queue.add(task2)

    print(f"Added 2 tasks to the queue")
    print(f"Task 1 ID: {task1.id}")
    print(f"Task 2 ID: {task2.id}")

asyncio.run(main())
```

### Step 4: Process Tasks with a Worker

Use sheppy CLI to run worker:

```bash
sheppy work
```

## Complete Example

Here's a real-world example with Redis:

```python
# (todo)
```

## Sync vs Async Tasks

Sheppy supports both async and sync functions:

```python
@task
async def async_task(x: int) -> int:
    await asyncio.sleep(1)
    return x * 2

@task
def sync_task(x: int) -> int:
    time.sleep(1)  # blocking operation
    return x * 2
```

Sync tasks automatically run in a thread pool to avoid blocking the event loop.

## Task State and Results

Tasks have several states throughout their lifecycle:

```python
# create a task (not yet queued)
task = my_task(arg1, arg2)

# add to queue
await queue.add(task)

# blocking wait for task completion
task = await queue.wait_for_result(task)
assert task.completed

# or retrieve the task anytime even if it's still pending
# task = await queue.get_task(task)

if task.completed:
    print(f"Success! Result: {task.result}")
elif task.error:
    print(f"Failed with error: {task.error}")
else:
    print("Still processing...")
```

## Error Handling

Tasks that raise exceptions are marked as failed. Exception message is stored in `error` attribute:

```python
@task
async def risky_task(value: int) -> int:
    if value < 0:
        raise ValueError("Value must be positive")
    return value * 2

task = risky_task(-5)
await queue.add(task)

task = await queue.wait_for_result(task)

if task.error:
    print(f"Task failed: {task.error}")  # prints "Task failed: Value must be positive"
```

## Quick Tips

1. **Tasks are just functions** - Keep them simple and focused
2. **Return Pydantic models** - They're automatically serialized/deserialized
3. **If you are using FastAPI** - Use `Depends()` with Sheppy for Dependency Injection the same way you use it with FastAPI
