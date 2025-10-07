# Quickstart

Get up and running with Sheppy in minutes. This guide walks you through creating your first background task, from installation to seeing it process in real-time.

## Installation

```bash
pip install sheppy
# or if you're using uv:
uv add sheppy
source .venv/bin/activate
```

## Your First Task

We'll build a simple task that adds two numbers. Nothing fancy, just enough to understand the core workflow.

### Step 0: Import Required Modules

Create a file called `quickstart.py` with these imports:

```python title="quickstart.py"
--8<-- "examples/quickstart.py:1:2"
```

### Step 1: Define a Task

Add the `@task` decorator to your function:

```python title="quickstart.py"
--8<-- "examples/quickstart.py:5:7"
```

The decorator transforms your function: when you call `add(2, 1)`, instead of executing immediately, it creates a `Task` instance that can be queued for later execution.

!!! tip
    Tasks can be sync or async. Sheppy handles both automatically. See [Sync vs Async Tasks](#sync-vs-async-tasks) below.

### Step 2: Create a Queue

Queues need a backend for task storage. Let's use Redis:

```python title="quickstart.py"
--8<-- "examples/quickstart.py:10:11"
```

!!! tip
    Start Redis with Docker: `docker run -d --name redis -p 6379:6379 redis:latest`

!!! note
    Sheppy supports Redis and in-memory backends out of the box. More backends are coming (see [Roadmap](../about/roadmap.md)), or implement your own by extending the `Backend` class.

### Step 3: Add Tasks to the Queue

Sheppy is async-first, so wrap your queue operations in an async function:

```python
--8<-- "examples/quickstart.py:14:20"
```

Calling `add(2, 1)` creates a `Task` instance. Adding it to the queue makes it available for workers to process.

### Step 4: Wait for Task Completion

Use `wait_for()` to block until the worker processes the task:

```python
--8<-- "examples/quickstart.py:14:14"
    # ... previous code ...

--8<-- "examples/quickstart.py:22:31"
```

The `timeout` parameter controls how long to wait. In production, you would typically return the task ID immediately and check status via a separate endpoint.

### Step 5: Run the Script

Add the entry point to run your async main function:

```python
--8<-- "examples/quickstart.py:34:35"
```

Run it:

```bash
python quickstart.py
```

The script will hang waiting for a worker. Let's fix that.

### Step 6: Start a Worker

In a separate terminal, start a worker:

```bash
sheppy work --redis-url redis://127.0.0.1:6379
```

The worker immediately picks up and processes the task.

```plaintext title="Second Terminal Output"
bash:~$ sheppy work --redis-url redis://127.0.0.1:6379
Starting worker for queue 'default'
  Backend: redis [redis://127.0.0.1:6379]
  Job processing: True  Scheduler: True  Cron Manager: True
  Max concurrent tasks: 10

[03:35:21]  INFO   <Scheduler> started
            INFO   <CronManager> started
            INFO   <Worker> Processing task 074396c1-e11f-40a3-b22b-094dc89573ea
                   (examples.quickstart:add)
            INFO   <Worker> Task 074396c1-e11f-40a3-b22b-094dc89573ea completed
                   successfully
```

Back in the first terminal, you'll see the result:

```plaintext title="First Terminal Output"
bash:~$ python quickstart.py
Task 074396c1-e11f-40a3-b22b-094dc89573ea added to the queue.
Task 074396c1-e11f-40a3-b22b-094dc89573ea completed with result: 3
bash:~$
```

### Step 7: Celebrate!

And that's it! You've successfully created and processed your first task with Sheppy! 🎉

## Complete Example

Here's the full `quickstart.py` for reference:

```python title="quickstart.py"
--8<-- "examples/quickstart.py"
```

## Sync vs Async Tasks

Sheppy handles both async and sync tasks automatically:

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
