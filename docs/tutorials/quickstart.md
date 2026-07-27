# Quickstart

Install Sheppy, define your first task, and watch a worker process it.

## Installation

```bash
pip install sheppy
# or if you're using uv:
uv add sheppy
source .venv/bin/activate
```

## Your First Task

You will build a task that adds two numbers: enough to see the full flow from enqueue to result.

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

The decorator changes what calling the function means: `calculate(2, 1)` no longer executes the function. It returns a `Task` instance that can be queued.

!!! tip
    Tasks can be sync or async. Sheppy handles both. See [Sync vs Async Tasks](#sync-vs-async-tasks) below.

### Step 2: Create a Queue

Queues need a backend for task storage. Use Redis:

```python title="quickstart.py"
--8<-- "examples/quickstart.py:10:10"
```

!!! tip
    Start Redis with Docker: `docker run -d --name redis -p 6379:6379 redis:7`

!!! note
    Sheppy ships with Redis and in-memory backends. More backends are on the [Roadmap](../about/roadmap.md), or implement your own by extending the `Backend` class.

### Step 3: Add Tasks to the Queue

Sheppy is async-first, so wrap your queue operations in an async function:

```python
--8<-- "examples/quickstart.py:13:19"
```

Calling `calculate(1, 2)` creates a `Task` instance. Adding it to the queue makes it available for workers to process.

### Step 4: Wait for Task Completion

Use `wait_for()` to block until a worker processes the task:

```python
--8<-- "examples/quickstart.py:13:13"
    # ... previous code ...

--8<-- "examples/quickstart.py:21:30"
```

The `timeout` parameter controls how long to wait. In a web application you would usually return the task id to the client immediately and expose status through a separate endpoint instead of blocking.

### Step 5: Run the Script

Add the entry point to run your async main function:

```python
--8<-- "examples/quickstart.py:33:34"
```

Run it:

```bash
python quickstart.py
```

The script hangs waiting for a worker. Start one next.

### Step 6: Start a Worker

In a separate terminal, start a worker:

```bash
sheppy work --backend-url redis://localhost:6379
```

The worker picks up and processes the task:

```plaintext title="Second terminal output"
bash:~$ sheppy work --backend-url redis://localhost:6379
Starting worker for queue 'default'
  Backend: RedisBackend [redis://localhost:6379]
  Job processing: True  Scheduler: True  Cron Manager: True
  Max concurrent tasks: 10

[15:01:45]  INFO     <Scheduler> started
            INFO     <CronManager> started
[15:01:48]  INFO     <Worker> Processing task 074396c1-e11f-40a3-b22b-094dc89573ea
                     (quickstart:calculate)
            INFO     <Worker> Task 074396c1-e11f-40a3-b22b-094dc89573ea completed
                     successfully
```

Back in the first terminal, you see the result:

```plaintext title="First terminal output"
bash:~$ python quickstart.py
Task 074396c1-e11f-40a3-b22b-094dc89573ea added to the queue.
Task 074396c1-e11f-40a3-b22b-094dc89573ea completed with result: 3
bash:~$
```

(The task id and timestamps differ on every run.)

## Complete Example

Here is the full `quickstart.py` for reference:

```python title="quickstart.py"
--8<-- "examples/quickstart.py"
```

## Sync vs Async Tasks

Sheppy handles both async and sync tasks:

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

Sync tasks run in a thread pool so they do not block the worker's event loop.
