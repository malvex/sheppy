# Quickstart

This guide will walk you through installing Sheppy and creating your first task queue.

## Installation

```bash
pip install sheppy
# or if you're using uv:
uv add sheppy
source .venv/bin/activate
```

## Your First Task

Let's create a simple task that adds two numbers together.

### Step 0: Import Required Modules

First, create a file called `quickstart.py` and add the following code:

```python title="quickstart.py"
--8<-- "examples/quickstart.py:1:2"
```

This will import the necessary components from Sheppy and other standard libraries we will use.

### Step 1: Define a Task

Next, define a task using the `@task` decorator:

```python title="quickstart.py"
--8<-- "examples/quickstart.py:5:7"
```

The `@task` decorator converts your function into a task that can be queued and processed asynchronously.

!!! tip
    Tasks can also be synchronous functions. See the [Sync vs Async Tasks](#sync-vs-async-tasks) section below.

### Step 2: Create a Queue

To interact with tasks, we need a queue. Queues require a backend to store tasks, so let's define a Redis backend and create a queue.

```python title="quickstart.py"
--8<-- "examples/quickstart.py:10:11"
```

!!! note
    * Sheppy currently only supports Redis and in-memory backends, but more are coming soon (see [Roadmap](../about/roadmap.md)).
    * Alternatively, you can implement your own backend by extending the `Backend` class. More details in the [Custom Backend](advanced/custom-backend.md) guide (advanced).

!!! tip
    Start Redis using docker: `docker run -d --name redis -p 6379:6379 redis:latest`

### Step 3: Instantiating and Adding Tasks to the Queue

Sheppy uses asynchronous python, so we need to define an async `main` function to interact with the queue.

```python
--8<-- "examples/quickstart.py:14:20"
```

### Step 4: Wait for Task Completion

Now we need to wait for the task to get processed to get the result.

```python
--8<-- "examples/quickstart.py:14:14"
    # ... previous code ...

--8<-- "examples/quickstart.py:22:31"
```

### Step 5: Run the Script

First add this to the bottom of `quickstart.py` to run the `main` function:

```python
--8<-- "examples/quickstart.py:34:35"
```

Then run the script in your terminal:

```bash
python quickstart.py
```

The script will hang because there is no worker to process the task yet.

### Step 6: Start a Worker

In separate terminal, start a worker to process tasks from the queue:

```bash
sheppy work --redis-url redis://127.0.0.1:6379
```

You should immediately see the worker pick up and process the task.

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

Meanwhile in the first terminal, you should see the result printed after the task is processed:

```plaintext title="First Terminal Output"
bash:~$ python quickstart.py
Task 074396c1-e11f-40a3-b22b-094dc89573ea added to the queue.
Task 074396c1-e11f-40a3-b22b-094dc89573ea completed with result: 3
bash:~$
```

### Step 7: Celebrate!

And that's it! You've successfully created and processed your first task with Sheppy! 🎉

## Complete Example

Here's the complete code for `quickstart.py`. You can copy and paste it into a file to try it out yourself:

```python title="quickstart.py"
--8<-- "examples/quickstart.py"
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

## Next Steps

Now that you understand the basics:

- Learn about [Handling Errors in Tasks](error-handling.md) to manage failures and retries
- Explore [FastAPI Integration](advanced/fastapi-integration.md) for using Sheppy with FastAPI
- Read the [Testing Guide](testing.md) to learn about testing strategies
- Check out [Examples](../examples/index.md) for real-world usage patterns
