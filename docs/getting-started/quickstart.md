# Quickstart

This guide will walk you through installing Sheppy and creating your first task queue.

## Installation

```bash
pip install sheppy
# or if you're using uv:
uv add sheppy
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
--8<-- "examples/quickstart.py:7:9"
```

The `@task` decorator converts your function into a task that can be queued and processed asynchronously.

!!! tip
    Tasks can also be synchronous functions. See the [Sync vs Async Tasks](#sync-vs-async-tasks) section below.

### Step 2: Create a Queue

You need a backend to store tasks.

Let's define a Redis backend and create a queue. (FIXME)

```python title="quickstart.py"
--8<-- "examples/quickstart.py:2:6"
```

!!! note
    * Sheppy currently only supports Redis and in-memory backends, but more are coming soon (see [Roadmap](../about/roadmap.md)).
    * Alternatively, you can implement your own backend by extending the `Backend` class. More details in the [Custom Backend](advanced/custom-backend.md) guide (advanced topic).

!!! tip
    Start Redis using docker: `docker run -d --name redis -p 6379:6379 redis:latest`

### Step 3: Instantiating and Adding Tasks to the Queue

```python
--8<-- "examples/quickstart.py:16:22"
```

### Step 4: Wait for Task Completion

Now we need to wait for the task to get processed to get the result.

```python
--8<-- "examples/quickstart.py:16:16"
    # ... previous code ...

--8<-- "examples/quickstart.py:25:33"
```

### Step 5: Run the Script

First add this to the bottom of `quickstart.py` to run the `main` function:

```python
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

Then run the script in your terminal:

```bash
python quickstart.py
```

The script will hang because there is no worker to process the task yet.

### Step 6: Start a Worker

In separate terminal, start a worker to process tasks from the queue:

```bash
sheppy work --redis-url redis://localhost:6379
```

You should immediately see the worker pick up and process the task.

```plaintext title="Second Terminal Output"
Worker started, listening for tasks...
Processing task (some UUID)...
Task (some UUID) completed.
```

Meanwhile in the first terminal, you should see the result printed after the task is processed:

```plaintext title="First Terminal Output"
Task (some UUID) added to the queue.
Task (some UUID) completed with result: 3
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

## Task State and Results

Tasks have several states throughout their lifecycle:

```python
# create a task (not yet queued)
task = my_task(arg1, arg2)

# add to queue
await queue.add(task)

# blocking wait for task completion
task = await queue.wait_for(task)
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

task = await queue.wait_for(task)

if task.error:
    print(f"Task failed: {task.error}")  # prints "Task failed: Value must be positive"
```

## Next Steps

Now that you understand the basics:

- Learn about [Core Concepts](core-concepts.md) like task lifecycle and queue mechanics
- Explore [FastAPI Integration](fastapi-integration.md) for using Sheppy with FastAPI
- Read the [Testing Guide](testing.md) to learn about testing strategies
- Check out [Examples](../examples/index.md) for real-world usage patterns

## Quick Tips

1. **Tasks are just functions** - Keep them simple and focused
2. **Return Pydantic models** - They're automatically serialized/deserialized
3. **If you are using FastAPI** - Use `Depends()` with Sheppy for Dependency Injection the same way you use it with FastAPI
