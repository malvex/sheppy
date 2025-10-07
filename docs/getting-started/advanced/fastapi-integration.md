# Integration with FastAPI

Sheppy is designed to feel native to FastAPI users. If you know how to use `Depends()` for database connections, you already know how to use Sheppy.

This guide demonstrates building a FastAPI application with background task processing, from basic setup to production testing patterns.

## Basic Setup

We will build an email service that processes messages in the background. Start by defining your task with Pydantic models for type safety:

```python title="app/tasks.py"
--8<-- "examples/fastapi-integration/simple/app/tasks.py"
```

Notice that `send_email_task` accepts and returns Pydantic models. Sheppy handles validation automatically.

## Creating the FastAPI Application

The queue is injected exactly like you would inject a database session:

```python title="app/main.py"
--8<-- "examples/fastapi-integration/simple/app/main.py"
```

**Key points:**

- `get_queue()` is a standard FastAPI dependency
- `queue.add(t)` enqueues the task for background processing
- `queue.wait_for(t, timeout=5)` blocks until the task completes (useful for synchronous APIs)
- The worker process runs separately and picks up tasks from the queue

## Running the Application

Start the FastAPI server:

```bash
fastapi dev app/main.py
```

In a separate terminal, start the worker:

```bash
sheppy work
```

Visit http://localhost:8000/docs to test the API interactively.

## Testing Strategies

Sheppy provides flexible testing approaches depending on what you want to verify. For more details on testing with Sheppy, see the [Testing guide](../testing.md).

### Unit Testing: Test Tasks Directly

The simplest approach is testing the task logic in isolation using `TestQueue`:

```python title="tests/test_tasks.py"
--8<-- "examples/fastapi-integration/simple/tests/test_tasks.py"
```

`TestQueue` provides a synchronous API with explicit control over task processing. Perfect for fast unit tests.

### Integration Testing: Test the Full Stack

For end-to-end testing, you need to test the FastAPI endpoint with an actual worker processing tasks.

#### Synchronous Tests

Using FastAPI's `TestClient` requires running a worker in a background thread:

```python title="tests/test_app.py"
--8<-- "examples/fastapi-integration/simple/tests/test_app.py"
```

!!! note
    Work in Progress - future versions will provide simpler testing utilities for FastAPI.

#### Async Tests (Recommended)

For async test suites, use `httpx.AsyncClient` with `asyncio.create_task` for cleaner worker management:

```python title="tests/test_app_async.py"
--8<-- "examples/fastapi-integration/simple/tests/test_app_async.py"
```
