# Integration with FastAPI

Sheppy integrates seamlessly with FastAPI, allowing you to create robust web applications that leverage background task processing. This guide walks you through setting up a FastAPI application with Sheppy, defining tasks and workflows, and handling task results.

## Setting Up FastAPI with Sheppy

Let's start by creating a simple `tasks.py` file with a task named `send_email_task` that simulates sending an email:

```python title="app/tasks.py"
--8<-- "examples/fastapi-integration/simple/app/tasks.py"
```

Next, create a `main.py` file to set up the FastAPI application and define `/send-email` endpoint that accepts queue as a dependency using `Depends`, and adds the `send_email_task` to the queue:

```python title="app/main.py"
--8<-- "examples/fastapi-integration/simple/app/main.py"
```

You can run this app with `fastapi dev` and go to http://localhost:8000/docs and running worker process by executing `sheppy work` in a separate terminal.

## Writing Tests

There are two ways to test `send_email_task` in `tasks.py`:

### 1. Testing the Task Directly (Unit Test)

You can test the task directly using TestQueue:

```python title="tests/test_tasks.py"
--8<-- "examples/fastapi-integration/simple/tests/test_tasks.py"
```

### 2. Testing the FastAPI Endpoint (Integration Test)

You can also test the FastAPI endpoint using FastAPI's `TestClient`, however, since the task is executed asynchronously in the background, the setup is a bit more involved as you need to run actual worker process inside synchronous test (note: this is temporary and there will be better solutions in near future):

```python title="tests/test_app.py"
--8<-- "examples/fastapi-integration/simple/tests/test_app.py"
```

### 3. Async Test Example

Alternatively, if you are comfortable with async tests, you can use `httpx.AsyncClient` to test the FastAPI endpoint asynchronously. This allows you to start the worker process in the background using `asyncio.create_task`, which is a cleaner approach:

```python title="tests/test_app_async.py"
--8<-- "examples/fastapi-integration/simple/tests/test_app_async.py"
```
