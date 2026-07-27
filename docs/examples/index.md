# Code Examples

All examples live in the [`examples/`](https://github.com/malvex/sheppy/tree/master/examples) directory of the repository. Each entry below says what the example demonstrates, how to run it, and what to expect.

Examples that need Redis expect a server at `redis://localhost:6379` (`docker run -d -p 6379:6379 redis:7`) and a worker in a second terminal (`sheppy work`). The self-contained examples use the in-memory backend and need neither.

## Basics

- **quickstart.py**: the task from the [Quickstart](../tutorials/quickstart.md). Needs Redis and a worker. Prints `Task <id> completed with result: 3`.
- **simple.py**: self-contained: memory backend plus an in-process worker. `python examples/simple.py` prints the "Sent email" line from the task.
- **simple_scheduled.py**: scheduling with `at=timedelta(...)`; self-contained. See [Task scheduling](../guides/task-scheduling.md).
- **tldr.py**: `add`, `schedule`, and `wait_for` in one script. Needs Redis and a worker. Prints `Task succeed with result: Hello, World!`.

## Features

- **pydantic_example.py**: argument validation with Pydantic models. Exits with a `ValidationError` on purpose; see [Validation with Pydantic](../guides/validation-with-pydantic.md).
- **rate_limits.py**: a sliding-window rate limit of 2 tasks per 5 seconds. Needs Redis and a worker. Three tasks are added; two run immediately (result `0`) and one runs five seconds later (result `5`), printed in completion order.
- **workflows/simple.py**: a fan-out workflow that greets four people and joins the results. Self-contained (memory backend plus in-process worker); prints the joined greetings.
- **workflows/fan_in_fan_out.py**: a workflow with a failing step, a rollback, and notification fan-out. Self-contained; the failure path ends with `Workflow failed as expected: ...`.

## Testing

Run these with pytest from the repository root, e.g. `pytest examples/testqueue_example.py`.

- **testqueue_example.py**: `TestQueue` basics.
- **example_dependency.py**: swapping a dependency with `dependency_overrides`.
- **example_timeout.py**: `timeout` and `retry_on_timeout` behavior.
- **testing/**: failure and retry assertions plus a small project layout (`basic-example/`). See [Testing tasks](../guides/testing.md).

## FastAPI

These need FastAPI installed (`pip install fastapi`).

- **simple_with_fastapi.py**: a single-file app with a SQLModel database (install `sqlmodel` separately). Run with `fastapi run examples/simple_with_fastapi.py`, then open http://localhost:8000/docs.
- **fastapi-integration/simple/**: the email-service app from the [FastAPI guide](../guides/fastapi-integration.md), with unit and integration tests.
- **fastapi-integration/example_apirouter.py**: the monitoring router from `sheppy.fastapi.create_router` mounted into an app. Run with `fastapi dev examples/fastapi-integration/example_apirouter.py`.

## Project scaffold

- **quick-start/**: a standalone project with its own `pyproject.toml` and `docker-compose.yml`, showing a minimal deployable setup.
