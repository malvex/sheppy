# Handling Errors in Tasks

When a task raises an exception, worker catches the exception, marks the task `failed`, and stores it on the task as a `TaskException`:

```python
@task
def divide(x: int, y: int) -> float:
    return x / y

# after a worker processes divide(1, 0):
# task.status == 'failed'
# task.exception.type == 'ZeroDivisionError'
# task.exception.message == 'division by zero'
# str(task.exception) == "ZeroDivisionError: division by zero"
```

`TaskException` also carries the exception's `module`, sanitized `args`, and the full formatted `traceback`, so a failure can be inspected from another process. Calling `task.exception.to_exception()` rebuilds the original exception on a best-effort basis. When the class cannot be imported or constructed, you get a `TaskFailedError` instead as a fallback.

## Retries

Set `retry` to re-run a failed task automatically, and `retry_delay` to wait between attempts:

```python
@task(retry=3, retry_delay=[1, 10, 60])
async def charge_card(order_id: int) -> str:
    ...
```

- A single number (`retry_delay=5.0`) waits the same time before every attempt.
- A list sets the delay per attempt: 1s before the first retry, 10s before the second, 60s before the third. When attempts outnumber list entries, the last value is reused.
- Between attempts the task has status `retrying`; it becomes `failed` only when no attempts remain. `task.retry_count` tracks how many retries have happened.

Retry a task manually with `queue.retry()`:

```python
await queue.retry(task)                           # re-queue now
await queue.retry(task, at=timedelta(hours=1))    # re-queue later
await queue.retry(task, force=True)               # re-run even a completed task
```

Or from the CLI: `sheppy task retry <task-id>`.

## Timeouts

`timeout` caps execution time in seconds. A task that exceeds it fails with a `TaskTimeoutError`:

```python
@task(timeout=30)
async def generate_report(report_id: int) -> str:
    ...
```

Timed-out tasks are not retried by default: a task that hangs once usually hangs again. Set `retry_on_timeout=True` to opt in.

## Worker crashes

If a worker process dies mid-task, another worker eventually reclaims the task and marks it `crashed`, with a synthesized `WorkerCrashedError` exception (no traceback, since nothing was raised). For consistency reasons, crashed tasks are not retried by default. Use `retry_on_crash=True` if you want to automatically rerun the task on worker crash:

```python
@task(retry=3, retry_on_crash=True)
async def import_data(file_path: str) -> str:
    ...
```

## How long failures are kept

Finished tasks, including failed ones, keep their metadata in the backend until it expires. Failed and crashed tasks use `error_ttl` if set, otherwise the normal `ttl`; both exist per task (`@task(error_ttl=300)`) and per backend. Set either to `None` to keep records forever. See the [`RedisBackend` reference](../reference/backends/redis-backend.md) for defaults.

## Example: a task that fails once

`examples/testing/test_retry_logic.py` defines a task that fails on its first attempt and succeeds on retry, and asserts on both attempts:

```python title="examples/testing/test_retry_logic.py"
--8<-- "examples/testing/test_retry_logic.py"
```

Note the first processed attempt has status `retrying`, not `failed`. The task is `failed` only after the last attempt.
