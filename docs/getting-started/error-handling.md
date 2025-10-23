# Handling Errors in Tasks

Robust error handling is critical for building reliable task queues. Sheppy provides built-in mechanisms for retry logic, error tracking, and graceful failure handling to ensure your tasks can recover from transient failures.

## Understanding Task Errors

When a task raises an exception during execution:

1. The exception is caught by the worker
2. The task's `error` field is set to the exception message
3. The task's `completed` field remains `False`
4. The worker logs the error with full traceback
5. If retry is configured, the task is automatically rescheduled

## Basic Error Handling

### Checking for Errors

After a task completes, check its `error` and `completed` fields:

```python
import asyncio
from sheppy import Queue, RedisBackend, task

@task
async def divide(a: int, b: int) -> float:
    return a / b

async def main():
    queue = Queue(RedisBackend("redis://127.0.0.1:6379"))
    
    # This will fail with division by zero
    task = divide(10, 0)
    await queue.add(task)
    
    # Wait for task to complete (or fail)
    task = await queue.wait_for(task)
    
    if task.error:
        print(f"Task failed with error: {task.error}")
        # Output: Task failed with error: division by zero
    elif task.completed:
        print(f"Task succeeded with result: {task.result}")
    else:
        print("Task is still pending")

if __name__ == "__main__":
    asyncio.run(main())
```

### Error Information

The `Task` object provides several fields for error handling:

- `error` (str|None) - The error message if the task failed
- `completed` (bool) - `True` only if the task finished successfully
- `finished_at` (datetime|None) - When the task completed (success or final failure)
- `retry_count` (int) - Number of times the task has been retried
- `last_retry_at` (datetime|None) - When the task was last retried
- `next_retry_at` (datetime|None) - When the task is scheduled for retry

## Automatic Retry

### Configuring Retry

Configure retry behavior using the `@task` decorator:

```python
from sheppy import task

# Retry up to 3 times with 1 second delay between attempts
@task(retry=3, retry_delay=1.0)
async def unstable_api_call(url: str) -> dict:
    response = await fetch_data(url)  # might fail intermittently
    return response

# Retry with exponential backoff: 1s, 5s, 10s
@task(retry=3, retry_delay=[1.0, 5.0, 10.0])
async def fetch_with_backoff(url: str) -> dict:
    response = await fetch_data(url)
    return response

# No retry (default)
@task
async def critical_operation() -> str:
    # This won't retry on failure
    return "result"
```

### How Retry Works

When a task fails and has retry configured:

1. The worker catches the exception and increments `retry_count`
2. If `retry_count < retry` (max retries), the task is rescheduled
3. The task is scheduled to run again after `retry_delay` seconds
4. The worker logs a warning about the retry
5. The process repeats until success or max retries is reached

### Retry Delay Strategies

Sheppy supports two retry delay strategies:

#### Constant Delay

Use a single float value to retry with the same delay:

```python
@task(retry=5, retry_delay=2.0)
async def task_with_constant_delay() -> str:
    # Retries after 2 seconds each time
    return "result"
```

#### Exponential Backoff

Use a list of floats for different delays per retry attempt:

```python
@task(retry=4, retry_delay=[1.0, 2.0, 5.0, 10.0])
async def task_with_backoff() -> str:
    # 1st retry: after 1 second
    # 2nd retry: after 2 seconds
    # 3rd retry: after 5 seconds
    # 4th retry: after 10 seconds
    return "result"
```

!!! tip
    If the retry count exceeds the length of the delay list, the last delay value is used for remaining retries.

## Complete Example

Here's a complete example demonstrating error handling with retry:

```python
--8<-- "examples/testing/test_retry_logic.py"
```

This example shows:

1. A task that fails on the first attempt
2. Automatic retry after failure
3. Success on the second attempt
4. Verification that both attempts have the same task ID

## Manual Retry

You can manually retry failed tasks using `queue.retry()`:

```python
async def main():
    queue = Queue(RedisBackend("redis://127.0.0.1:6379"))
    
    task = divide(10, 0)
    await queue.add(task)
    
    # Wait for task to fail
    task = await queue.wait_for(task)
    
    if task.error:
        print(f"Task failed: {task.error}")
        
        # Retry immediately
        await queue.retry(task)
        
        # Or retry after a delay
        from datetime import timedelta
        await queue.retry(task, at=timedelta(seconds=30))
        
        # Or retry at a specific time
        from datetime import datetime, timezone
        await queue.retry(task, at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
```

!!! warning
    By default, `retry()` won't retry successfully completed tasks. Use `force=True` to override:
    
    ```python
    await queue.retry(task, force=True)
    ```

## Worker Error Logging

The worker provides detailed logging for different error scenarios:

### Non-Retriable Task Failure

```plaintext
ERROR <Worker> Task 074396c1-e11f-40a3-b22b-094dc89573ea failed: division by zero
Traceback (most recent call last):
  ...
ZeroDivisionError: division by zero
```

### Retriable Task - Retry Scheduled

```plaintext
WARNING <Worker> Task 074396c1-e11f-40a3-b22b-094dc89573ea failed (attempt 1/3), scheduling retry at 2025-10-23 10:15:30+00:00
```

### Retriable Task - Final Failure

```plaintext
ERROR <Worker> Task 074396c1-e11f-40a3-b22b-094dc89573ea failed after 3 retries: connection timeout
Traceback (most recent call last):
  ...
TimeoutError: connection timeout
```

## Error Handling Patterns

### Pattern 1: Graceful Degradation

Handle expected errors within your task:

```python
@task
async def fetch_user_data(user_id: int) -> dict:
    try:
        return await api.get_user(user_id)
    except UserNotFoundError:
        # Handle expected error gracefully
        return {"id": user_id, "status": "not_found"}
    except APIError as e:
        # Let unexpected errors propagate for retry
        raise
```

### Pattern 2: Conditional Retry

Use retry for transient errors only:

```python
@task(retry=3, retry_delay=2.0)
async def send_email(to: str, subject: str, body: str) -> str:
    try:
        await email_service.send(to, subject, body)
        return "sent"
    except ValidationError as e:
        # Don't retry validation errors
        return f"invalid: {e}"
    except ConnectionError:
        # Retry connection errors
        raise
```

### Pattern 3: Error Monitoring

Track errors for alerting and monitoring:

```python
@task(retry=2)
async def critical_task(data: dict) -> str:
    try:
        result = await process_data(data)
        return result
    except Exception as e:
        # Log to monitoring system
        await monitoring.track_error("critical_task", str(e))
        raise  # Re-raise for retry mechanism
```

## Checking Task Properties

Use the task's properties to determine retry status:

```python
task = await queue.get_task(task_id)

# Check if task is configured for retry
if task.is_retriable:
    print(f"Task can retry up to {task.config.retry} times")

# Check if task should retry
if task.should_retry:
    print(f"Task will retry at {task.next_retry_at}")
else:
    print("Task won't retry anymore")

# Check retry count
print(f"Task has been retried {task.retry_count} times")
```

## Best Practices

1. **Configure appropriate retry counts** - Too few retries might miss transient errors, too many might delay failure detection
2. **Use exponential backoff** - Give external services time to recover
3. **Log errors properly** - Include context to help with debugging
4. **Make tasks idempotent** - Tasks should be safe to retry multiple times
5. **Handle permanent failures gracefully** - Not all errors should trigger retries
6. **Monitor failed tasks** - Set up alerts for tasks that fail after all retries
7. **Set reasonable timeouts** - Prevent tasks from hanging indefinitely

## Common Pitfalls

### ❌ Don't catch all exceptions silently

```python
@task
async def bad_example():
    try:
        await do_something()
        return "success"
    except Exception:
        return "failed"  # Error is hidden!
```

### ✅ Do let errors propagate for retry

```python
@task(retry=3)
async def good_example():
    try:
        await do_something()
        return "success"
    except ValidationError as e:
        # Handle expected errors
        return f"validation_error: {e}"
    # Let unexpected errors propagate for retry
```

### ❌ Don't use naive datetime for retry

```python
@task
async def bad_retry_example():
    # This will cause errors
    await queue.retry(task, at=datetime(2026, 1, 1, 12, 0, 0))
```

### ✅ Do use timezone-aware datetime

```python
@task
async def good_retry_example():
    from datetime import timezone
    await queue.retry(task, at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
```

## See Also

- [Task Scheduling](task-scheduling.md) - Learn how to schedule tasks
- [Testing](testing.md) - Test error handling with TestQueue
- [Task Config Reference](../reference/task-config.md) - Complete Config class documentation
- [Worker Reference](../reference/worker.md) - Worker class documentation
