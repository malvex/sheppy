# Handling Errors in Tasks

Sheppy provides several mechanisms for handling task failures, from automatic retries to dead letter queue (DLQ) handlers.

## Automatic Retries

Tasks can be configured to automatically retry on failure using the `retry` and `retry_delay` parameters:

```python
from sheppy import task

@task(retry=3, retry_delay=1.0)
async def unreliable_task():
    # This task will be retried up to 3 times with a 1 second delay between attempts
    ...
```

You can also specify exponential backoff by providing a list of delays:

```python
@task(retry=3, retry_delay=[1, 2, 4])
async def exponential_backoff_task():
    # First retry after 1s, second after 2s, third after 4s
    ...
```

## Dead Letter Queue (DLQ)

For tasks that fail permanently (either non-retriable tasks or tasks that have exhausted all retries), Sheppy provides a Dead Letter Queue handler that you can use to process failed tasks.

### What is a DLQ Handler?

A DLQ handler is a callback function that gets invoked automatically when a task fails permanently. This allows you to:

- Log failures to a monitoring system
- Send alerts to your team
- Store failed tasks in a database for manual review
- Trigger alternative recovery workflows
- Archive failed tasks for later analysis

### Using the DLQ Handler

You can specify a DLQ handler when creating a Worker by passing it to the `on_failure` parameter:

```python
import asyncio
from sheppy import Worker, RedisBackend, Task

# Define your DLQ handler
async def handle_failed_task(task: Task):
    """Called when a task fails permanently."""
    print(f"Task {task.id} failed permanently!")
    print(f"Error: {task.error}")
    print(f"Function: {task.spec.func}")
    print(f"Retry count: {task.retry_count}")
    
    # Send to monitoring system
    await send_to_datadog(task)
    
    # Store in database for manual review
    await db.failed_tasks.insert(task.model_dump())
    
    # Send alert
    await send_slack_alert(f"Task {task.id} failed: {task.error}")

# Create worker with DLQ handler
async def main():
    backend = RedisBackend()
    worker = Worker(
        queue_name="default",
        backend=backend,
        on_failure=handle_failed_task
    )
    
    await worker.work()

if __name__ == "__main__":
    asyncio.run(main())
```

### When is the DLQ Handler Called?

The DLQ handler is invoked when:

1. **Non-retriable task fails**: A task without retry configuration (or `retry=0`) fails
2. **Retriable task exhausts all retries**: A task with retry configuration fails after all retry attempts

The DLQ handler is NOT called:

- When a task succeeds
- When a task fails but still has retry attempts remaining

### Synchronous vs Async Handlers

The DLQ handler can be either synchronous or asynchronous:

```python
# Async handler (recommended for I/O operations)
async def async_dlq_handler(task: Task):
    await send_to_monitoring_system(task)

# Sync handler (for simple operations)
def sync_dlq_handler(task: Task):
    logger.error(f"Task {task.id} failed: {task.error}")

# Both work the same way
worker = Worker("default", backend, on_failure=async_dlq_handler)
# or
worker = Worker("default", backend, on_failure=sync_dlq_handler)
```

### Error Handling in DLQ Handlers

If your DLQ handler raises an exception, it will be logged but won't crash the worker. The worker will continue processing other tasks:

```python
async def faulty_handler(task: Task):
    raise RuntimeError("Oops!")  # This won't crash the worker

worker = Worker("default", backend, on_failure=faulty_handler)
await worker.work()  # Worker continues despite handler errors
```

### Best Practices

1. **Keep it simple**: DLQ handlers should be lightweight and fast
2. **Handle errors**: Wrap your DLQ handler logic in try-except to prevent cascading failures
3. **Use async for I/O**: If your handler does I/O operations (database, API calls), make it async
4. **Don't retry in the handler**: The task has already failed permanently; retrying in the handler can cause infinite loops
5. **Log everything**: Make sure to log the task details for debugging

### Complete Example

Here's a complete example showing DLQ handling in action:

```python
import asyncio
from datetime import datetime
from sheppy import MemoryBackend, Queue, Task, Worker, task

# Define tasks
@task
async def send_email(to: str, subject: str) -> dict:
    if "invalid" in to:
        raise ValueError(f"Invalid email: {to}")
    return {"status": "sent"}

@task(retry=2, retry_delay=0.5)
async def send_notification(user_id: int) -> dict:
    if user_id == 999:
        raise RuntimeError("User not found")
    return {"status": "sent"}

# DLQ handler
async def handle_failed_task(task: Task):
    print(f"\n{'='*60}")
    print(f"[DLQ] Task {task.id} failed permanently!")
    print(f"[DLQ] Function: {task.spec.func}")
    print(f"[DLQ] Error: {task.error}")
    print(f"[DLQ] Retry count: {task.retry_count}")
    print(f"{'='*60}\n")

async def main():
    queue = Queue(MemoryBackend())
    
    # Start worker with DLQ handler
    worker_task = asyncio.create_task(
        Worker("default", queue.backend, on_failure=handle_failed_task).work()
    )
    
    # Add tasks
    await queue.add(send_email("valid@example.com", "Hello"))  # Will succeed
    await queue.add(send_email("invalid@test", "Test"))  # Will fail -> DLQ
    await queue.add(send_notification(999))  # Will fail after retries -> DLQ
    
    await asyncio.sleep(2)
    worker_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
```

See the [complete DLQ example](../../examples/dlq_example.py) for more details.

