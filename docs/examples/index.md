# Code Examples

This section contains practical examples demonstrating various features of Sheppy.

## Error Handling

### [Dead Letter Queue (DLQ)](../../examples/dlq_example.py)

Shows how to use the `on_failure` callback to handle permanently failed tasks. This example demonstrates:

- Handling non-retriable task failures
- Handling tasks that fail after exhausting all retries
- Using DLQ handlers for monitoring, alerting, and recovery

```python
from sheppy import Worker, Task

async def handle_failed_task(task: Task):
    print(f"Task {task.id} failed permanently: {task.error}")
    # Send to monitoring system, log to database, etc.

worker = Worker("default", backend, on_failure=handle_failed_task)
await worker.work()
```

See the [Error Handling guide](../getting-started/error-handling.md#dead-letter-queue-dlq) for complete documentation.

## Additional Examples

More examples can be found in the [examples directory](https://github.com/malvex/sheppy/tree/main/examples) of the repository.
