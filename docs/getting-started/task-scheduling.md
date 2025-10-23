# Task Scheduling

Sheppy provides flexible task scheduling capabilities that allow you to execute tasks at specific times or after a delay. This is useful for scenarios like sending reminder emails, processing delayed notifications, or running periodic maintenance tasks.

## Scheduling Basics

There are two main ways to schedule tasks in Sheppy:

1. **Relative scheduling** - Schedule a task to run after a certain delay (using `timedelta`)
2. **Absolute scheduling** - Schedule a task to run at a specific date and time (using `datetime`)

Both methods use the `queue.schedule()` method.

## Scheduling with Timedelta

Use `timedelta` to schedule tasks relative to the current time:

```python
import asyncio
from datetime import timedelta
from sheppy import Queue, RedisBackend, task

@task
async def send_reminder(email: str, message: str) -> str:
    print(f"Sending reminder to {email}: {message}")
    return f"Reminder sent to {email}"

async def main():
    queue = Queue(RedisBackend("redis://127.0.0.1:6379"))
    
    # Schedule task to run 10 seconds from now
    await queue.schedule(
        send_reminder("user@example.com", "Your appointment is tomorrow"),
        at=timedelta(seconds=10)
    )
    
    # Schedule task to run in 5 minutes
    await queue.schedule(
        send_reminder("user@example.com", "Meeting in 1 hour"),
        at=timedelta(minutes=5)
    )
    
    # Schedule task to run in 24 hours
    await queue.schedule(
        send_reminder("user@example.com", "Daily summary"),
        at=timedelta(hours=24)
    )

if __name__ == "__main__":
    asyncio.run(main())
```

## Scheduling with Datetime

Use `datetime` to schedule tasks at a specific point in time:

!!! warning
    The `datetime` object **must be offset-aware** (i.e., include timezone information). Sheppy will raise a `TypeError` if you provide a naive datetime.

```python
import asyncio
from datetime import datetime, timezone
from sheppy import Queue, RedisBackend, task

@task
async def send_birthday_email(name: str, email: str) -> str:
    print(f"Sending birthday wishes to {name}")
    return f"Birthday email sent to {email}"

async def main():
    queue = Queue(RedisBackend("redis://127.0.0.1:6379"))
    
    # Schedule task for New Year's Day 2026 (UTC)
    new_year = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    await queue.schedule(
        send_birthday_email("Alice", "alice@example.com"),
        at=new_year
    )
    
    # Or use fromisoformat with timezone offset
    birthday = datetime.fromisoformat("2026-03-15 09:00:00+00:00")
    await queue.schedule(
        send_birthday_email("Bob", "bob@example.com"),
        at=birthday
    )

if __name__ == "__main__":
    asyncio.run(main())
```

## Complete Example

Here's a complete example demonstrating task scheduling with both immediate and delayed execution:

```python
--8<-- "examples/simple_scheduled.py"
```

In this example:

1. A welcome email is sent immediately using `queue.add()`
2. A survey email is scheduled to be sent 2 seconds later using `queue.schedule()`
3. The worker processes both tasks at their scheduled times
4. We verify that the scheduled task wasn't executed immediately

## Viewing Scheduled Tasks

You can retrieve all scheduled tasks using `queue.get_scheduled()`:

```python
async def main():
    queue = Queue(RedisBackend("redis://127.0.0.1:6379"))
    
    # Get all scheduled tasks
    scheduled_tasks = await queue.get_scheduled()
    
    for task in scheduled_tasks:
        print(f"Task {task.id} scheduled for {task.scheduled_at}")
```

Or use the CLI:

```bash
sheppy task list --scheduled
```

## How Scheduled Tasks Work

When you schedule a task:

1. The task is stored in a scheduled queue with its `scheduled_at` timestamp
2. The worker's scheduler component periodically checks for tasks whose scheduled time has arrived
3. When a scheduled task is ready, it's moved to the pending queue for processing
4. A worker picks it up and executes it like any other task

!!! tip
    The scheduler checks for ready tasks every second by default. This means tasks will be executed within approximately 1 second of their scheduled time.

## Rescheduling Tasks

You can reschedule failed tasks or even successful ones using the `queue.retry()` method with the `at` parameter:

```python
async def main():
    queue = Queue(RedisBackend("redis://127.0.0.1:6379"))
    
    # Create and add a task
    task = send_reminder("user@example.com", "Test")
    await queue.add(task)
    
    # Wait for it to complete
    task = await queue.wait_for(task)
    
    # Reschedule it to run in 1 hour
    await queue.retry(task, at=timedelta(hours=1))
    
    # Or reschedule at a specific time
    await queue.retry(task, at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
```

!!! note
    By default, `retry()` won't allow rescheduling successfully completed tasks. Use `force=True` to override this behavior:
    
    ```python
    await queue.retry(task, at=timedelta(hours=1), force=True)
    ```

## Best Practices

1. **Always use timezone-aware datetimes** - This prevents ambiguity and errors when scheduling tasks
2. **Use timedelta for relative scheduling** - It's simpler and less error-prone than calculating future timestamps manually
3. **Monitor scheduled tasks** - Use `queue.get_scheduled()` or the CLI to verify your tasks are scheduled correctly
4. **Consider task idempotency** - Scheduled tasks might run slightly later than expected, so design them to handle delayed execution gracefully

## See Also

- [Error Handling](error-handling.md) - Learn how to handle task failures
- [Cron Jobs](cron.md) - For recurring scheduled tasks
- [Queue API Reference](../reference/queue.md) - Complete Queue class documentation
