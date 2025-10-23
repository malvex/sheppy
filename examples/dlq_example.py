"""
Example demonstrating Dead Letter Queue (DLQ) functionality.

This example shows how to use the on_failure callback to handle permanently failed tasks.
"""

import asyncio
from datetime import datetime

from sheppy import MemoryBackend, Queue, Task, Worker, task


# Define tasks that can fail
@task
async def send_email(to: str, subject: str) -> dict[str, str]:
    """A task that might fail."""
    print(f"[{datetime.now()}] Sending email to {to}, Subject: {subject}")
    
    # Simulate failure for certain emails
    if "invalid" in to:
        raise ValueError(f"Invalid email address: {to}")
    
    return {"status": "sent", "to": to}


@task(retry=2, retry_delay=0.1)
async def send_notification(user_id: int, message: str) -> dict[str, str]:
    """A retriable task that might fail."""
    print(f"[{datetime.now()}] Sending notification to user {user_id}: {message}")
    
    # Simulate transient failures
    if user_id == 999:
        raise RuntimeError(f"Failed to send notification to user {user_id}")
    
    return {"status": "sent", "user_id": user_id}


# Define DLQ handler
async def handle_failed_task(task: Task) -> None:
    """
    Dead Letter Queue handler for permanently failed tasks.
    
    This function is called automatically by the worker when a task fails permanently.
    You can use this to:
    - Log failures to a monitoring system
    - Send alerts
    - Store failed tasks in a separate database
    - Retry with different logic
    - etc.
    """
    print(f"\n{'='*60}")
    print(f"[DLQ] Task {task.id} failed permanently!")
    print(f"[DLQ] Function: {task.spec.func}")
    print(f"[DLQ] Error: {task.error}")
    print(f"[DLQ] Retry count: {task.retry_count}")
    print(f"[DLQ] Failed at: {task.finished_at}")
    print(f"{'='*60}\n")
    
    # In a real application, you might:
    # - Send an alert to a monitoring system
    # - Store the failed task in a database for manual review
    # - Trigger a different recovery workflow
    # - etc.


queue = Queue(MemoryBackend())


async def run_worker():
    """Run worker with DLQ handler."""
    w = Worker("default", backend=queue.backend, on_failure=handle_failed_task)
    await w.work()


async def main():
    # Start worker in background
    worker_process = asyncio.create_task(run_worker())

    # Add some tasks that will succeed
    print("Adding successful tasks...")
    await queue.add(send_email("user@example.com", "Welcome!"))
    await queue.add(send_notification(123, "Hello!"))

    # Add a task that will fail immediately (non-retriable)
    print("Adding non-retriable failing task...")
    await queue.add(send_email("invalid@invalid", "Test"))

    # Add a task that will fail after retries (retriable)
    print("Adding retriable failing task...")
    await queue.add(send_notification(999, "This will fail"))

    # Wait a bit for processing
    await asyncio.sleep(2)

    # Check queue status
    pending = await queue.size()
    print(f"\nPending tasks: {pending}")

    # Stop worker
    worker_process.cancel()
    try:
        await worker_process
    except asyncio.CancelledError:
        pass

    print("\nExample completed!")


if __name__ == "__main__":
    asyncio.run(main())
